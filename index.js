const express = require("express");
const cors = require("cors");
const pino = require("pino");
const fs = require("fs");
const path = require("path");
const {
  default: makeWASocket,
  useMultiFileAuthState,
  DisconnectReason,
  fetchLatestBaileysVersion,
  makeCacheableSignalKeyStore,
  Browsers,
  delay
} = require("@whiskeysockets/baileys");

process.on("uncaughtException", (err) => {
  console.log("⚠️ تجاهل خطأ خلفي:", err.message);
});
process.on("unhandledRejection", (err) => {
  console.log("⚠️ تجاهل وعد غير معالج:", err?.message || err);
});

const app = express();
app.use(cors());
app.use(express.json({ limit: "10mb" }));

const SESSION_DIR = path.join(__dirname, "auth_info_baileys");
const PHONE_NUMBER = "201208027294";
const FIREBASE_API_KEY = "AIzaSyD7ZJP8n8fhMewPfEsTBANn0h9To_q15BY";
const FIREBASE_PROJECT_ID = "sman-ca8f8";
const logger = pino({ level: "silent" });

let sock = null;
let isConnected = false;
let isConnecting = false;
let isInitialStartup = true;
let currentPairingCode = null;
let pairingRetryTimer = null;
let reconnectTimer = null;
let isSendingQueueRunning = false;
let sentInBatchCounter = 0;
let queueRestoredOnce = false;
const sendQueue = [];

// خزنة الرسائل لحل مشكلة "في انتظار هذه الرسالة" عند طلب إعادة التشفير
const messageStore = new Map();
// منع تكرار الرد الآلي على نفس المتصل خلال 24 ساعة
const callAutoReplyCooldown = new Map();

let cachedFbToken = null;
let cachedFbTokenExpiresAt = 0;

// === دوال الربط السحابي مع Firebase لحفظ الطابور واستقبال المكالمات ===
async function getFirebaseIdToken() {
  const email = process.env.FIREBASE_EMAIL;
  const password = process.env.FIREBASE_PASSWORD;
  if (!email || !password) return null;

  if (cachedFbToken && Date.now() < cachedFbTokenExpiresAt) {
    return cachedFbToken;
  }

  try {
    const res = await fetch(`https://identitytoolkit.googleapis.com/v1/accounts:signInWithPassword?key=${FIREBASE_API_KEY}`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ email, password, returnSecureToken: true })
    });
    const data = await res.json();
    if (data.idToken) {
      cachedFbToken = data.idToken;
      cachedFbTokenExpiresAt = Date.now() + 45 * 60 * 1000;
      return cachedFbToken;
    }
  } catch (e) {}
  return null;
}

async function saveQueueBackupToFirestore() {
  const token = await getFirebaseIdToken();
  if (!token) return;
  try {
    const url = `https://firestore.googleapis.com/v1/projects/${FIREBASE_PROJECT_ID}/databases/(default)/documents/inventory/wa_queue_backup?updateMask.fieldPaths=queueJson&updateMask.fieldPaths=updatedAt`;
    await fetch(url, {
      method: "PATCH",
      headers: {
        "Authorization": "Bearer " + token,
        "Content-Type": "application/json"
      },
      body: JSON.stringify({
        fields: {
          queueJson: { stringValue: JSON.stringify(sendQueue) },
          updatedAt: { integerValue: String(Date.now()) }
        }
      })
    });
  } catch (e) {}
}

async function restoreQueueBackupFromFirestore() {
  if (queueRestoredOnce) return;
  queueRestoredOnce = true;

  const token = await getFirebaseIdToken();
  if (!token) return;

  try {
    const url = `https://firestore.googleapis.com/v1/projects/${FIREBASE_PROJECT_ID}/databases/(default)/documents/inventory/wa_queue_backup`;
    const res = await fetch(url, {
      method: "GET",
      headers: { "Authorization": "Bearer " + token }
    });
    if (!res.ok) return;
    const doc = await res.json();
    const rawJson = doc?.fields?.queueJson?.stringValue;
    if (rawJson) {
      const savedItems = JSON.parse(rawJson);
      if (Array.isArray(savedItems) && savedItems.length > 0) {
        sendQueue.push(...savedItems);
        console.log(`🔄 تم استرجاع (${savedItems.length}) رسالة متبقية من الطابور السحابي لاستكمال الإرسال!`);
        processSendQueue();
      }
    }
  } catch (e) {}
}

async function updateCampaignStatsInFirestore(campaignId, isSuccess) {
  if (!campaignId) return;
  const token = await getFirebaseIdToken();
  if (!token) return;

  try {
    const docUrl = `https://firestore.googleapis.com/v1/projects/${FIREBASE_PROJECT_ID}/databases/(default)/documents/wa_campaigns/${campaignId}`;
    const getRes = await fetch(docUrl, {
      headers: { "Authorization": "Bearer " + token }
    });
    if (!getRes.ok) return;
    const docData = await getRes.json();
    const fields = docData.fields || {};

    const total = Number(fields.total?.integerValue || 0);
    let sentCount = Number(fields.sentCount?.integerValue || 0);
    let failedCount = Number(fields.failedCount?.integerValue || 0);

    if (isSuccess) sentCount++;
    else failedCount++;

    const isDone = total > 0 && (sentCount + failedCount) >= total;
    const status = isDone ? "completed" : "running";

    const patchUrl = `${docUrl}?updateMask.fieldPaths=sentCount&updateMask.fieldPaths=failedCount&updateMask.fieldPaths=status&updateMask.fieldPaths=updatedAt`;
    await fetch(patchUrl, {
      method: "PATCH",
      headers: {
        "Authorization": "Bearer " + token,
        "Content-Type": "application/json"
      },
      body: JSON.stringify({
        fields: {
          sentCount: { integerValue: String(sentCount) },
          failedCount: { integerValue: String(failedCount) },
          status: { stringValue: status },
          updatedAt: { integerValue: String(Date.now()) }
        }
      })
    });
  } catch (e) {}
}

function normalizeLocalEgyptPhone(raw) {
  if (!raw) return "";
  let n = raw.toString().replace(/\D/g, "");
  if (n.startsWith("0020")) n = "0" + n.substring(4);
  else if (n.startsWith("201")) n = "0" + n.substring(2);
  else if (n.length === 10 && n.startsWith("1")) n = "0" + n;
  return n;
}

async function handleIncomingCallEvent(rawPhone, callSource = "phone_call") {
  const localPhone = normalizeLocalEgyptPhone(rawPhone);
  if (!localPhone || localPhone.length < 10) return { ok: false, reason: "invalid_phone" };

  console.log(`📞 مكالمة واردة (${callSource}) من الرقم: ${localPhone}`);
  const token = await getFirebaseIdToken();
  if (!token) return { ok: false, reason: "no_firebase_auth" };

  let customerName = "";
  let zone = "";
  let ordersCount = 0;
  let isRegistered = false;

  try {
    // 1. البحث عن العميل في أرشيف العملاء
    const custUrl = `https://firestore.googleapis.com/v1/projects/${FIREBASE_PROJECT_ID}/databases/(default)/documents/customers/${localPhone}`;
    const custRes = await fetch(custUrl, { headers: { "Authorization": "Bearer " + token } });
    if (custRes.ok) {
      const custDoc = await custRes.json();
      const f = custDoc.fields || {};
      customerName = f.name?.stringValue || f.customerName?.stringValue || "";
      zone = f.zone?.stringValue || "";
      ordersCount = Number(f.ordersCount?.integerValue || 0);
      if (customerName) isRegistered = true;
    }

    // 2. قراءة إعدادات الرد الآلي على المكالمات من inventory/settings
    const settingsUrl = `https://firestore.googleapis.com/v1/projects/${FIREBASE_PROJECT_ID}/databases/(default)/documents/inventory/settings`;
    const setRes = await fetch(settingsUrl, { headers: { "Authorization": "Bearer " + token } });
    let autoReplyEnabled = false;
    let knownTpl = "أهلاً بك يا {اسم_العميل} في سمان ههيا 🌹\nشرفنا اتصالك، لو حابب تحجز في الدفعة الحالية ابعتلنا الكمية المطلوبة هنا وهنأكدلك الحجز فوراً 🦆";
    let newTpl = "أهلاً بك في سمان ههيا 🦆✨\nشرفنا اتصالك! بنوفر سمان مدبوح فريش طازج بالتوصيل لحد باب البيت.\nابعتلنا منطقتك والكمية المطلوبة وهنرد عليك بكل التفاصيل فوراً 🌹";

    if (setRes.ok) {
      const setDoc = await setRes.json();
      const sf = setDoc.fields || {};
      autoReplyEnabled = sf.autoReplyCallsEnabled?.booleanValue === true;
      if (sf.autoReplyKnownTemplate?.stringValue) knownTpl = sf.autoReplyKnownTemplate.stringValue;
      if (sf.autoReplyNewTemplate?.stringValue) newTpl = sf.autoReplyNewTemplate.stringValue;
    }

    // 3. إرسال الرد الآلي الذكي (بحد أقصى مرة كل 24 ساعة لنفس الرقم)
    let autoReplied = false;
    const lastReplyTs = callAutoReplyCooldown.get(localPhone) || 0;
    const isWithin24h = (Date.now() - lastReplyTs) < 24 * 60 * 60 * 1000;

    if (autoReplyEnabled && isConnected && !isWithin24h) {
      let msgToSend = isRegistered
        ? knownTpl.replace(/{اسم_العميل}/g, customerName).replace(/{المنطقة}/g, zone)
        : newTpl;

      callAutoReplyCooldown.set(localPhone, Date.now());
      sendQueue.unshift({ phone: localPhone, message: msgToSend, isPriority: true });
      processSendQueue();
      autoReplied = true;
    }

    // 4. تسجيل المكالمة في سجل المكالمات wa_call_logs
    const logCreateUrl = `https://firestore.googleapis.com/v1/projects/${FIREBASE_PROJECT_ID}/databases/(default)/documents/wa_call_logs`;
    await fetch(logCreateUrl, {
      method: "POST",
      headers: {
        "Authorization": "Bearer " + token,
        "Content-Type": "application/json"
      },
      body: JSON.stringify({
        fields: {
          phone: { stringValue: localPhone },
          customerName: { stringValue: customerName || "رقم جديد غير مسجل" },
          zone: { stringValue: zone || "" },
          ordersCount: { integerValue: String(ordersCount) },
          isRegistered: { booleanValue: isRegistered },
          source: { stringValue: callSource },
          autoReplied: { booleanValue: autoReplied },
          createdAtMs: { integerValue: String(Date.now()) }
        }
      })
    });

    return { ok: true, phone: localPhone, isRegistered, customerName, autoReplied };
  } catch (e) {
    console.log("⚠️ خطأ أثناء معالجة المكالمة الواردة:", e.message);
    return { ok: false, error: e.message };
  }
}

// === دوال اتصال الواتساب الأساسية ===
function cleanCorruptedSessionIfNeeded() {
  const credsPath = path.join(SESSION_DIR, "creds.json");
  if (fs.existsSync(credsPath)) {
    try {
      const creds = JSON.parse(fs.readFileSync(credsPath, "utf-8"));
      if (isInitialStartup && !creds.registered) {
        console.log("🧹 مسح بقايا محاولة ربط قديمة غير مكتملة للبدء بمفاتيح جديدة...");
        fs.rmSync(SESSION_DIR, { recursive: true, force: true });
      }
    } catch (e) {
      console.log("🧹 مسح ملف جلسة تالف للبدء على نظافة...");
      fs.rmSync(SESSION_DIR, { recursive: true, force: true });
    }
  }
}

function scheduleReconnect(delayMs) {
  if (reconnectTimer) return;
  reconnectTimer = setTimeout(() => {
    reconnectTimer = null;
    connectToWhatsApp();
  }, delayMs);
}

async function requestPairingCodeSafely(targetSock) {
  if (pairingRetryTimer) {
    clearTimeout(pairingRetryTimer);
    pairingRetryTimer = null;
  }

  try {
    if (!targetSock || targetSock !== sock || targetSock.authState.creds.registered) {
      return;
    }
    let code = await targetSock.requestPairingCode(PHONE_NUMBER);
    code = code?.match(/.{1,4}/g)?.join("-") || code;
    currentPairingCode = code;
    console.log("========================================");
    console.log("🔑 كود ربط الواتساب الجديد الخاص بك هو: ", code);
    console.log("📱 أدخل هذا الكود الآن في واتساب (رقم 01208027294) خلال دقيقة واحدة");
    console.log("========================================");
  } catch (err) {
    console.log("⚠️ جاري إعادة محاولة استخراج كود الربط خلال 5 ثوانٍ...", err.message);
    pairingRetryTimer = setTimeout(() => {
      requestPairingCodeSafely(targetSock);
    }, 5000);
  }
}

async function connectToWhatsApp() {
  if (isConnecting) return;
  isConnecting = true;

  if (pairingRetryTimer) {
    clearTimeout(pairingRetryTimer);
    pairingRetryTimer = null;
  }

  if (sock) {
    try {
      sock.ev.removeAllListeners();
      sock.end();
    } catch (e) {}
    sock = null;
  }

  try {
    cleanCorruptedSessionIfNeeded();
    isInitialStartup = false;

    const { state, saveCreds } = await useMultiFileAuthState(SESSION_DIR);

    let version = [2, 3000, 1015901307];
    try {
      const latest = await fetchLatestBaileysVersion();
      if (latest?.version) {
        version = latest.version;
      }
    } catch (e) {
      console.log("⚠️ استخدام نسخة واتساب الاحتياطية للاتصال...");
    }

    const currentSock = makeWASocket({
      version,
      logger,
      printQRInTerminal: false,
      auth: {
        creds: state.creds,
        keys: makeCacheableSignalKeyStore ? makeCacheableSignalKeyStore(state.keys, logger) : state.keys
      },
      browser: Browsers.macOS("Chrome"),
      syncFullHistory: false,
      shouldSyncHistoryMessage: () => false,
      markOnlineOnConnect: true,
      connectTimeoutMs: 60000,
      defaultQueryTimeoutMs: 60000,
      keepAliveIntervalMs: 25000,
      getMessage: async (key) => {
        if (key?.id && messageStore.has(key.id)) {
          return messageStore.get(key.id);
        }
        return { conversation: "سمان ههيا" };
      }
    });

    sock = currentSock;
    let pairingStartedForThisSocket = false;

    currentSock.ev.on("creds.update", saveCreds);

    // حفظ أي رسالة صادرة في الذاكرة لتلبية طلبات فك التشفير من هواتف العملاء
    currentSock.ev.on("messages.upsert", ({ messages }) => {
      for (const m of messages) {
        if (m.key?.id && m.message) {
          messageStore.set(m.key.id, m.message);
          if (messageStore.size > 500) {
            const firstKey = messageStore.keys().next().value;
            messageStore.delete(firstKey);
          }
        }
      }
    });

    // التقاط مكالمات الواتساب الواردة تلقائياً
    currentSock.ev.on("call", async (calls) => {
      for (const c of calls) {
        if (c.status === "offer" && c.from && !c.from.includes("@lid")) {
          const callerDigits = c.from.split("@")[0];
          await handleIncomingCallEvent(callerDigits, "whatsapp_call");
        }
      }
    });

    currentSock.ev.on("connection.update", async (update) => {
      if (currentSock !== sock) return;
      const { connection, lastDisconnect, qr } = update;

      if (qr && !currentSock.authState.creds.registered && !pairingStartedForThisSocket) {
        pairingStartedForThisSocket = true;
        pairingRetryTimer = setTimeout(() => {
          requestPairingCodeSafely(currentSock);
        }, 1500);
      }

      if (connection === "close") {
        isConnected = false;
        isConnecting = false;
        if (pairingRetryTimer) {
          clearTimeout(pairingRetryTimer);
          pairingRetryTimer = null;
        }

        const statusCode = lastDisconnect?.error?.output?.statusCode;
        console.log(`⚠️ الاتصال فصل (كود: ${statusCode})`);

        if (statusCode === DisconnectReason.loggedOut || statusCode === 401) {
          currentPairingCode = null;
          console.log("🧹 تم تسجيل الخروج (401).. جاري مسح الجلسة القديمة وتوليد كود جديد...");
          try {
            fs.rmSync(SESSION_DIR, { recursive: true, force: true });
          } catch (e) {}
          scheduleReconnect(3000);
        } else {
          scheduleReconnect(2000);
        }
      } else if (connection === "open") {
        isConnected = true;
        isConnecting = false;
        currentPairingCode = null;
        if (pairingRetryTimer) {
          clearTimeout(pairingRetryTimer);
          pairingRetryTimer = null;
        }
        console.log("========================================");
        console.log("✅ تم ربط الواتساب بنجاح والسيرفر جاهز للعمل 24/7!");
        console.log("========================================");
        restoreQueueBackupFromFirestore();
      }
    });
  } catch (err) {
    isConnecting = false;
    console.log("⚠️ خطأ أثناء تهيئة الاتصال، إعادة المحاولة خلال 4 ثوانٍ...", err.message);
    scheduleReconnect(4000);
  }
}

connectToWhatsApp();

// دالة إرسال موحدة تفحص الرقم وتنشط التشفير قبل الإرسال
async function sendSafeWhatsAppMessage(rawPhone, textMessage) {
  let cleanPhone = rawPhone.toString().replace(/\D/g, "");
  if (cleanPhone.startsWith("0020")) cleanPhone = cleanPhone.substring(2);
  else if (cleanPhone.startsWith("0")) cleanPhone = "2" + cleanPhone;
  else if (cleanPhone.length === 10 && cleanPhone.startsWith("1")) cleanPhone = "20" + cleanPhone;

  let targetJid = cleanPhone + "@s.whatsapp.net";

  try {
    const [result] = await sock.onWhatsApp(cleanPhone);
    if (result && result.exists && result.jid) {
      targetJid = result.jid;
    }
  } catch (e) {}

  try {
    const typingMs = Math.floor(Math.random() * (3000 - 1500 + 1)) + 1500;
    await sock.presenceSubscribe(targetJid);
    await sock.sendPresenceUpdate("composing", targetJid);
    await delay(typingMs);
    await sock.sendPresenceUpdate("paused", targetJid);
  } catch (e) {}

  const sentMsg = await sock.sendMessage(targetJid, { text: textMessage });
  if (sentMsg?.key?.id && sentMsg.message) {
    messageStore.set(sentMsg.key.id, sentMsg.message);
  }
  return cleanPhone;
}

// مشغل طابور الإرسال المتسلسل بفواصل زمنية بشرية آمنة وحفظ سحابي مستمر
async function processSendQueue() {
  if (isSendingQueueRunning) return;
  isSendingQueueRunning = true;

  while (sendQueue.length > 0) {
    const item = sendQueue.shift();
    if (!isConnected) {
      console.log("⏳ انتظار عودة اتصال الواتساب...");
      await delay(5000);
      sendQueue.unshift(item);
      continue;
    }

    try {
      const sentTo = await sendSafeWhatsAppMessage(item.phone, item.message);
      sentInBatchCounter++;
      console.log(`✅ تم الإرسال بنجاح إلى: ${sentTo} (متبقي في الطابور: ${sendQueue.length})`);
      await updateCampaignStatsInFirestore(item.campaignId, true);
    } catch (err) {
      console.log(`❌ فشل الإرسال إلى ${item.phone}:`, err.message);
      await updateCampaignStatsInFirestore(item.campaignId, false);
    }

    await saveQueueBackupToFirestore();

    if (sendQueue.length > 0) {
      if (sentInBatchCounter >= 15) {
        sentInBatchCounter = 0;
        const cooldownMs = Math.floor(Math.random() * (60000 - 45000 + 1)) + 45000;
        console.log(`🛡️ استراحة أمان لمدة ${Math.round(cooldownMs / 1000)} ثانية لتجنب قيود السبام...`);
        await delay(cooldownMs);
      } else {
        const waitMs = Math.floor(Math.random() * (28000 - 15000 + 1)) + 15000;
        await delay(waitMs);
      }
    }
  }

  sentInBatchCounter = 0;
  isSendingQueueRunning = false;
  await saveQueueBackupToFirestore();
  console.log("🎉 اكتمل إرسال جميع الرسائل في الطابور!");
}

app.get("/", (req, res) => {
  res.status(200).json({
    ok: true,
    connected: isConnected,
    pairingCode: currentPairingCode,
    queueLength: sendQueue.length,
    message: isConnected ? "Server is running & WhatsApp Connected!" : "Waiting for WhatsApp pairing..."
  });
});

app.post("/send-bulk-smart", async (req, res) => {
  const { bulkData, campaignId } = req.body;
  if (!bulkData || !Array.isArray(bulkData)) {
    return res.status(400).json({ error: "البيانات ناقصة!" });
  }
  if (!isConnected) {
    return res.status(503).json({ error: "الواتساب غير متصل بالسيرفر حالياً!" });
  }

  bulkData.forEach((item) => {
    if (item.phone && item.message) {
      sendQueue.push({
        phone: item.phone,
        name: item.name || "",
        message: item.message,
        campaignId: campaignId || item.campaignId || null
      });
    }
  });

  await saveQueueBackupToFirestore();
  res.status(200).json({ status: "queued", added: bulkData.length, totalInQueue: sendQueue.length });
  processSendQueue();
});

app.post("/send-bulk", async (req, res) => {
  const { numbers, message } = req.body;
  if (!numbers || !Array.isArray(numbers) || !message) {
    return res.status(400).send("البيانات ناقصة!");
  }
  if (!isConnected) {
    return res.status(503).send("الواتساب غير متصل!");
  }

  numbers.forEach((num) => sendQueue.push({ phone: num, message }));
  await saveQueueBackupToFirestore();
  res.status(200).send("تمت الإضافة لطابور الإرسال");
  processSendQueue();
});

// مسار استقبال إشعارات المكالمات الواردة من تطبيق الهاتف (مثل MacroDroid)
app.all("/incoming-call", async (req, res) => {
  const rawPhone = req.body?.phone || req.query?.phone;
  if (!rawPhone) {
    return res.status(400).json({ ok: false, error: "يرجى إرسال رقم المتصل في حقل phone" });
  }
  const result = await handleIncomingCallEvent(rawPhone, "phone_call");
  res.status(200).json(result);
});

const PORT = process.env.PORT || 8080;
app.listen(PORT, () => {
  console.log(`🌐 Server listening on port ${PORT}`);
});
