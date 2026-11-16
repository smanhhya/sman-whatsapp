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
  fetchLatestWaWebVersion,
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
const RESET_FLAG_FILE = path.join(__dirname, ".reset_wa_secret");
const PHONE_NUMBER = "201208027294";
const FIREBASE_API_KEY = "AIzaSyD7ZJP8n8fhMewPfEsTBANn0h9To_q15BY";
const FIREBASE_PROJECT_ID = "sman-ca8f8";
const logger = pino({ level: "silent" });

let sock = null;
let isConnected = false;
let isConnecting = false;
let pairingMode = "code"; // "code" أو "qr" لمنع التعارض بينهما
let currentPairingCode = null;
let currentQrString = null;
let pairingCodeGeneratedAt = 0;
let pairingRetryTimer = null;
let reconnectTimer = null;
let isSendingQueueRunning = false;
let sentInBatchCounter = 0;
let queueRestoredOnce = false;
const sendQueue = [];

const messageStore = new Map();
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
function wipeSessionDirectory(markSecretReset = false) {
  try {
    fs.rmSync(SESSION_DIR, { recursive: true, force: true });
    if (markSecretReset) {
      fs.writeFileSync(RESET_FLAG_FILE, String(Date.now()));
    }
  } catch (e) {}
}

function cleanUnregisteredSessionBeforeStart() {
  const credsPath = path.join(SESSION_DIR, "creds.json");
  if (fs.existsSync(credsPath)) {
    try {
      const creds = JSON.parse(fs.readFileSync(credsPath, "utf-8"));
      // إذا لم تكتمل عملية الربط ولم يتم تعيين هوية الحساب بعد، نبدأ بمفاتيح نظيفة تماماً
      if (!creds.registered && !creds.me) {
        console.log("🧹 تنظيف محاولة ربط سابقة غير مكتملة للبدء بمفاتيح تشفير جديدة...");
        wipeSessionDirectory(false);
      }
    } catch (e) {
      console.log("🧹 مسح ملف جلسة تالف للبدء على نظافة...");
      wipeSessionDirectory(false);
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

async function resolveLatestWaVersion() {
  try {
    if (typeof fetchLatestWaWebVersion === "function") {
      const waWeb = await fetchLatestWaWebVersion({});
      if (waWeb?.version) return waWeb.version;
    }
  } catch (e) {}
  try {
    const latest = await fetchLatestBaileysVersion();
    if (latest?.version) return latest.version;
  } catch (e) {}
  return [2, 3000, 1020500000];
}

async function requestPairingCodeSafely(targetSock) {
  if (pairingRetryTimer) {
    clearTimeout(pairingRetryTimer);
    pairingRetryTimer = null;
  }

  try {
    if (!targetSock || targetSock !== sock || targetSock.authState.creds.registered || pairingMode !== "code") {
      return;
    }
    let code = await targetSock.requestPairingCode(PHONE_NUMBER);
    code = code?.match(/.{1,4}/g)?.join("-") || code;
    currentPairingCode = code;
    currentQrString = null;
    pairingCodeGeneratedAt = Date.now();
    console.log("========================================");
    console.log("🔑 كود ربط الواتساب الجديد الخاص بك هو: ", code);
    console.log("📱 أدخل هذا الكود الآن في واتساب (رقم 01208027294)");
    console.log("========================================");
  } catch (err) {
    console.log("⚠️ لم ينجح استخراج الكود في هذه المحاولة:", err.message);
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
    cleanUnregisteredSessionBeforeStart();

    const { state, saveCreds } = await useMultiFileAuthState(SESSION_DIR);
    const version = await resolveLatestWaVersion();
    console.log("📡 جاري الاتصال بسيرفر واتساب باستخدام الإصدار:", version.join("."));

    const currentSock = makeWASocket({
      version,
      logger,
      printQRInTerminal: false,
      auth: {
        creds: state.creds,
        keys: makeCacheableSignalKeyStore ? makeCacheableSignalKeyStore(state.keys, logger) : state.keys
      },
      browser: Browsers.ubuntu("Chrome"),
      syncFullHistory: false,
      shouldSyncHistoryMessage: () => false,
      markOnlineOnConnect: false, // مهم جداً لإتمام عملية الربط بدون فصل
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

      if (qr && !currentSock.authState.creds.registered) {
        if (pairingMode === "qr") {
          currentQrString = qr;
          currentPairingCode = null;
          console.log("📷 تم توليد رمز QR جديد، امسحه الآن من صفحة /pair");
        } else if (pairingMode === "code" && !pairingStartedForThisSocket) {
          pairingStartedForThisSocket = true;
          currentQrString = null;
          pairingRetryTimer = setTimeout(() => {
            requestPairingCodeSafely(currentSock);
          }, 2500);
        }
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

        const fatalStatuses = [
          DisconnectReason.loggedOut,
          DisconnectReason.badSession,
          DisconnectReason.forbidden,
          DisconnectReason.multideviceMismatch,
          401, 403, 405, 411, 500
        ];

        if (fatalStatuses.includes(statusCode)) {
          currentPairingCode = null;
          currentQrString = null;
          console.log(`🧹 جلسة غير صالحة (${statusCode}).. جاري مسح الجلسة القديمة وتوليد مفاتيح جديدة...`);
          wipeSessionDirectory(true);
          scheduleReconnect(3000);
        } else if (statusCode === DisconnectReason.restartRequired || statusCode === 515) {
          console.log("🔄 إتمام المصافحة (515).. جاري إعادة الاتصال الفوري لتثبيت الجلسة...");
          scheduleReconnect(1000);
        } else {
          // إذا لم يتم الربط بعد وانتهت مهلة المحاولة (408)، ننظف المجلد لبدء كود جديد نظيف
          if (!currentSock.authState?.creds?.registered && !currentSock.authState?.creds?.me) {
            currentPairingCode = null;
            currentQrString = null;
            wipeSessionDirectory(false);
          }
          scheduleReconnect(3000);
        }
      } else if (connection === "open") {
        isConnected = true;
        isConnecting = false;
        currentPairingCode = null;
        currentQrString = null;
        try {
          if (fs.existsSync(RESET_FLAG_FILE)) fs.unlinkSync(RESET_FLAG_FILE);
        } catch (e) {}
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
    mode: pairingMode,
    pairingCode: currentPairingCode,
    qrString: currentQrString,
    pairingCodeAgeSec: pairingCodeGeneratedAt ? Math.round((Date.now() - pairingCodeGeneratedAt) / 1000) : null,
    queueLength: sendQueue.length,
    message: isConnected ? "Server is running & WhatsApp Connected!" : "Waiting for WhatsApp pairing..."
  });
});

app.post("/reset-pair", (req, res) => {
  if (req.body?.mode === "qr" || req.body?.mode === "code") {
    pairingMode = req.body.mode;
  }
  currentPairingCode = null;
  currentQrString = null;
  isConnected = false;
  isConnecting = false;
  if (pairingRetryTimer) {
    clearTimeout(pairingRetryTimer);
    pairingRetryTimer = null;
  }
  if (reconnectTimer) {
    clearTimeout(reconnectTimer);
    reconnectTimer = null;
  }
  if (sock) {
    try {
      sock.ev.removeAllListeners();
      sock.end();
    } catch (e) {}
    sock = null;
  }
  wipeSessionDirectory(true);
  scheduleReconnect(1000);
  res.status(200).json({ ok: true, mode: pairingMode });
});

app.get("/pair", (req, res) => {
  res.setHeader("Content-Type", "text/html; charset=utf-8");
  res.send(`<!DOCTYPE html>
<html lang="ar" dir="rtl">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>ربط واتساب سمان ههيا</title>
<script src="https://cdnjs.cloudflare.com/ajax/libs/qrcodejs/1.0.0/qrcode.min.js"></script>
<style>
  body { font-family: system-ui, sans-serif; background: #0f172a; color: #f8fafc; display: flex; align-items: center; justify-content: center; min-height: 100vh; margin: 0; padding: 16px; }
  .card { background: #1e293b; border: 1px solid #334155; border-radius: 24px; padding: 24px 20px; max-width: 420px; width: 100%; text-align: center; box-shadow: 0 20px 40px rgba(0,0,0,0.4); }
  .tabs { display: flex; gap: 8px; margin-bottom: 14px; }
  .tab { flex: 1; background: #0f172a; color: #94a3b8; border: 1px solid #334155; padding: 10px; border-radius: 12px; font-weight: 700; cursor: pointer; font-size: 13px; }
  .tab.active { background: #25D366; color: #052e16; border-color: #25D366; }
  .code { font-size: 32px; font-weight: 900; letter-spacing: 3px; background: #0f172a; color: #4ade80; padding: 14px; border-radius: 16px; border: 2px dashed #22c55e; margin: 14px 0; direction: ltr; }
  .qr-wrap { background: #ffffff; padding: 14px; border-radius: 16px; display: inline-block; margin: 12px auto; }
  button.action { background: #25D366; color: #052e16; border: none; padding: 13px 18px; border-radius: 14px; font-size: 15px; font-weight: 800; width: 100%; cursor: pointer; margin-bottom: 10px; }
  button.sec { background: #334155; color: #f8fafc; font-size: 13px; border: none; padding: 12px; border-radius: 14px; width: 100%; cursor: pointer; font-weight: 700; }
  .sub { color: #94a3b8; font-size: 13px; line-height: 1.6; }
</style>
</head>
<body>
<div class="card">
  <h2 style="margin-top:0">📱 ربط واتساب سمان ههيا</h2>
  <div id="tabsBox" class="tabs">
    <button id="tabCode" class="tab active" onclick="switchMode('code')">🔢 كود 8 أرقام</button>
    <button id="tabQr" class="tab" onclick="switchMode('qr')">📷 مسح QR Code</button>
  </div>
  <div id="status" class="sub">جاري الاتصال بالسيرفر...</div>
  <div id="codeBox" class="code" style="display:none">----</div>
  <button id="copyBtn" class="action" style="display:none" onclick="copyCode()">📋 نسخ كود الربط (8 أرقام)</button>
  <div id="qrSection" style="display:none">
    <div class="qr-wrap"><div id="qrcode"></div></div>
  </div>
  <button id="resetBtn" class="sec" onclick="resetPair()">🔄 تصفير الجلسة وتوليد ربط جديد الآن</button>
  <p class="sub" style="margin-top:10px">رقم الشغل: <b>01208027294</b> • اختر إما كود الأرقام أو مسح الـ QR (بدون الجمع بينهما في نفس اللحظة لضمان نجاح الربط).</p>
</div>
<script>
  let latestCode = "";
  let lastQr = "";
  let currentMode = "code";

  function copyCode() {
    if (!latestCode) return;
    navigator.clipboard.writeText(latestCode.replace(/-/g, ""));
    document.getElementById("copyBtn").innerText = "✅ تم النسخ! الصقه في واتساب الآن";
  }

  async function switchMode(newMode) {
    currentMode = newMode;
    document.getElementById("tabCode").className = "tab" + (newMode === "code" ? " active" : "");
    document.getElementById("tabQr").className = "tab" + (newMode === "qr" ? " active" : "");
    document.getElementById("status").innerText = "⏳ جاري التبديل وتجهيز الجلسة...";
    document.getElementById("codeBox").style.display = "none";
    document.getElementById("copyBtn").style.display = "none";
    document.getElementById("qrSection").style.display = "none";
    lastQr = "";
    await fetch("/reset-pair", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ mode: newMode })
    });
  }

  async function resetPair() {
    document.getElementById("resetBtn").innerText = "⏳ جاري التصفير...";
    lastQr = "";
    await fetch("/reset-pair", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ mode: currentMode })
    });
    document.getElementById("resetBtn").innerText = "🔄 تصفير الجلسة وتوليد ربط جديد الآن";
  }

  async function poll() {
    try {
      const r = await fetch("/");
      const d = await r.json();
      if (d.connected) {
        document.getElementById("status").innerHTML = "<b style='color:#4ade80;font-size:18px'>✅ تم ربط الواتساب بنجاح والسيرفر متصل!</b>";
        document.getElementById("tabsBox").style.display = "none";
        document.getElementById("codeBox").style.display = "none";
        document.getElementById("copyBtn").style.display = "none";
        document.getElementById("qrSection").style.display = "none";
        document.getElementById("resetBtn").style.display = "none";
        return;
      }

      currentMode = d.mode || "code";
      document.getElementById("tabCode").className = "tab" + (currentMode === "code" ? " active" : "");
      document.getElementById("tabQr").className = "tab" + (currentMode === "qr" ? " active" : "");

      if (currentMode === "code") {
        document.getElementById("qrSection").style.display = "none";
        if (d.pairingCode) {
          latestCode = d.pairingCode;
          document.getElementById("status").innerText = "الكود جاهز (منذ " + (d.pairingCodeAgeSec || 0) + " ثانية):";
          document.getElementById("codeBox").style.display = "block";
          document.getElementById("codeBox").innerText = d.pairingCode;
          document.getElementById("copyBtn").style.display = "block";
        } else {
          document.getElementById("codeBox").style.display = "none";
          document.getElementById("copyBtn").style.display = "none";
          document.getElementById("status").innerText = "⏳ جاري توليد كود الـ 8 أرقام...";
        }
      } else {
        document.getElementById("codeBox").style.display = "none";
        document.getElementById("copyBtn").style.display = "none";
        if (d.qrString && window.QRCode) {
          document.getElementById("status").innerText = "امسح رمز الـ QR الآن من تطبيق واتساب:";
          document.getElementById("qrSection").style.display = "block";
          if (d.qrString !== lastQr) {
            lastQr = d.qrString;
            const qrEl = document.getElementById("qrcode");
            qrEl.innerHTML = "";
            new QRCode(qrEl, { text: d.qrString, width: 210, height: 210 });
          }
        } else {
          document.getElementById("status").innerText = "⏳ جاري توليد رمز الـ QR...";
        }
      }
    } catch (e) {}
  }
  poll();
  setInterval(poll, 2500);
</script>
</body>
</html>`);
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
