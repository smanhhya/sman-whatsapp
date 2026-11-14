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
const PHONE_NUMBER = "201280531968";
const logger = pino({ level: "silent" });

let sock = null;
let isConnected = false;
let isConnecting = false;
let currentPairingCode = null;
let pairingRetryTimer = null;
let isSendingQueueRunning = false;
const sendQueue = [];

// خزنة الرسائل لحل مشكلة "في انتظار هذه الرسالة" عند طلب إعادة التشفير
const messageStore = new Map();

function cleanCorruptedSessionIfNeeded() {
  const credsPath = path.join(SESSION_DIR, "creds.json");
  if (fs.existsSync(credsPath)) {
    try {
      JSON.parse(fs.readFileSync(credsPath, "utf-8"));
    } catch (e) {
      console.log("🧹 مسح ملف جلسة تالف للبدء على نظافة...");
      fs.rmSync(SESSION_DIR, { recursive: true, force: true });
    }
  }
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
      browser: Browsers.ubuntu("Chrome"),
      syncFullHistory: false,
      shouldSyncHistoryMessage: () => false,
      markOnlineOnConnect: true,
      connectTimeoutMs: 60000,
      defaultQueryTimeoutMs: 60000,
      keepAliveIntervalMs: 20000,
      // الدالة الحاسمة لمنع تعليق الرسائل على "في انتظار هذه الرسالة"
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

    currentSock.ev.on("connection.update", async (update) => {
      if (currentSock !== sock) return;
      const { connection, lastDisconnect, qr } = update;

      // طلب كود الربط فور جاهزية قناة الاتصال مع خوادم واتساب
      if ((qr || connection === "connecting") && !currentSock.authState.creds.registered && !pairingStartedForThisSocket) {
        pairingStartedForThisSocket = true;
        pairingRetryTimer = setTimeout(() => {
          requestPairingCodeSafely(currentSock);
        }, 2500);
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

        // نمسح الجلسة فقط إذا تم تسجيل الخروج رسمياً (401)، ونحافظ عليها أثناء إعادة التشغيل (515)
        if (statusCode === DisconnectReason.loggedOut || statusCode === 401) {
          currentPairingCode = null;
          console.log("🧹 تم تسجيل الخروج (401).. جاري مسح الجلسة القديمة وتوليد كود جديد...");
          try {
            fs.rmSync(SESSION_DIR, { recursive: true, force: true });
          } catch (e) {}
          setTimeout(() => connectToWhatsApp(), 3000);
        } else {
          setTimeout(() => connectToWhatsApp(), 3000);
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
      }
    });
  } catch (err) {
    isConnecting = false;
    console.log("⚠️ خطأ أثناء تهيئة الاتصال، إعادة المحاولة خلال 4 ثوانٍ...", err.message);
    setTimeout(() => connectToWhatsApp(), 4000);
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

  // التحقق من الرقم وجلب الـ JID الرسمي ومفاتيح التشفير الخاصة بالعميل
  try {
    const [result] = await sock.onWhatsApp(cleanPhone);
    if (result && result.exists && result.jid) {
      targetJid = result.jid;
    }
  } catch (e) {}

  // محاكاة الكتابة لمدة ثانية ونصف لتنشيط قناة التشفير ومنع الحظر
  try {
    await sock.presenceSubscribe(targetJid);
    await sock.sendPresenceUpdate("composing", targetJid);
    await delay(1500);
    await sock.sendPresenceUpdate("paused", targetJid);
  } catch (e) {}

  const sentMsg = await sock.sendMessage(targetJid, { text: textMessage });
  if (sentMsg?.key?.id && sentMsg.message) {
    messageStore.set(sentMsg.key.id, sentMsg.message);
  }
  return cleanPhone;
}

// مشغل طابور الإرسال المتسلسل لمنع تداخل الرسائل
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
      console.log(`✅ تم الإرسال بنجاح إلى: ${sentTo} (متبقي في الطابور: ${sendQueue.length})`);
    } catch (err) {
      console.log(`❌ فشل الإرسال إلى ${item.phone}:`, err.message);
    }

    if (sendQueue.length > 0) {
      const waitMs = Math.floor(Math.random() * (8000 - 5000 + 1)) + 5000;
      await delay(waitMs);
    }
  }

  isSendingQueueRunning = false;
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

app.post("/send-bulk-smart", (req, res) => {
  const { bulkData } = req.body;
  if (!bulkData || !Array.isArray(bulkData)) {
    return res.status(400).json({ error: "البيانات ناقصة!" });
  }
  if (!isConnected) {
    return res.status(503).json({ error: "الواتساب غير متصل بالسيرفر حالياً!" });
  }

  bulkData.forEach((item) => {
    if (item.phone && item.message) {
      sendQueue.push({ phone: item.phone, message: item.message });
    }
  });

  res.status(200).json({ status: "queued", added: bulkData.length, totalInQueue: sendQueue.length });
  processSendQueue();
});

app.post("/send-bulk", (req, res) => {
  const { numbers, message } = req.body;
  if (!numbers || !Array.isArray(numbers) || !message) {
    return res.status(400).send("البيانات ناقصة!");
  }
  if (!isConnected) {
    return res.status(503).send("الواتساب غير متصل!");
  }

  numbers.forEach((num) => sendQueue.push({ phone: num, message }));
  res.status(200).send("تمت الإضافة لطابور الإرسال");
  processSendQueue();
});

const PORT = process.env.PORT || 8080;
app.listen(PORT, () => {
  console.log(`🌐 Server listening on port ${PORT}`);
});
