const crypto = require("node:crypto");
const fs = require("node:fs");
const http = require("node:http");
const path = require("node:path");
const { promisify } = require("node:util");
const admin = require("firebase-admin");
const express = require("express");
const { Server: SocketIOServer } = require("socket.io");
const { WebSocket, WebSocketServer } = require("ws");

const serviceAccountJson = process.env.FIREBASE_SERVICE_ACCOUNT_JSON;
if (!serviceAccountJson) {
  throw new Error("FIREBASE_SERVICE_ACCOUNT_JSON environment variable is required");
}
const serviceAccount = JSON.parse(serviceAccountJson);
admin.initializeApp({
  credential: admin.credential.cert(serviceAccount),
  databaseURL: process.env.FIREBASE_DATABASE_URL ||
    `https://${serviceAccount.project_id}-default-rtdb.firebaseio.com`
});

const PORT = Number(process.env.PORT) || 3000;
const ADMIN_PLAYER_ID = (process.env.ADMIN_PLAYER_ID || "").trim().toUpperCase();
const FRONTEND_ORIGINS = new Set((process.env.FRONTEND_ORIGINS || "").split(",")
  .map(origin => origin.trim()).filter(Boolean).map(origin => new URL(origin).origin));
const ROOM_LIMIT = 500;
const RACE_LENGTH_MS = 10_000;
const MATCHMAKING_TIMEOUT_MS = 3_000;
const ECONOMY_RACE_DURATIONS = new Set([60_000, 180_000, 300_000, 600_000]);
const ECONOMY_UPGRADES = {
  tap: { baseCost: 10, maxLevel: 18 },
  auto: { baseCost: 30, maxLevel: 18 }
};
const INVITE_TTL_MS = 60_000;
const MAX_TAPS_PER_SECOND = 20;
const ROOM_CHARACTERS = "ABCDEFGHJKLMNPQRSTUVWXYZ23456789";
const PLAYER_ID_CHARACTERS = "ABCDEFGHJKLMNPQRSTUVWXYZ23456789";
const RACE_TYPES = new Set(["click", "economy"]);
const SESSION_TTL_MS = 7 * 24 * 60 * 60 * 1000;
const SESSION_COOKIE = "aura_session";
const PASSWORD_BYTES = 64;
const scrypt = promisify(crypto.scrypt);
const COSMETIC_IDS = new Set([
  "mint", "sunset", "ice", "galaxy", "forest", "candy", "aurora", "ocean", "volcano", "cloud",
  "midnight", "peach", "cyber", "desert", "prism", "berry", "sakura", "storm", "space",
  "bolt", "smile", "cat", "star", "ghost", "frog", "rocket", "dragon", "heart", "panda",
  "penguin", "unicorn", "pizza", "avocado", "shark", "koala", "fox", "owl", "turtle",
  "cherry", "cookie", "sun", "moon", "robot", "alien", "crown", "fire", "snowflake",
  "watermelon", "tiger", "whale", "chicken", "cupcake", "butterfly", "dolphin", "leaf",
  "music", "diamond", "rocketship", "poop", "rainbow-poop", "mystery-poop"
]);
const BACKGROUND_IDS = new Set([
  "mint", "sunset", "ice", "galaxy", "forest", "candy", "aurora", "ocean", "volcano", "cloud",
  "midnight", "peach", "cyber", "desert", "prism", "berry", "sakura", "storm", "space"
]);
const TITLE_IDS = new Set([
  "newbie", "clicker", "star-chaser", "aura-hero", "legend", "myth-maker"
]);
const rooms = new Map();
const onlineClients = new Map();
const matchmakingQueue = [];
const gamePage = fs.readFileSync(path.join(__dirname, "index.html"));
const backendConfig = fs.readFileSync(path.join(__dirname, "backend-config.js"));
const legacyAccountsFile = path.join(process.env.AURA_DATA_DIR || path.join(__dirname, "data"), "accounts.json");
const accountsReference = admin.database().ref("emojiClicker/accounts");
const authSessions = new Map();
const authRateLimits = new Map();
let accountStore;
let storeWriteQueue = Promise.resolve();
const accountMaintenanceTimer = setInterval(() => {
  const now = Date.now();
  authSessions.forEach((session, token) => {
    if (session.expiresAt <= now) authSessions.delete(token);
  });
  authRateLimits.forEach((limit, key) => {
    if (limit.resetAt <= now) authRateLimits.delete(key);
  });
}, 10 * 60_000);
accountMaintenanceTimer.unref();

async function loadAccountStore() {
  const snapshot = await accountsReference.get();
  const stored = snapshot.val();
  if (stored === null) {
    if (fs.existsSync(legacyAccountsFile)) {
      const legacyStore = JSON.parse(fs.readFileSync(legacyAccountsFile, "utf8"));
      if (!legacyStore || typeof legacyStore !== "object" || !legacyStore.accounts ||
          typeof legacyStore.accounts !== "object" || Array.isArray(legacyStore.accounts)) {
        throw new Error("The legacy account data file has an invalid format.");
      }
      await accountsReference.set(legacyStore);
      return legacyStore;
    }
    const emptyStore = { accounts: {} };
    await accountsReference.set(emptyStore);
    return emptyStore;
  }
  if (!stored || typeof stored !== "object" || !stored.accounts ||
      typeof stored.accounts !== "object" || Array.isArray(stored.accounts)) {
    throw new Error("The Firebase account data has an invalid format.");
  }
  return stored;
}

function createPlayerId(accounts) {
  const existing = new Set(Object.values(accounts).map(account => account.id));
  let id;
  do {
    id = "EC-" + Array.from({ length: 8 }, () =>
      PLAYER_ID_CHARACTERS[crypto.randomInt(PLAYER_ID_CHARACTERS.length)]).join("");
  } while (existing.has(id));
  return id;
}

async function migrateAccountStore() {
  const accounts = { ...accountStore.accounts };
  const usedIds = new Set();
  let changed = false;
  for (const [key, account] of Object.entries(accounts)) {
    const nextAccount = { ...account };
    if (typeof nextAccount.id !== "string" || !/^EC-[A-HJ-NP-Z2-9]{8}$/.test(nextAccount.id) || usedIds.has(nextAccount.id)) {
      nextAccount.id = createPlayerId({ ...accounts, ...Object.fromEntries([...usedIds].map(id => [id, { id }])) });
      changed = true;
    }
    usedIds.add(nextAccount.id);
    for (const field of ["friendIds", "incomingRequests", "outgoingRequests"]) {
      if (!Array.isArray(nextAccount[field])) {
        nextAccount[field] = [];
        changed = true;
      }
    }
    accounts[key] = nextAccount;
  }
  const validIds = new Set(Object.values(accounts).map(account => account.id));
  for (const account of Object.values(accounts)) {
    for (const field of ["friendIds", "incomingRequests", "outgoingRequests"]) {
      const clean = [...new Set(account[field].filter(id =>
        typeof id === "string" && validIds.has(id) && id !== account.id))];
      if (clean.length !== account[field].length) changed = true;
      account[field] = clean;
    }
  }
  if (changed) await persistAccountStore({ accounts });
}

async function persistAccountStore(nextStore) {
  await accountsReference.set(nextStore);
  accountStore = nextStore;
}

function serializeStoreWrite(operation) {
  const result = storeWriteQueue.then(operation);
  storeWriteQueue = result.then(() => undefined, () => undefined);
  return result;
}

function sendJson(response, status, data, extraHeaders = {}) {
  response.writeHead(status, {
    "Content-Type": "application/json; charset=utf-8",
    "Cache-Control": "no-store",
    ...extraHeaders
  });
  response.end(JSON.stringify(data));
}

function sessionCookie(value, maxAge, request) {
  const secure = process.env.NODE_ENV === "production" || (request && isSecureRequest(request)) ? "; Secure" : "";
  const sameSite = process.env.NODE_ENV === "production" ? "None" : "Strict";
  return SESSION_COOKIE + "=" + value + "; HttpOnly; SameSite=" + sameSite + "; Path=/; Max-Age=" + maxAge + secure;
}

function sessionFor(request) {
  const authorization = request.headers.authorization || "";
  const bearer = authorization.match(/^Bearer ([a-f0-9]{64})$/i);
  const webSocketProtocols = (request.headers["sec-websocket-protocol"] || "").split(",").map(value => value.trim());
  const cookieHeader = request.headers.cookie || "";
  const cookie = cookieHeader.split(";").map(value => value.trim())
    .find(value => value.startsWith(SESSION_COOKIE + "="));
  const token = bearer ? bearer[1] :
    (webSocketProtocols.length === 2 && webSocketProtocols[0] === "aura-auth" ? webSocketProtocols[1] : "") ||
    (cookie ? cookie.slice(SESSION_COOKIE.length + 1) : "");
  if (!/^[a-f0-9]{64}$/i.test(token)) return null;
  const session = authSessions.get(token);
  if (!session) return null;
  if (session.expiresAt <= Date.now()) {
    authSessions.delete(token);
    return null;
  }
  return { token, ...session };
}

function usernameForRequest(request) {
  const session = sessionFor(request);
  if (!session) return null;
  const account = accountStore.accounts[session.username.toLowerCase()];
  return account && !account.banned ? account.username : null;
}

function accountForRequest(request) {
  const username = usernameForRequest(request);
  return username ? accountStore.accounts[username.toLowerCase()] : null;
}

function isAdminAccount(account) {
  return Boolean(ADMIN_PLAYER_ID && account && account.id === ADMIN_PLAYER_ID);
}

function requireAdmin(request, response) {
  const account = accountForRequest(request);
  if (!account) {
    sendJson(response, 401, { error: "Sign in to use the admin panel." });
    return null;
  }
  if (!isAdminAccount(account)) {
    sendJson(response, 403, { error: "This account does not have admin access." });
    return null;
  }
  return account;
}

function validUsername(value) {
  return typeof value === "string" && /^[A-Za-z0-9_]{3,20}$/.test(value);
}

function validPassword(value) {
  return typeof value === "string" && [...value].length >= 12 &&
    [...value].length <= 128 && Buffer.byteLength(value, "utf8") <= 512;
}

function validateSave(save) {
  if (!save || typeof save !== "object" || Array.isArray(save)) return null;
  const numberKeys = [
    "chips", "total", "tapPower", "fingerLevel", "doubleLevel", "autoLevel",
    "superAutoLevel", "autoPower", "level"
  ];
  const clean = {};
  for (const key of numberKeys) {
    if (!Number.isSafeInteger(save[key]) || save[key] < 0) return null;
    clean[key] = save[key];
  }
  if (!Array.isArray(save.upgradeLevels) || save.upgradeLevels.length > 100 ||
      save.upgradeLevels.some(level => !Number.isSafeInteger(level) || level < 0)) return null;
  clean.upgradeLevels = save.upgradeLevels.slice();
  for (const key of ["ownedSkins", "guestSkins"]) {
    if (!Array.isArray(save[key]) || save[key].length > COSMETIC_IDS.size ||
        save[key].some(id => typeof id !== "string" || !COSMETIC_IDS.has(id))) return null;
    clean[key] = [...new Set(save[key])];
  }
  if (typeof save.activeSkin !== "string" || !BACKGROUND_IDS.has(save.activeSkin) ||
      typeof save.activeEmoji !== "string" || !COSMETIC_IDS.has(save.activeEmoji) ||
      BACKGROUND_IDS.has(save.activeEmoji)) return null;
  if (!clean.ownedSkins.includes(save.activeSkin) || !clean.ownedSkins.includes(save.activeEmoji)) return null;
  clean.activeSkin = save.activeSkin;
  clean.activeEmoji = save.activeEmoji;
  if (!Array.isArray(save.claimedAchievements) || save.claimedAchievements.length > 100 ||
      save.claimedAchievements.some(id => typeof id !== "string" || !/^[a-z0-9-]{1,40}$/.test(id))) return null;
  clean.claimedAchievements = [...new Set(save.claimedAchievements)];
  if (!Array.isArray(save.friendMatchWins) || save.friendMatchWins.length !== 2 ||
      save.friendMatchWins.some(wins => !Number.isSafeInteger(wins) || wins < 0)) return null;
  clean.friendMatchWins = save.friendMatchWins.slice();
  if (!Array.isArray(save.ownedTitles) || save.ownedTitles.length > TITLE_IDS.size ||
      save.ownedTitles.some(id => typeof id !== "string" || !TITLE_IDS.has(id))) return null;
  clean.ownedTitles = [...new Set(save.ownedTitles)];
  if (!clean.ownedTitles.includes("newbie") || typeof save.activeTitle !== "string" ||
      !clean.ownedTitles.includes(save.activeTitle)) return null;
  clean.activeTitle = save.activeTitle;
  if (!save.settings || typeof save.settings !== "object" || Array.isArray(save.settings) ||
      ["animatedScenery", "reducedMotion", "confirmBets"].some(key => typeof save.settings[key] !== "boolean")) return null;
  clean.settings = {
    animatedScenery: save.settings.animatedScenery,
    reducedMotion: save.settings.reducedMotion,
    confirmBets: save.settings.confirmBets
  };
  clean.profileName = "";
  return clean;
}

async function readJsonBody(request) {
  const chunks = [];
  let bytes = 0;
  for await (const chunk of request) {
    bytes += chunk.length;
    if (bytes > 64 * 1024) throw Object.assign(new Error("Request is too large."), { statusCode: 413 });
    chunks.push(chunk);
  }
  try {
    return JSON.parse(Buffer.concat(chunks).toString("utf8"));
  } catch {
    throw Object.assign(new Error("Please send valid JSON."), { statusCode: 400 });
  }
}

function sameOrigin(request) {
  const origin = request.headers.origin;
  if (!origin) return false;
  try {
    const parsedOrigin = new URL(origin);
    if (parsedOrigin.origin !== origin || !["https:", "http:"].includes(parsedOrigin.protocol)) return false;
    return parsedOrigin.host === request.headers.host || FRONTEND_ORIGINS.has(parsedOrigin.origin);
  } catch {
    return false;
  }
}

function isLoopback(address) {
  return address === "::1" || address === "127.0.0.1" ||
    (typeof address === "string" && address.startsWith("127.")) ||
    address === "::ffff:127.0.0.1" ||
    (typeof address === "string" && address.startsWith("::ffff:127."));
}

function isSecureRequest(request) {
  if (request.socket.encrypted) return true;
  if (process.env.TRUST_PROXY === "1" && request.headers["x-forwarded-proto"] === "https") return true;
  return process.env.NODE_ENV !== "production" && isLoopback(request.socket.remoteAddress);
}

function rateLimited(request, key, maxRequests, windowMs) {
  const now = Date.now();
  const ip = request.socket.remoteAddress || "unknown";
  const mapKey = key + ":" + ip;
  const record = authRateLimits.get(mapKey);
  if (!record || record.resetAt <= now) {
    authRateLimits.set(mapKey, { count: 1, resetAt: now + windowMs });
    return false;
  }
  record.count += 1;
  return record.count > maxRequests;
}

function createSession(username, request) {
  const token = crypto.randomBytes(32).toString("hex");
  const expiresAt = Date.now() + SESSION_TTL_MS;
  authSessions.set(token, { username, expiresAt });
  return { token, cookie: sessionCookie(token, SESSION_TTL_MS / 1000, request) };
}

async function handleApi(request, response, pathname) {
  if (!isSecureRequest(request)) {
    return sendJson(response, 426, { error: "Online accounts require HTTPS. Ask an adult to set up secure hosting." });
  }
  if (pathname === "/api/auth/me" && request.method === "GET") {
    const username = usernameForRequest(request);
    if (!username) return sendJson(response, 401, { error: "Sign in to use a cloud account." });
    const account = accountStore.accounts[username.toLowerCase()];
    if (!account) return sendJson(response, 401, { error: "Please sign in again." });
    return sendJson(response, 200, { username: account.username, playerId: account.id });
  }
  if (pathname === "/api/admin/status" && request.method === "GET") {
    const account = accountForRequest(request);
    if (!account) return sendJson(response, 401, { error: "Sign in to check admin access." });
    return sendJson(response, 200, { isAdmin: isAdminAccount(account) });
  }
  if (pathname === "/api/admin/accounts" && request.method === "GET") {
    if (!requireAdmin(request, response)) return;
    const search = (new URL(request.url, "http://localhost").searchParams.get("search") || "").trim().toLowerCase();
    const accounts = Object.values(accountStore.accounts)
      .filter(account => !search || account.username.toLowerCase().includes(search) || account.id.toLowerCase().includes(search))
      .sort((first, second) => first.username.localeCompare(second.username))
      .slice(0, 100)
      .map(account => ({
        username: account.username,
        playerId: account.id,
        banned: Boolean(account.banned),
        aura: account.save.chips,
        total: account.save.total,
        cosmetics: account.save.ownedSkins.length
      }));
    return sendJson(response, 200, { accounts });
  }
  if (pathname === "/api/admin/account" && request.method === "GET") {
    if (!requireAdmin(request, response)) return;
    const playerId = (new URL(request.url, "http://localhost").searchParams.get("playerId") || "").trim().toUpperCase();
    const account = Object.values(accountStore.accounts).find(item => item.id === playerId);
    if (!account) return sendJson(response, 404, { error: "No account has that player ID." });
    return sendJson(response, 200, {
      username: account.username,
      playerId: account.id,
      banned: Boolean(account.banned),
      save: account.save
    });
  }

  if (request.method === "GET" && pathname === "/api/save") {
    const username = usernameForRequest(request);
    if (!username) return sendJson(response, 401, { error: "Sign in to load your cloud save." });
    const account = accountStore.accounts[username.toLowerCase()];
    if (!account) return sendJson(response, 401, { error: "Please sign in again." });
    return sendJson(response, 200, { save: account.save });
  }

  if (pathname === "/api/friends" && request.method === "GET") {
    const username = usernameForRequest(request);
    if (!username) return sendJson(response, 401, { error: "Sign in to use your friends list." });
    const account = accountStore.accounts[username.toLowerCase()];
    if (!account) return sendJson(response, 401, { error: "Please sign in again." });
    const summary = id => {
      const friend = Object.values(accountStore.accounts).find(item => item.id === id);
      return friend ? { id: friend.id, username: friend.username } : null;
    };
    return sendJson(response, 200, {
      playerId: account.id,
      friends: account.friendIds.map(summary).filter(Boolean),
      incoming: account.incomingRequests.map(summary).filter(Boolean),
      outgoing: account.outgoingRequests.map(summary).filter(Boolean)
    });
  }
  if (pathname === "/api/leaderboard" && request.method === "GET") {
    const entries = Object.values(accountStore.accounts)
      .map(account => ({
        id: account.id,
        username: account.username,
        total: account.save.total,
        level: Math.floor(account.save.chips / 50) + 1
      }))
      .sort((first, second) => second.total - first.total || first.username.localeCompare(second.username))
      .slice(0, 50)
      .map((entry, index) => ({ rank: index + 1, ...entry }));
    return sendJson(response, 200, { entries });
  }

  if (!pathname.startsWith("/api/")) return false;
  if (request.method !== "POST") return sendJson(response, 405, { error: "That action is not available." }, { Allow: "GET, POST" });
  if (!sameOrigin(request)) return sendJson(response, 403, { error: "That request was blocked. Reload the game and try again." });
  if (!(request.headers["content-type"] || "").toLowerCase().startsWith("application/json")) {
    return sendJson(response, 415, { error: "Please send a JSON request." });
  }

  const body = await readJsonBody(request);
  if (!body || typeof body !== "object" || Array.isArray(body)) {
    return sendJson(response, 400, { error: "Please send a valid account request." });
  }
  if (pathname === "/api/admin/action") {
    const adminAccount = requireAdmin(request, response);
    if (!adminAccount) return;
    if (rateLimited(request, "admin-actions", 60, 5 * 60_000)) {
      return sendJson(response, 429, { error: "Too many admin actions. Wait a little and try again." });
    }
    const action = body.action;
    const targetId = typeof body.playerId === "string" ? body.playerId.trim().toUpperCase() : "";
    if (!/^EC-[A-HJ-NP-Z2-9]{8}$/.test(targetId)) {
      return sendJson(response, 400, { error: "Select a valid player account." });
    }
    return serializeStoreWrite(async () => {
      const entry = Object.entries(accountStore.accounts).find(([, account]) => account.id === targetId);
      if (!entry) return sendJson(response, 404, { error: "No account has that player ID." });
      const [key, account] = entry;
      if (["ban", "delete"].includes(action) &&
          (account.id === adminAccount.id || account.id === ADMIN_PLAYER_ID)) {
        return sendJson(response, 400, { error: "The configured admin account cannot be disabled or deleted." });
      }
      if (action === "adjust-aura") {
        const amount = body.amount;
        if (!Number.isSafeInteger(amount) || amount === 0 ||
            !Number.isSafeInteger(account.save.chips + amount) || account.save.chips + amount < 0) {
          return sendJson(response, 400, { error: "Enter a non-zero whole Aura adjustment that keeps the balance at zero or above." });
        }
        const save = { ...account.save, chips: account.save.chips + amount };
        await persistAccountStore({
          accounts: { ...accountStore.accounts, [key]: { ...account, save } }
        });
        return sendJson(response, 200, { ok: true, message: "Aura balance updated.", aura: save.chips, save });
      }
      if (action === "set-aura") {
        const amount = body.amount;
        if (!Number.isSafeInteger(amount) || amount < 0) {
          return sendJson(response, 400, { error: "Enter a whole Aura balance of zero or more." });
        }
        const save = { ...account.save, chips: amount };
        await persistAccountStore({
          accounts: { ...accountStore.accounts, [key]: { ...account, save } }
        });
        return sendJson(response, 200, { ok: true, message: "Aura balance set.", aura: save.chips, save });
      }
      if (action === "grant-all-cosmetics") {
        const save = { ...account.save, ownedSkins: [...COSMETIC_IDS] };
        await persistAccountStore({
          accounts: { ...accountStore.accounts, [key]: { ...account, save } }
        });
        return sendJson(response, 200, {
          ok: true,
          message: "All emoji skins and backgrounds granted.",
          ...(account.id === adminAccount.id ? { save } : {})
        });
      }
      if (action === "ban" || action === "unban") {
        const banned = action === "ban";
        await persistAccountStore({
          accounts: { ...accountStore.accounts, [key]: { ...account, banned } }
        });
        if (banned) {
          authSessions.forEach((session, token) => {
            if (session.username.toLowerCase() === key) authSessions.delete(token);
          });
          const client = onlineClients.get(account.id);
          if (client) client.close(4003, "Account disabled.");
        }
        return sendJson(response, 200, { ok: true, message: banned ? "Account disabled." : "Account restored." });
      }
      if (action === "delete") {
        const accounts = { ...accountStore.accounts };
        delete accounts[key];
        for (const [otherKey, otherAccount] of Object.entries(accounts)) {
          accounts[otherKey] = {
            ...otherAccount,
            friendIds: otherAccount.friendIds.filter(id => id !== targetId),
            incomingRequests: otherAccount.incomingRequests.filter(id => id !== targetId),
            outgoingRequests: otherAccount.outgoingRequests.filter(id => id !== targetId)
          };
        }
        await persistAccountStore({ accounts });
        authSessions.forEach((session, token) => {
          if (session.username.toLowerCase() === key) authSessions.delete(token);
        });
        const client = onlineClients.get(targetId);
        if (client) client.close(4003, "Account deleted.");
        return sendJson(response, 200, { ok: true, message: "Account deleted." });
      }
      return sendJson(response, 400, { error: "Choose a supported admin action." });
    });
  }
  if (pathname === "/api/auth/signup" || pathname === "/api/auth/login") {
    const signup = pathname.endsWith("/signup");
    if (rateLimited(request, signup ? "signup" : "login", signup ? 5 : 20, signup ? 60 * 60_000 : 15 * 60_000)) {
      return sendJson(response, 429, { error: "Too many attempts. Wait a while and try again." });
    }
    if (!validUsername(body.username)) {
      return sendJson(response, 400, { error: "Username must be 3–20 characters using letters, numbers, or _." });
    }
    const username = body.username.toLowerCase();
    if (signup) {
      if (!validPassword(body.password)) {
        return sendJson(response, 400, { error: "Use a unique password with 12–128 characters." });
      }
      const save = validateSave(body.save);
      if (!save) return sendJson(response, 400, { error: "The local game save could not be uploaded. Please reload and try again." });
      const salt = crypto.randomBytes(16);
      const passwordHash = await scrypt(body.password, salt, PASSWORD_BYTES);
      await serializeStoreWrite(async () => {
        if (accountStore.accounts[username]) return false;
        const nextStore = {
          accounts: {
            ...accountStore.accounts,
            [username]: {
              username: body.username,
              id: createPlayerId(accountStore.accounts),
              friendIds: [],
              incomingRequests: [],
              outgoingRequests: [],
              salt: salt.toString("hex"),
              passwordHash: passwordHash.toString("hex"),
              save
            }
          }
        };
        await persistAccountStore(nextStore);
        return true;
      }).then(createdAccount => {
        if (!createdAccount) return sendJson(response, 409, { error: "That username is not available. Choose another." });
        const savedAccount = accountStore.accounts[username];
        const session = createSession(body.username, request);
        return sendJson(response, 201, {
          username: body.username,
          playerId: savedAccount.id,
          sessionToken: session.token
        }, { "Set-Cookie": session.cookie });
      });
      return;
    }
    if (typeof body.password !== "string" || Buffer.byteLength(body.password, "utf8") > 512) {
      return sendJson(response, 401, { error: "Username or password is incorrect." });
    }
    const account = accountStore.accounts[username];
    const salt = account ? Buffer.from(account.salt, "hex") : Buffer.alloc(16, 0);
    const expected = account ? Buffer.from(account.passwordHash, "hex") : Buffer.alloc(PASSWORD_BYTES, 0);
    const actual = await scrypt(body.password, salt, PASSWORD_BYTES);
    const matches = crypto.timingSafeEqual(actual, expected);
    if (!account || !matches) return sendJson(response, 401, { error: "Username or password is incorrect." });
    if (account.banned) return sendJson(response, 403, { error: "This account is disabled. Contact the game owner." });
    const session = createSession(account.username, request);
    return sendJson(response, 200, {
      username: account.username,
      playerId: account.id,
      save: account.save,
      sessionToken: session.token
    }, { "Set-Cookie": session.cookie });
  }

  const username = usernameForRequest(request);
  if (!username) return sendJson(response, 401, { error: "Sign in to use a cloud account." });
  if (pathname === "/api/auth/logout") {
    const session = sessionFor(request);
    if (session) authSessions.delete(session.token);
    return sendJson(response, 200, { ok: true }, { "Set-Cookie": sessionCookie("", 0, request) });
  }
  if (pathname === "/api/save") {
    const save = validateSave(body.save);
    if (!save) return sendJson(response, 400, { error: "That cloud save was not accepted." });
    return serializeStoreWrite(async () => {
      const normalized = username.toLowerCase();
      const account = accountStore.accounts[normalized];
      if (!account) return sendJson(response, 401, { error: "Please sign in again." });
      const nextStore = {
        accounts: {
          ...accountStore.accounts,
          [normalized]: { ...account, save }
        }
      };
      await persistAccountStore(nextStore);
      return sendJson(response, 200, { ok: true });
    });
  }
  const friendActions = new Map([
    ["/api/friends/request", "request"],
    ["/api/friends/accept", "accept"],
    ["/api/friends/decline", "decline"],
    ["/api/friends/cancel", "cancel"],
    ["/api/friends/remove", "remove"]
  ]);
  const friendAction = friendActions.get(pathname);
  if (friendAction) {
    if (rateLimited(request, "friend-actions", 40, 5 * 60_000)) {
      return sendJson(response, 429, { error: "Too many friend-list actions. Wait a little and try again." });
    }
    const playerId = typeof body.friendId === "string" ? body.friendId.trim().toUpperCase() : "";
    if (!/^EC-[A-HJ-NP-Z2-9]{8}$/.test(playerId)) {
      return sendJson(response, 400, { error: "Enter a valid Emoji Clicker player ID." });
    }
    const result = await serializeStoreWrite(async () => {
      const accounts = { ...accountStore.accounts };
      const myKey = username.toLowerCase();
      const me = accounts[myKey] && { ...accounts[myKey] };
      const friendEntry = Object.entries(accounts).find(([, account]) => account.id === playerId);
      if (!me) return { status: 401, error: "Please sign in again." };
      if (!friendEntry) return { status: 404, error: "No account has that player ID." };
      const [friendKey, storedFriend] = friendEntry;
      if (friendKey === myKey) return { status: 400, error: "You cannot add your own account." };
      const friend = { ...storedFriend };
      me.friendIds = [...me.friendIds];
      me.incomingRequests = [...me.incomingRequests];
      me.outgoingRequests = [...me.outgoingRequests];
      friend.friendIds = [...friend.friendIds];
      friend.incomingRequests = [...friend.incomingRequests];
      friend.outgoingRequests = [...friend.outgoingRequests];
      const removeId = (items, id) => items.filter(item => item !== id);
      let message = "Friend-list action complete.";

      if (friendAction === "request") {
        if (me.friendIds.includes(friend.id)) return { status: 409, error: "You are already friends." };
        if (me.incomingRequests.includes(friend.id)) return { status: 409, error: "They already sent you a request. Accept it from your friend requests." };
        if (me.outgoingRequests.includes(friend.id)) return { status: 409, error: "Your friend request is already waiting." };
        me.outgoingRequests.push(friend.id);
        friend.incomingRequests.push(me.id);
        message = "Friend request sent.";
      } else if (friendAction === "accept") {
        if (!me.incomingRequests.includes(friend.id) || !friend.outgoingRequests.includes(me.id)) {
          return { status: 409, error: "That friend request is no longer waiting." };
        }
        me.friendIds.push(friend.id);
        friend.friendIds.push(me.id);
        me.incomingRequests = removeId(me.incomingRequests, friend.id);
        friend.outgoingRequests = removeId(friend.outgoingRequests, me.id);
        me.outgoingRequests = removeId(me.outgoingRequests, friend.id);
        friend.incomingRequests = removeId(friend.incomingRequests, me.id);
        message = "Friend request accepted.";
      } else if (friendAction === "decline") {
        if (!me.incomingRequests.includes(friend.id)) return { status: 409, error: "That friend request is no longer waiting." };
        me.incomingRequests = removeId(me.incomingRequests, friend.id);
        friend.outgoingRequests = removeId(friend.outgoingRequests, me.id);
        message = "Friend request declined.";
      } else if (friendAction === "cancel") {
        if (!me.outgoingRequests.includes(friend.id)) return { status: 409, error: "That outgoing request is no longer waiting." };
        me.outgoingRequests = removeId(me.outgoingRequests, friend.id);
        friend.incomingRequests = removeId(friend.incomingRequests, me.id);
        message = "Friend request cancelled.";
      } else {
        if (!me.friendIds.includes(friend.id)) return { status: 409, error: "That player is not on your friends list." };
        me.friendIds = removeId(me.friendIds, friend.id);
        friend.friendIds = removeId(friend.friendIds, me.id);
        message = "Friend removed.";
      }

      accounts[myKey] = me;
      accounts[friendKey] = friend;
      await persistAccountStore({ accounts });
      return { status: 200, message };
    });
    if (result.status === 200) {
      const friendClient = onlineClients.get(playerId);
      if (friendClient) send(friendClient, { type: "friend-list-changed", action: friendAction });
    }
    return sendJson(response, result.status, result.status === 200 ? { ok: true, message: result.message } : { error: result.error });
  }
  return sendJson(response, 404, { error: "That game action was not found." });
}

const app = express();
app.use((request, response) => {
  const pathname = new URL(request.url, "http://localhost").pathname;
  const secureHeaders = {
    "Content-Security-Policy": "default-src 'self'; style-src 'self' 'unsafe-inline'; script-src 'self' 'unsafe-inline'; connect-src 'self' ws: wss:; img-src 'self' data:; object-src 'none'; base-uri 'self'; frame-ancestors 'none'",
    "Referrer-Policy": "no-referrer",
    "X-Content-Type-Options": "nosniff",
    "X-Frame-Options": "DENY"
  };
  if (pathname === "/healthz" && request.method === "GET") {
    return sendJson(response, 200, { status: "ok" });
  }
  if (pathname === "/backend-config.js" && request.method === "GET") {
    response.writeHead(200, {
      "Content-Type": "application/javascript; charset=utf-8",
      "Cache-Control": "no-store",
      ...secureHeaders
    });
    response.end(backendConfig);
    return;
  }
  if (pathname.startsWith("/api/")) {
    const origin = request.headers.origin;
    if (origin && !sameOrigin(request)) {
      return sendJson(response, 403, { error: "That website is not allowed to use this account server." });
    }
    if (origin) {
      response.setHeader("Access-Control-Allow-Origin", origin);
      response.setHeader("Access-Control-Allow-Credentials", "true");
      response.setHeader("Access-Control-Allow-Methods", "GET, POST, OPTIONS");
      response.setHeader("Access-Control-Allow-Headers", "Content-Type, Authorization");
      response.setHeader("Vary", "Origin");
    }
    if (request.method === "OPTIONS") {
      response.writeHead(204);
      response.end();
      return;
    }
    void handleApi(request, response, pathname).then(result => {
      if (result === false && !response.headersSent) sendJson(response, 404, { error: "That game action was not found." });
    }).catch(error => {
      console.error("Game account API error:", error.message);
      if (!response.headersSent) sendJson(response, error.statusCode || 500, {
        error: error.statusCode ? error.message : "The account service could not finish that action. Please try again."
      });
      else if (!response.writableEnded) response.end();
    });
    return;
  }
  if (request.method !== "GET" || !["/", "/index.html"].includes(pathname)) {
    response.writeHead(404, { "Content-Type": "text/plain; charset=utf-8" });
    response.end("Not found");
    return;
  }

  response.writeHead(200, {
    "Content-Type": "text/html; charset=utf-8",
    "Cache-Control": "no-store",
    ...secureHeaders
  });
  response.end(gamePage);
});

const server = http.createServer(app);
const io = new SocketIOServer(server, { maxHttpBufferSize: 2048 });
const multiplayerPlayers = new Map();

io.on("connection", socket => {
  const player = { id: socket.id, x: 0, y: 0 };
  multiplayerPlayers.set(socket.id, player);
  socket.emit("players", Array.from(multiplayerPlayers.values()));
  socket.broadcast.emit("playerJoined", player);

  socket.on("move", position => {
    if (!position || typeof position !== "object" ||
        !Number.isFinite(position.x) || !Number.isFinite(position.y)) {
      socket.emit("movementError", { message: "Position must include finite x and y numbers." });
      return;
    }

    player.x = position.x;
    player.y = position.y;
    socket.broadcast.emit("playerMoved", player);
  });

  socket.on("disconnect", () => {
    multiplayerPlayers.delete(socket.id);
    socket.broadcast.emit("playerLeft", socket.id);
  });
});

const webSockets = new WebSocketServer({
  server,
  path: "/multiplayer",
  maxPayload: 16 * 1024,
  perMessageDeflate: false,
  handleProtocols: protocols => protocols.has("aura-auth") ? "aura-auth" : false,
  verifyClient: ({ origin, req }, done) => {
    if (!isSecureRequest(req) || !usernameForRequest(req)) {
      done(false, 401, "Sign in to play online.");
      return;
    }
    if (!origin) {
      done(true);
      return;
    }
    done(sameOrigin({ headers: { origin, host: req.headers.host } }), 403, "Origin not allowed");
  }
});

const webSocketHeartbeat = setInterval(() => {
  webSockets.clients.forEach(client => {
    if (client.readyState !== WebSocket.OPEN) return;
    if (client.isAlive === false) {
      client.terminate();
      return;
    }
    client.isAlive = false;
    client.ping();
  });
}, 30_000);
webSocketHeartbeat.unref();

function send(client, message) {
  if (client.readyState === WebSocket.OPEN) client.send(JSON.stringify(message));
}

function broadcast(room, message) {
  room.players.forEach(player => send(player, message));
}

function createRoomCode() {
  let code;
  do {
    code = Array.from({ length: 6 }, () => ROOM_CHARACTERS[crypto.randomInt(ROOM_CHARACTERS.length)]).join("");
  } while (rooms.has(code));
  return code;
}

function roomFor(client) {
  return client.roomCode ? rooms.get(client.roomCode) : null;
}

function areFriends(firstId, secondId) {
  const first = Object.values(accountStore.accounts).find(account => account.id === firstId);
  return Boolean(first && first.friendIds.includes(secondId));
}

function removeFromMatchmaking(client) {
  const index = matchmakingQueue.indexOf(client);
  if (index !== -1) matchmakingQueue.splice(index, 1);
  if (client.matchmakingTimer) clearTimeout(client.matchmakingTimer);
  client.matchmakingTimer = null;
  client.matchmaking = false;
  client.matchmakingMode = null;
}

function createRoom(players, mode, inviteeId = null, raceType = "click") {
  if (rooms.size >= ROOM_LIMIT) return null;
  const code = createRoomCode();
  const room = {
    players,
    inventories: [[], []],
    inventoryReady: [false, false],
    scores: [0, 0],
    economy: [
      { aura: 0, earned: 0, tapPower: 1, tapLevel: 0, autoPower: 0, autoLevel: 0 },
      { aura: 0, earned: 0, tapPower: 1, tapLevel: 0, autoPower: 0, autoLevel: 0 }
    ],
    economyTimer: null,
    racing: false,
    raceTimer: null,
    inviteTimer: null,
    pendingTrade: null,
    mode,
    raceType,
    inviteeId
  };
  rooms.set(code, room);
  players.forEach((player, index) => {
    player.roomCode = code;
    player.playerNumber = index + 1;
    player.matchmaking = false;
  });
  return room;
}

function sendRoomReady(room) {
  room.players.forEach((player, index) => {
    const opponent = room.players.length === 2 ? room.players[1 - index] : null;
    send(player, {
      type: "room-ready",
      code: player.roomCode,
      player: index + 1,
      players: room.players.length,
      mode: room.mode,
      raceType: room.raceType,
      opponentId: opponent ? opponent.accountId : null,
      opponentUsername: opponent ? opponent.username : null
    });
  });
}

function sendOpponentInfo(room) {
  room.players.forEach((player, index) => {
    const opponent = room.players[1 - index];
    send(player, { type: "opponent-info", id: opponent.accountId, username: opponent.username });
  });
}

function createPracticeRoom(client, raceType) {
  const bot = {
    readyState: WebSocket.OPEN,
    accountId: "AI-PRACTICE",
    username: "Aura Bot",
    isPracticeBot: true,
    roomCode: null,
    playerNumber: 0,
    matchmaking: false,
    send() {}
  };
  const room = createRoom([client, bot], "practice", null, raceType);
  if (!room) {
    send(client, { type: "error", message: "A practice match could not start. Try searching again." });
    return;
  }
  room.inventoryReady[1] = true;
  sendRoomReady(room);
  sendOpponentInfo(room);
  send(client, { type: "practice-opponent" });
  if (raceType === "click") startRace(room);
}

function sendEconomyState(room) {
  room.players.forEach((player, index) => {
    send(player, {
      type: "economy-state",
      players: room.economy.map(({ aura, earned, tapPower, autoPower, tapLevel, autoLevel }) => ({
        aura,
        earned,
        tapPower,
        autoPower,
        tapCost: tapLevel < ECONOMY_UPGRADES.tap.maxLevel
          ? ECONOMY_UPGRADES.tap.baseCost * (2 ** tapLevel) : null,
        autoCost: autoLevel < ECONOMY_UPGRADES.auto.maxLevel
          ? ECONOMY_UPGRADES.auto.baseCost * (2 ** autoLevel) : null
      })),
      player: index + 1
    });
  });
}

function startPracticeBot(room) {
  if (!room.players[1].isPracticeBot) return;
  room.botTimer = setInterval(() => {
    if (!room.racing) return;
    if (room.raceType === "click") {
      room.scores[1] += Math.random() < 0.65 ? 1 : 2;
      broadcast(room, { type: "score", scores: room.scores });
      return;
    }
    const bot = room.economy[1];
    const autoCost = bot.autoLevel < ECONOMY_UPGRADES.auto.maxLevel
      ? ECONOMY_UPGRADES.auto.baseCost * (2 ** bot.autoLevel) : Infinity;
    const tapCost = bot.tapLevel < ECONOMY_UPGRADES.tap.maxLevel
      ? ECONOMY_UPGRADES.tap.baseCost * (2 ** bot.tapLevel) : Infinity;
    if (bot.aura >= autoCost && bot.autoLevel <= bot.tapLevel) {
      bot.aura -= autoCost;
      bot.autoPower += 1;
      bot.autoLevel += 1;
    } else if (bot.aura >= tapCost && Math.random() < 0.25) {
      bot.aura -= tapCost;
      bot.tapPower += 1;
      bot.tapLevel += 1;
    } else {
      bot.aura = Math.min(Number.MAX_SAFE_INTEGER, bot.aura + bot.tapPower);
      bot.earned = Math.min(Number.MAX_SAFE_INTEGER, bot.earned + bot.tapPower);
    }
    room.scores[1] = bot.earned;
    sendEconomyState(room);
    broadcast(room, { type: "score", scores: room.scores });
  }, 250);
}

function startRace(room, requestedDuration) {
  room.scores = [0, 0];
  room.economy = [
    { aura: 0, earned: 0, tapPower: 1, tapLevel: 0, autoPower: 0, autoLevel: 0 },
    { aura: 0, earned: 0, tapPower: 1, tapLevel: 0, autoPower: 0, autoLevel: 0 }
  ];
  room.racing = true;
  const duration = room.raceType === "economy" ? requestedDuration : RACE_LENGTH_MS;
  broadcast(room, { type: "race-started", duration, raceType: room.raceType });
  if (room.raceType === "economy") {
    sendEconomyState(room);
    room.economyTimer = setInterval(() => {
      if (!room.racing) return;
      room.economy.forEach((player, index) => {
        player.aura = Math.min(Number.MAX_SAFE_INTEGER, player.aura + player.autoPower);
        player.earned = Math.min(Number.MAX_SAFE_INTEGER, player.earned + player.autoPower);
        room.scores[index] = player.earned;
      });
      sendEconomyState(room);
      broadcast(room, { type: "score", scores: room.scores });
    }, 1000);
  }
  startPracticeBot(room);
  room.raceTimer = setTimeout(() => finishRace(room), duration);
}

function sendInventoryState(room) {
  if (room.players.some((player, index) => !room.inventoryReady[index])) return;
  room.players.forEach((player, index) => {
    send(player, {
      type: "inventory-state",
      yourItems: room.inventories[index],
      opponentItems: room.inventories[1 - index]
    });
  });
}

function finishRace(room) {
  if (!room.racing) return;
  clearTimeout(room.raceTimer);
  room.raceTimer = null;
  if (room.economyTimer) clearInterval(room.economyTimer);
  room.economyTimer = null;
  if (room.botTimer) clearInterval(room.botTimer);
  room.botTimer = null;
  room.racing = false;
  const winner = room.scores[0] === room.scores[1]
    ? 0
    : room.scores[0] > room.scores[1] ? 1 : 2;
  broadcast(room, { type: "race-ended", scores: room.scores, winner });
}

function removeFromRoom(client) {
  removeFromMatchmaking(client);
  const code = client.roomCode;
  const room = roomFor(client);
  if (!room) return;
  if (room.raceTimer) clearTimeout(room.raceTimer);
  if (room.economyTimer) clearInterval(room.economyTimer);
  if (room.botTimer) clearInterval(room.botTimer);
  if (room.inviteTimer) clearTimeout(room.inviteTimer);
  if (room.inviteeId && room.players.length === 1) {
    const invitee = onlineClients.get(room.inviteeId);
    if (invitee) send(invitee, { type: "invite-expired", roomCode: code });
  }
  room.players.forEach(player => {
    if (player !== client) {
      player.roomCode = null;
      player.playerNumber = 0;
      send(player, { type: "opponent-left" });
    }
  });
  rooms.delete(code);
  client.roomCode = null;
  client.playerNumber = 0;
}

function leaveWithError(client, message) {
  send(client, { type: "error", message });
}

webSockets.on("connection", (client, request) => {
  client._socket.setNoDelay(true);
  client.isAlive = true;
  client.on("pong", () => {
    client.isAlive = true;
  });
  const session = sessionFor(request);
  const username = session ? session.username : null;
  const account = username ? accountStore.accounts[username.toLowerCase()] : null;
  if (!account) {
    client.close(1008, "Sign in to play online.");
    return;
  }
  client.username = account.username;
  client.accountId = account.id;
  client.roomCode = null;
  client.playerNumber = 0;
  client.matchmaking = false;
  client.tapWindowStart = Date.now();
  client.tapCount = 0;
  client.inventoryWindowStart = Date.now();
  client.inventoryMessageCount = 0;
  const previousClient = onlineClients.get(client.accountId);
  if (previousClient && previousClient !== client && previousClient.readyState === WebSocket.OPEN) {
    send(previousClient, { type: "error", message: "This account connected from another game window." });
    previousClient.close(4001, "Account connected elsewhere.");
  }
  onlineClients.set(client.accountId, client);
  send(client, { type: "connected", playerId: client.accountId });

  client.on("message", rawMessage => {
    let message;
    try {
      message = JSON.parse(rawMessage.toString());
    } catch {
      leaveWithError(client, "That message was not valid. Please reconnect.");
      client.close();
      return;
    }
    if (!message || typeof message.type !== "string") {
      leaveWithError(client, "That action was not recognized.");
      return;
    }

    if (message.type === "connect") {
      send(client, { type: "connected", playerId: client.accountId });
      return;
    }

    if (message.type === "find-random") {
      if (client.roomCode || client.matchmaking) {
        leaveWithError(client, "Leave your current room or queue before finding a random player.");
        return;
      }
      if (message.raceType !== undefined && !RACE_TYPES.has(message.raceType)) {
        leaveWithError(client, "Choose a valid online race mode.");
        return;
      }
      const raceType = RACE_TYPES.has(message.raceType) ? message.raceType : "click";
      let opponentIndex = -1;
      for (let index = 0; index < matchmakingQueue.length;) {
        const opponent = matchmakingQueue[index];
        if (opponent.readyState !== WebSocket.OPEN || onlineClients.get(opponent.accountId) !== opponent ||
            opponent.roomCode || opponent.accountId === client.accountId) {
          matchmakingQueue.splice(index, 1);
          continue;
        }
        if (opponent.matchmakingMode === raceType) {
          opponentIndex = index;
          break;
        }
        index += 1;
      }
      if (opponentIndex === -1) {
        if (rooms.size >= ROOM_LIMIT || matchmakingQueue.length >= ROOM_LIMIT) {
          leaveWithError(client, "The game is busy. Try again in a little while.");
          return;
        }
        client.matchmaking = true;
        client.matchmakingMode = raceType;
        matchmakingQueue.push(client);
        send(client, { type: "queue-joined", raceType });
        client.matchmakingTimer = setTimeout(() => {
          if (!client.matchmaking || !matchmakingQueue.includes(client)) return;
          removeFromMatchmaking(client);
          createPracticeRoom(client, raceType);
        }, MATCHMAKING_TIMEOUT_MS);
        return;
      }
      const opponent = matchmakingQueue.splice(opponentIndex, 1)[0];
      removeFromMatchmaking(opponent);
      const room = createRoom([opponent, client], "random", null, raceType);
      if (!room) {
        leaveWithError(client, "The game is busy. Try again in a little while.");
        return;
      }
      sendRoomReady(room);
      sendOpponentInfo(room);
      sendInventoryState(room);
      return;
    }

    if (message.type === "cancel-matchmaking") {
      removeFromMatchmaking(client);
      send(client, { type: "matchmaking-cancelled" });
      return;
    }

    if (message.type === "challenge-friend") {
      const friendId = typeof message.friendId === "string" ? message.friendId.trim().toUpperCase() : "";
      if (message.raceType !== undefined && !RACE_TYPES.has(message.raceType)) {
        leaveWithError(client, "Choose a valid online race mode.");
        return;
      }
      const raceType = RACE_TYPES.has(message.raceType) ? message.raceType : "click";
      if (!/^EC-[A-HJ-NP-Z2-9]{8}$/.test(friendId) || !areFriends(client.accountId, friendId)) {
        leaveWithError(client, "You can only invite someone on your friends list.");
        return;
      }
      if (client.roomCode || client.matchmaking) {
        leaveWithError(client, "Leave your current room or queue before sending an invite.");
        return;
      }
      const friend = onlineClients.get(friendId);
      if (!friend || friend.readyState !== WebSocket.OPEN) {
        leaveWithError(client, "That friend is offline. Ask them to connect to the game first.");
        return;
      }
      if (friend.roomCode || friend.matchmaking) {
        leaveWithError(client, "That friend is already in a room or looking for a player.");
        return;
      }
      const room = createRoom([client], "friend", friendId, raceType);
      if (!room) {
        leaveWithError(client, "The game is busy. Try again in a little while.");
        return;
      }
      sendRoomReady(room);
      room.inviteTimer = setTimeout(() => {
        if (rooms.get(client.roomCode) !== room || room.inviteeId !== friendId) return;
        const inviteRoomCode = client.roomCode;
        rooms.delete(inviteRoomCode);
        client.roomCode = null;
        client.playerNumber = 0;
        room.inviteeId = null;
        send(client, { type: "invite-expired" });
        send(friend, { type: "invite-expired", roomCode: inviteRoomCode });
      }, INVITE_TTL_MS);
      send(friend, {
        type: "race-invite",
        fromId: client.accountId,
        fromUsername: client.username,
        roomCode: client.roomCode,
        raceType
      });
      return;
    }

    if (message.type === "accept-race-invite") {
      const code = typeof message.roomCode === "string" ? message.roomCode.trim().toUpperCase() : "";
      const room = rooms.get(code);
      if (client.roomCode || client.matchmaking || !room || room.mode !== "friend" ||
          room.players.length !== 1 || room.inviteeId !== client.accountId ||
          !areFriends(client.accountId, room.players[0].accountId)) {
        leaveWithError(client, "That friend invite is no longer available.");
        return;
      }
      room.players.push(client);
      clearTimeout(room.inviteTimer);
      room.inviteTimer = null;
      room.inviteeId = null;
      client.roomCode = code;
      client.playerNumber = 2;
      send(client, {
        type: "room-ready",
        code,
        player: 2,
        players: 2,
        mode: room.mode,
        raceType: room.raceType,
        opponentId: room.players[0].accountId,
        opponentUsername: room.players[0].username
      });
      broadcast(room, { type: "players", count: 2 });
      sendOpponentInfo(room);
      sendInventoryState(room);
      return;
    }

    if (message.type === "decline-race-invite") {
      const code = typeof message.roomCode === "string" ? message.roomCode.trim().toUpperCase() : "";
      const room = rooms.get(code);
      if (room && room.mode === "friend" && room.inviteeId === client.accountId) {
        if (room.inviteTimer) clearTimeout(room.inviteTimer);
        send(room.players[0], { type: "invite-declined" });
        rooms.delete(code);
        room.players[0].roomCode = null;
        room.players[0].playerNumber = 0;
      }
      return;
    }

    if (message.type === "leave-room") {
      if (!client.roomCode) {
        send(client, { type: "left-room" });
        return;
      }
      removeFromRoom(client);
      send(client, { type: "left-room" });
      return;
    }

    if (message.type === "create-room") {
      if (client.roomCode || client.matchmaking) {
        leaveWithError(client, "You are already in a room.");
        return;
      }
      if (message.raceType !== undefined && !RACE_TYPES.has(message.raceType)) {
        leaveWithError(client, "Choose a valid online race mode.");
        return;
      }
      if (rooms.size >= ROOM_LIMIT) {
        leaveWithError(client, "The game is busy. Try again in a little while.");
        return;
      }
      const raceType = RACE_TYPES.has(message.raceType) ? message.raceType : "click";
      const room = createRoom([client], "open", null, raceType);
      if (!room) {
        leaveWithError(client, "The game is busy. Try again in a little while.");
        return;
      }
      sendRoomReady(room);
      return;
    }

    if (message.type === "join-room") {
      if (client.roomCode || client.matchmaking) {
        leaveWithError(client, "You are already in a room.");
        return;
      }
      const code = typeof message.code === "string" ? message.code.trim().toUpperCase() : "";
      const room = rooms.get(code);
      if (!room || room.players.length !== 1) {
        leaveWithError(client, "Room not found or already full. Check the code with your friend.");
        return;
      }
      room.players.push(client);
      client.roomCode = code;
      client.playerNumber = 2;
      sendRoomReady(room);
      broadcast(room, { type: "players", count: 2 });
      sendOpponentInfo(room);
      sendInventoryState(room);
      return;
    }

    const room = roomFor(client);
    if (!room) {
      leaveWithError(client, "Create or join a room first.");
      return;
    }
    if (message.type === "peer-signal") {
      if (room.players.length !== 2 || !message.signal || typeof message.signal !== "object" ||
          !["offer", "answer", "candidate"].includes(message.signal.type) ||
          JSON.stringify(message.signal).length > 12 * 1024) {
        leaveWithError(client, "Could not relay the peer connection setup.");
        return;
      }
      const opponent = room.players[1 - (client.playerNumber - 1)];
      send(opponent, { type: "peer-signal", signal: message.signal });
      return;
    }
    if (room.mode !== "practice" &&
        ["start-race", "tap", "economy-tap", "economy-upgrade"].includes(message.type)) {
      leaveWithError(client, "The room creator's device hosts this race.");
      return;
    }
    const playerIndex = client.playerNumber - 1;

    if (message.type === "inventory-sync") {
      const now = Date.now();
      if (now - client.inventoryWindowStart >= 1000) {
        client.inventoryWindowStart = now;
        client.inventoryMessageCount = 0;
      }
      if (client.inventoryMessageCount >= 5) return;
      client.inventoryMessageCount += 1;
      if (!Array.isArray(message.items) || message.items.length > COSMETIC_IDS.size) {
        leaveWithError(client, "That inventory could not be shared.");
        return;
      }
      room.inventories[playerIndex] = [...new Set(message.items.filter(id => typeof id === "string" && COSMETIC_IDS.has(id)))];
      room.inventoryReady[playerIndex] = true;
      sendInventoryState(room);
      return;
    }

    if (message.type === "start-race") {
      if (client.playerNumber !== 1 || room.players.length !== 2 || room.racing) {
        leaveWithError(client, "Wait for both players. Player 1 starts the race.");
        return;
      }
      const duration = Number(message.duration);
      if (room.raceType === "economy" && !ECONOMY_RACE_DURATIONS.has(duration)) {
        leaveWithError(client, "Choose a race time from 1, 3, 5, or 10 minutes.");
        return;
      }
      startRace(room, duration);
      return;
    }

    if (message.type === "economy-tap") {
      if (!room.racing || room.raceType !== "economy") return;
      const now = Date.now();
      if (now - client.tapWindowStart >= 1000) {
        client.tapWindowStart = now;
        client.tapCount = 0;
      }
      if (client.tapCount >= MAX_TAPS_PER_SECOND) return;
      client.tapCount += 1;
      const player = room.economy[playerIndex];
      player.aura = Math.min(Number.MAX_SAFE_INTEGER, player.aura + player.tapPower);
      player.earned = Math.min(Number.MAX_SAFE_INTEGER, player.earned + player.tapPower);
      room.scores[playerIndex] = player.earned;
      sendEconomyState(room);
      broadcast(room, { type: "score", scores: room.scores });
      return;
    }

    if (message.type === "economy-upgrade") {
      if (!room.racing || room.raceType !== "economy") return;
      const upgradeId = message.upgrade;
      const upgrade = Object.hasOwn(ECONOMY_UPGRADES, upgradeId) ? ECONOMY_UPGRADES[upgradeId] : null;
      const player = room.economy[playerIndex];
      const levelKey = upgradeId === "tap" ? "tapLevel" : "autoLevel";
      const powerKey = upgradeId === "tap" ? "tapPower" : "autoPower";
      if (!upgrade || player[levelKey] >= upgrade.maxLevel) {
        leaveWithError(client, "That match upgrade is not available.");
        return;
      }
      const cost = upgrade.baseCost * (2 ** player[levelKey]);
      if (!Number.isSafeInteger(cost) || player.aura < cost) {
        send(client, { type: "economy-error", message: "Earn more match Aura before buying that upgrade." });
        return;
      }
      player.aura -= cost;
      player[levelKey] += 1;
      player[powerKey] += 1;
      sendEconomyState(room);
      broadcast(room, { type: "score", scores: room.scores });
      return;
    }

    if (message.type === "tap") {
      if (!room.racing || room.raceType !== "click") return;
      const now = Date.now();
      if (now - client.tapWindowStart >= 1000) {
        client.tapWindowStart = now;
        client.tapCount = 0;
      }
      if (client.tapCount >= MAX_TAPS_PER_SECOND) return;
      client.tapCount += 1;
      room.scores[playerIndex] += 1;
      broadcast(room, { type: "score", scores: room.scores });
      return;
    }

    if (message.type === "trade-offer") {
      const give = message.give;
      const want = message.want;
      const otherIndex = 1 - playerIndex;
      if (room.players.length !== 2 || room.pendingTrade) {
        leaveWithError(client, "Wait until both players are here and no other trade is pending.");
        return;
      }
      if (!areFriends(client.accountId, room.players[otherIndex].accountId)) {
        leaveWithError(client, "Cosmetic trades are only available with friends.");
        return;
      }
      if (typeof give !== "string" || typeof want !== "string" ||
          !COSMETIC_IDS.has(give) || !COSMETIC_IDS.has(want) ||
          ["mint", "bolt", "smile"].includes(give) || ["mint", "bolt", "smile"].includes(want) ||
          give === want || !room.inventories[playerIndex].includes(give) ||
          !room.inventories[otherIndex].includes(want) ||
          room.inventories[playerIndex].includes(want) ||
          room.inventories[otherIndex].includes(give)) {
        leaveWithError(client, "Those cosmetics are not available for this trade.");
        return;
      }
      room.pendingTrade = { sender: playerIndex, give, want };
      send(client, { type: "trade-sent", give, want });
      send(room.players[otherIndex], { type: "trade-offer", give, want });
      return;
    }

    if (message.type === "trade-decline") {
      if (!room.pendingTrade || room.pendingTrade.sender === playerIndex) return;
      const sender = room.players[room.pendingTrade.sender];
      room.pendingTrade = null;
      send(sender, { type: "trade-declined" });
      send(client, { type: "trade-declined" });
      return;
    }

    if (message.type === "trade-accept") {
      const trade = room.pendingTrade;
      if (!trade || trade.sender === playerIndex || room.players.length !== 2) return;
      if (!areFriends(client.accountId, room.players[trade.sender].accountId)) {
        room.pendingTrade = null;
        broadcast(room, { type: "trade-declined" });
        return;
      }
      const senderItems = room.inventories[trade.sender];
      const receiverItems = room.inventories[playerIndex];
      if (!senderItems.includes(trade.give) || !receiverItems.includes(trade.want) ||
          senderItems.includes(trade.want) || receiverItems.includes(trade.give)) {
        room.pendingTrade = null;
        broadcast(room, { type: "trade-declined" });
        return;
      }
      room.inventories[trade.sender] = senderItems.filter(id => id !== trade.give).concat(trade.want);
      room.inventories[playerIndex] = receiverItems.filter(id => id !== trade.want).concat(trade.give);
      room.pendingTrade = null;
      room.players.forEach((player, index) => {
        send(player, { type: "trade-complete", yourItems: room.inventories[index] });
      });
      sendInventoryState(room);
      return;
    }

    leaveWithError(client, "That action was not recognized.");
  });

  client.on("close", () => {
    if (onlineClients.get(client.accountId) === client) onlineClients.delete(client.accountId);
    removeFromRoom(client);
  });
  client.on("error", error => console.error("WebSocket client error:", error.message));
});

server.on("error", error => {
  console.error("Could not start the game server:", error.message);
  process.exitCode = 1;
});

loadAccountStore().then(store => {
  accountStore = store;
  return migrateAccountStore();
}).then(() => {
  server.listen(PORT, "0.0.0.0", () => {
    console.log("Emoji Clicker is ready at http://localhost:" + PORT + " (accounts and saves stored in Firebase).");
  });
}).catch(error => {
  console.error("Could not load the Firebase account store:", error.message);
  process.exitCode = 1;
});
