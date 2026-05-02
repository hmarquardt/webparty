const SERVER_MAX_PARTICIPANTS = 4;
const MAX_MESSAGE_BYTES = 64 * 1024;
const MAX_CHAT_BYTES = 2 * 1024;
const ALLOWED_ORIGINS = []; // Example: ["https://YOUR_GITHUB_USERNAME.github.io"]
const HEARTBEAT_INTERVAL_MS = 30000;
const DEAD_SOCKET_MS = 90000;

const TARGETED_TYPES = new Set(["offer", "answer", "ice-candidate"]);
const BROADCAST_TYPES = new Set(["chat", "participant-status", "heartbeat"]);
const CLIENT_TYPES = new Set(["join", ...TARGETED_TYPES, ...BROADCAST_TYPES]);

export default {
  async fetch(request, env, ctx) {
    const url = new URL(request.url);

    if (url.pathname === "/") {
      return json({
        ok: true,
        service: "Browser Party Line signaling",
        websocket: "/room/<roomId>",
        maxParticipants: SERVER_MAX_PARTICIPANTS
      });
    }

    if (url.pathname === "/health") {
      return json({ ok: true });
    }

    const match = url.pathname.match(/^\/room\/([^/]+)$/);
    if (!match) {
      return json({ ok: false, error: "not_found" }, 404);
    }

    const roomId = decodeURIComponent(match[1]);
    if (!validateRoomId(roomId)) {
      return json({ ok: false, error: "invalid_room_id" }, 400);
    }

    if (request.headers.get("Upgrade") !== "websocket") {
      return json({ ok: false, error: "websocket_required" }, 426);
    }

    const origin = request.headers.get("Origin") || "";
    if (ALLOWED_ORIGINS.length > 0 && !ALLOWED_ORIGINS.includes(origin)) {
      return json({ ok: false, error: "origin_not_allowed" }, 403);
    }

    const id = env.PARTY_LINE_ROOM.idFromName(roomId);
    const room = env.PARTY_LINE_ROOM.get(id);
    return room.fetch(request);
  }
};

export class PartyLineRoom {
  constructor(state, env) {
    this.state = state;
    this.env = env;
    this.participants = new Map();
    this.sessions = new Map();
    this.heartbeatTimer = null;
  }

  async fetch(request) {
    if (request.headers.get("Upgrade") !== "websocket") {
      return json({ ok: false, error: "websocket_required" }, 426);
    }

    const pair = new WebSocketPair();
    const [client, server] = Object.values(pair);
    this.handleSession(server);
    return new Response(null, { status: 101, webSocket: client });
  }

  handleSession(webSocket) {
    webSocket.accept();
    const sessionId = crypto.randomUUID();
    const session = {
      sessionId,
      webSocket,
      participantId: null,
      joined: false,
      lastSeen: Date.now()
    };

    this.sessions.set(sessionId, session);
    this.ensureHeartbeat();

    webSocket.addEventListener("message", (event) => {
      this.handleMessage(sessionId, event.data);
    });

    webSocket.addEventListener("close", () => {
      this.cleanupSession(sessionId);
    });

    webSocket.addEventListener("error", () => {
      this.cleanupSession(sessionId);
    });

    safeSend(webSocket, {
      type: "hello",
      serverTime: new Date().toISOString(),
      maxParticipants: SERVER_MAX_PARTICIPANTS
    });
  }

  handleMessage(sessionId, rawMessage) {
    const session = this.sessions.get(sessionId);
    if (!session) return;
    session.lastSeen = Date.now();

    if (typeof rawMessage !== "string") {
      safeSend(session.webSocket, { type: "error", error: "text_json_required" });
      return;
    }

    if (byteLength(rawMessage) > MAX_MESSAGE_BYTES) {
      safeSend(session.webSocket, { type: "error", error: "message_too_large" });
      return;
    }

    let message;
    try {
      message = JSON.parse(rawMessage);
    } catch {
      safeSend(session.webSocket, { type: "error", error: "invalid_json" });
      return;
    }

    const error = validateMessage(message);
    if (error) {
      safeSend(session.webSocket, { type: "error", error });
      return;
    }

    if (message.type === "join") {
      this.handleJoin(session, message);
      return;
    }

    if (!session.joined || !session.participantId) {
      safeSend(session.webSocket, { type: "error", error: "join_required" });
      return;
    }

    if (message.from && message.from !== session.participantId) {
      safeSend(session.webSocket, { type: "error", error: "from_mismatch" });
      return;
    }

    if (message.type === "heartbeat") {
      safeSend(session.webSocket, { type: "heartbeat", t: Date.now() });
      return;
    }

    if (message.type === "participant-status") {
      this.updateParticipantStatus(session.participantId, message.status || {});
      this.broadcastParticipantList();
      this.broadcast({
        type: "participant-status",
        from: session.participantId,
        status: this.publicParticipant(this.participants.get(session.participantId))
      }, session.participantId);
      return;
    }

    if (message.type === "chat") {
      const text = String(message.text || "").slice(0, MAX_CHAT_BYTES);
      this.broadcast({
        type: "chat",
        from: session.participantId,
        text,
        sentAt: new Date().toISOString()
      });
      return;
    }

    if (TARGETED_TYPES.has(message.type)) {
      this.sendTo(message.to, { ...message, from: session.participantId });
      return;
    }
  }

  handleJoin(session, message) {
    const participantId = sanitizeId(message.participantId || "");
    if (!participantId) {
      safeSend(session.webSocket, { type: "error", error: "invalid_participant_id" });
      return;
    }

    if (this.participants.has(participantId)) {
      safeSend(session.webSocket, { type: "error", error: "participant_id_in_use" });
      session.webSocket.close(4009, "participant id in use");
      return;
    }

    if (this.participants.size >= SERVER_MAX_PARTICIPANTS) {
      safeSend(session.webSocket, {
        type: "room-full",
        maxParticipants: SERVER_MAX_PARTICIPANTS
      });
      session.webSocket.close(4008, "room full");
      return;
    }

    const participant = {
      participantId,
      displayName: cleanDisplayName(message.displayName),
      joinedAt: new Date().toISOString(),
      websocket: session.webSocket,
      micEnabled: Boolean(message.status?.micEnabled),
      cameraEnabled: Boolean(message.status?.cameraEnabled),
      screenSharing: Boolean(message.status?.screenSharing)
    };

    session.participantId = participantId;
    session.joined = true;
    this.participants.set(participantId, participant);

    safeSend(session.webSocket, {
      type: "joined",
      participantId,
      participants: this.publicParticipants(),
      maxParticipants: SERVER_MAX_PARTICIPANTS
    });

    this.broadcast({
      type: "participant-joined",
      participant: this.publicParticipant(participant)
    }, participantId);
    this.broadcastParticipantList();
  }

  updateParticipantStatus(participantId, status) {
    const participant = this.participants.get(participantId);
    if (!participant) return;
    if (typeof status.micEnabled === "boolean") participant.micEnabled = status.micEnabled;
    if (typeof status.cameraEnabled === "boolean") participant.cameraEnabled = status.cameraEnabled;
    if (typeof status.screenSharing === "boolean") participant.screenSharing = status.screenSharing;
  }

  broadcast(message, exceptParticipantId = null) {
    const payload = JSON.stringify(message);
    for (const participant of this.participants.values()) {
      if (participant.participantId === exceptParticipantId) continue;
      try {
        participant.websocket.send(payload);
      } catch {
        this.cleanupParticipant(participant.participantId);
      }
    }
  }

  sendTo(participantId, message) {
    const participant = this.participants.get(participantId);
    if (!participant) return false;
    safeSend(participant.websocket, message);
    return true;
  }

  broadcastParticipantList() {
    this.broadcast({
      type: "participant-list",
      participants: this.publicParticipants()
    });
  }

  cleanupSession(sessionId) {
    const session = this.sessions.get(sessionId);
    if (!session) return;
    this.sessions.delete(sessionId);
    if (session.participantId) {
      this.cleanupParticipant(session.participantId);
    }
    if (this.sessions.size === 0) {
      this.stopHeartbeat();
    }
  }

  cleanupParticipant(participantId) {
    const participant = this.participants.get(participantId);
    if (!participant) return;
    this.participants.delete(participantId);
    this.broadcast({ type: "participant-left", participantId });
    this.broadcastParticipantList();
  }

  publicParticipants() {
    return [...this.participants.values()].map((participant) => this.publicParticipant(participant));
  }

  publicParticipant(participant) {
    return {
      participantId: participant.participantId,
      displayName: participant.displayName,
      joinedAt: participant.joinedAt,
      micEnabled: participant.micEnabled,
      cameraEnabled: participant.cameraEnabled,
      screenSharing: participant.screenSharing
    };
  }

  ensureHeartbeat() {
    if (this.heartbeatTimer) return;
    this.heartbeatTimer = setInterval(() => {
      const now = Date.now();
      for (const [sessionId, session] of this.sessions.entries()) {
        if (now - session.lastSeen > DEAD_SOCKET_MS) {
          try {
            session.webSocket.close(4000, "heartbeat timeout");
          } catch {
            // Ignore close failures; cleanup below owns state removal.
          }
          this.cleanupSession(sessionId);
        } else {
          safeSend(session.webSocket, { type: "heartbeat", t: now });
        }
      }
    }, HEARTBEAT_INTERVAL_MS);
  }

  stopHeartbeat() {
    if (this.heartbeatTimer) clearInterval(this.heartbeatTimer);
    this.heartbeatTimer = null;
  }
}

function json(body, status = 200) {
  return new Response(JSON.stringify(body, null, 2), {
    status,
    headers: {
      "content-type": "application/json; charset=utf-8",
      "access-control-allow-origin": "*"
    }
  });
}

function validateRoomId(roomId) {
  return /^[A-Za-z0-9_-]{3,64}$/.test(roomId);
}

function validateMessage(message) {
  if (!message || typeof message !== "object" || Array.isArray(message)) return "invalid_message";
  if (!CLIENT_TYPES.has(message.type)) return "unknown_message_type";
  if (message.type === "join") return null;
  if (TARGETED_TYPES.has(message.type)) {
    if (!sanitizeId(message.to || "")) return "invalid_target";
  }
  if (message.type === "chat" && byteLength(String(message.text || "")) > MAX_CHAT_BYTES) {
    return "chat_message_too_large";
  }
  return null;
}

function sanitizeId(value) {
  const id = String(value || "");
  return /^[A-Za-z0-9_-]{3,96}$/.test(id) ? id : "";
}

function cleanDisplayName(value) {
  const name = String(value || "Guest").trim().replace(/\s+/g, " ").slice(0, 48);
  return name || "Guest";
}

function byteLength(value) {
  return new TextEncoder().encode(value).length;
}

function safeSend(webSocket, message) {
  try {
    webSocket.send(JSON.stringify(message));
  } catch {
    // Durable Object room state is ephemeral; failed sends are cleaned up on close/error.
  }
}

// This standard WebSocket implementation is intentionally simple for v3.
// Cloudflare's WebSocket Hibernation API can reduce idle-room cost for larger
// deployments by persisting attachments and avoiding always-active objects.
