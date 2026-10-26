import WebSocket from "ws";
import Davey from "@snazzah/davey";
import { Streamer } from "@dank074/discord-video-stream";

const VoiceOpCodes = {
  IDENTIFY: 0,
  SELECT_PROTOCOL: 1,
  READY: 2,
  HEARTBEAT: 3,
  SESSION_DESCRIPTION: 4,
  SPEAKING: 5,
  HEARTBEAT_ACK: 6,
  RESUME: 7,
  HELLO: 8,
  RESUMED: 9,
  CLIENTS_CONNECT: 11,
  CLIENT_DISCONNECT: 13,
  DAVE_PREPARE_TRANSITION: 21,
  DAVE_EXECUTE_TRANSITION: 22,
  DAVE_TRANSITION_READY: 23,
  DAVE_PREPARE_EPOCH: 24,
  MLS_INVALID_COMMIT_WELCOME: 31,
};

const VoiceOpCodesBinary = {
  MLS_EXTERNAL_SENDER: 25,
  MLS_KEY_PACKAGE: 26,
  MLS_PROPOSALS: 27,
  MLS_COMMIT_WELCOME: 28,
  MLS_ANNOUNCE_COMMIT_TRANSITION: 29,
  MLS_WELCOME: 30,
};

export class StreamWatchSession {
  constructor({ streamKey, endpoint, token, rtcServerId, rtcChannelId, userId, sessionId, onReady, onClose }) {
    this.streamKey = streamKey;
    this.endpoint = endpoint.replace(":443", "");
    this.token = token;
    this.rtcServerId = rtcServerId;
    this.rtcChannelId = rtcChannelId;
    this._daveSession = null;
    this._daveProtocolVersion = 0;
    this._davePendingTransitions = new Map();
    this._daveDowngraded = false;
    this._connectedUsers = new Set();
    this._sequenceNumber = 0;
    this.userId = userId;
    this.sessionId = sessionId;
    this.onReady = onReady;
    this.onClose = onClose;

    this.ws = null;
    this.heartbeatTimer = null;
    this.isReady = false;
    this.isClosed = false;
  }

  start() {
    if (this.isClosed) return;
    try {
      this.ws = new WebSocket(`wss://${this.endpoint}/?v=8`);
      this.ws.binaryType = "arraybuffer";

      this.ws.on("open", () => {
        // Awaiting HELLO (op 8)
      });

      this.ws.on("message", (raw) => {
        try {
          const buf = Buffer.isBuffer(raw) ? raw : Buffer.from(new Uint8Array(raw));
          if (buf.length > 0 && buf[0] !== 0x7b) {
            this.handleBinaryMessages(buf);
          } else {
            const packet = JSON.parse(buf.toString("utf-8"));
            this.handlePacket(packet);
          }
        } catch {}
      });

      this.ws.on("error", () => {
        // Handled via close
      });

      this.ws.on("close", (code,reason) => {
        console.log("[WS-CLOSE]", code, reason&&reason.toString());
        this.cleanup();
        if (this.onClose) this.onClose(this.streamKey);
      });
    } catch {
      this.cleanup();
      if (this.onClose) this.onClose(this.streamKey);
    }
  }

  handlePacket(packet) {
    const { op, d } = packet;

    if (op === VoiceOpCodes.HELLO) {
      const interval = d.heartbeat_interval || 30000;
      this.startHeartbeat(interval);

      this.send({
        op: VoiceOpCodes.IDENTIFY,
        d: {
          server_id: this.rtcServerId,
          user_id: this.userId,
          session_id: this.sessionId,
          token: this.token,
          video: true,
          streams: [
            {
              type: "video",
              rid: "100",
              quality: 100,
            },
          ],
          max_dave_protocol_version: Davey.DAVE_PROTOCOL_VERSION ?? 0,
        },
      });
    } else if (op === VoiceOpCodes.READY) {
      this._daveProtocolVersion = d.dave_protocol_version || 0;
      this.initDave();
      const markReady = () => {
        if (this._daveProtocolVersion && !this.daveReady) {
          return setTimeout(markReady, 100);
        }
        if (!this.isReady) {
          this.isReady = true;
          if (this.onReady) this.onReady(this.streamKey);
        }
      };
      setTimeout(markReady, 0);
    } else if (op === VoiceOpCodes.CLIENTS_CONNECT) {
      (d.user_ids || []).forEach((id) => this._connectedUsers.add(id));
    } else if (op === VoiceOpCodes.CLIENT_DISCONNECT) {
      this._connectedUsers.delete(d.user_id);
    } else if (op === VoiceOpCodes.DAVE_PREPARE_TRANSITION) {
      this._davePendingTransitions.set(d.transition_id, d.protocol_version);
      if (d.transition_id === 0) {
        this.executePendingTransition(d.transition_id);
      } else {
        if (d.protocol_version === 0) this._daveSession?.setPassthroughMode(true, 120);
        this.sendOpcode(VoiceOpCodes.DAVE_TRANSITION_READY, { transition_id: d.transition_id });
      }
    } else if (op === VoiceOpCodes.DAVE_EXECUTE_TRANSITION) {
      this.executePendingTransition(d.transition_id);
    } else if (op === VoiceOpCodes.DAVE_PREPARE_EPOCH) {
      if (d.epoch === 1) {
        this._daveProtocolVersion = d.protocol_version;
        this.initDave();
      }
    }
  }

  get daveReady() {
    return this._daveProtocolVersion && this._daveSession?.ready;
  }

  initDave() {
    if (this._daveProtocolVersion) {
      if (this._daveSession) {
        this._daveSession.reinit(this._daveProtocolVersion, this.userId, this.rtcChannelId);
      } else {
        this._daveSession = new Davey.DAVESession(this._daveProtocolVersion, this.userId, this.rtcChannelId);
      }
      this.sendOpcodeBinary(VoiceOpCodesBinary.MLS_KEY_PACKAGE, this._daveSession.getSerializedKeyPackage());
    } else if (this._daveSession) {
      this._daveSession.reset();
      this._daveSession.setPassthroughMode(true, 10);
    }
  }

  executePendingTransition(transitionId) {
    const newVersion = this._davePendingTransitions.get(transitionId);
    if (newVersion === undefined) return;
    this._daveProtocolVersion = newVersion;
    if (newVersion === 0) {
      this._daveDowngraded = true;
    } else if (transitionId > 0 && this._daveDowngraded) {
      this._daveDowngraded = false;
      this._daveSession?.setPassthroughMode(true, 10);
    }
    this._davePendingTransitions.delete(transitionId);
  }

  processInvalidCommit(transitionId) {
    this.sendOpcode(VoiceOpCodes.MLS_INVALID_COMMIT_WELCOME, { transition_id: transitionId });
    this.initDave();
  }

  sendOpcode(code, data) {
    if (this.ws?.readyState !== WebSocket.OPEN) return;
    this.ws.send(JSON.stringify({ op: code, d: data }));
  }

  sendOpcodeBinary(code, data) {
    if (this.ws?.readyState !== WebSocket.OPEN) return;
    const payload = Buffer.isBuffer(data) ? data : Buffer.from(data);
    const buf = Buffer.allocUnsafe(payload.length + 1);
    buf.writeUInt8(code, 0);
    payload.copy(buf, 1);
    this.ws.send(buf);
  }

  handleBinaryMessages(msg) {
    this._sequenceNumber = msg.readUInt16BE(0);
    const op = msg.readUInt8(2);
    switch (op) {
      case VoiceOpCodesBinary.MLS_EXTERNAL_SENDER:
        this._daveSession?.setExternalSender(msg.subarray(3));
        break;
      case VoiceOpCodesBinary.MLS_PROPOSALS: {
        const optype = msg.readUInt8(3);
        const { commit, welcome } = this._daveSession.processProposals(optype, msg.subarray(4), [...this._connectedUsers]);
        if (commit) {
          this.sendOpcodeBinary(VoiceOpCodesBinary.MLS_COMMIT_WELCOME, welcome ? Buffer.concat([commit, welcome]) : commit);
        }
        break;
      }
      case VoiceOpCodesBinary.MLS_ANNOUNCE_COMMIT_TRANSITION: {
        const transitionId = msg.readUInt16BE(3);
        try {
          this._daveSession?.processCommit(msg.subarray(5));
          if (transitionId) {
            this._davePendingTransitions.set(transitionId, this._daveProtocolVersion);
            this.sendOpcode(VoiceOpCodes.DAVE_TRANSITION_READY, { transition_id: transitionId });
          }
        } catch (e) {
          this.processInvalidCommit(transitionId);
        }
        break;
      }
      case VoiceOpCodesBinary.MLS_WELCOME: {
        const transitionId = msg.readUInt16BE(3);
        try {
          this._daveSession?.processWelcome(msg.subarray(5));
          if (transitionId) {
            this._davePendingTransitions.set(transitionId, this._daveProtocolVersion);
            this.sendOpcode(VoiceOpCodes.DAVE_TRANSITION_READY, { transition_id: transitionId });
          }
        } catch (e) {
          this.processInvalidCommit(transitionId);
        }
        break;
      }
    }
  }

  send(data) {
    if (this.ws && this.ws.readyState === WebSocket.OPEN) {
      this.ws.send(JSON.stringify(data));
    }
  }

  startHeartbeat(interval) {
    if (this.heartbeatTimer) clearInterval(this.heartbeatTimer);
    this.heartbeatTimer = setInterval(() => {
      this.send({
        op: VoiceOpCodes.HEARTBEAT,
        d: { t: Date.now(), seq_ack: this._sequenceNumber },
      });
    }, interval * 0.9);
  }

  cleanup() {
    this.isReady = false;
    if (this.heartbeatTimer) {
      clearInterval(this.heartbeatTimer);
      this.heartbeatTimer = null;
    }
  }

  stop() {
    this.isClosed = true;
    this.cleanup();
    if (this.ws) {
      try {
        this.ws.close();
      } catch {}
      this.ws = null;
    }
  }
}

export class WatcherManager {
  constructor(client, guildId, channelId, dashboard) {
    this.client = client;
    this.guildId = guildId;
    this.channelId = channelId;
    this.dashboard = dashboard;

    this.streamer = new Streamer(client);
    this.activeSessions = new Map(); // streamKey -> StreamWatchSession
    this.pendingData = new Map(); // streamKey -> { rtcServerId, endpoint, token }
    this.sessionId = null;
    this.isRunning = false;

    this.setupListeners();
  }

  setupListeners() {
    this.client.on("raw", (packet) => {
      if (!this.isRunning) return;

      if ((packet.t||"").includes("STREAM")) console.log("[RAW-DBG]", packet.t, JSON.stringify(packet.d).slice(0,200));
      if (packet.t === "VOICE_STATE_UPDATE") {
        if (packet.d?.user_id === this.client.user?.id) {
          if (packet.d.session_id) {
            this.sessionId = packet.d.session_id;
          }
        }
      } else if (packet.t === "STREAM_CREATE") {
        const { stream_key, rtc_server_id, rtc_channel_id } = packet.d || {};
        if (!stream_key) return;
        const entry = this.pendingData.get(stream_key) || {};
        entry.rtcServerId = rtc_server_id;
        entry.rtcChannelId = rtc_channel_id;
        this.pendingData.set(stream_key, entry);
        this.checkAndConnectSession(stream_key);
      } else if (packet.t === "STREAM_SERVER_UPDATE") {
        const { stream_key, endpoint, token, rtc_server_id } = packet.d || {};
        if (!stream_key) return;
        const entry = this.pendingData.get(stream_key) || {};
        entry.endpoint = endpoint;
        entry.token = token;
        if (rtc_server_id) {
          entry.rtcServerId = rtc_server_id;
        }
        this.pendingData.set(stream_key, entry);
        this.checkAndConnectSession(stream_key);
      } else if (packet.t === "STREAM_DELETE") {
        const { stream_key } = packet.d || {};
        const { reason } = packet.d || {};
        // ponytail: stale delete (stream_not_found) arrives around STREAM_CREATE; ignore it
        if (stream_key && reason !== "stream_not_found") {
          this.stopSession(stream_key);
        }
      }
    });
  }

  async start() {
    this.isRunning = true;
    try {
      this.dashboard?.setViewerStatus("Joining channel (WebRTC)...");
      try {
        this.streamer.leaveVoice();
      } catch {}
      await this.streamer.joinVoice(this.guildId, this.channelId);
      this.sessionId = this.streamer.voiceConnection?.session_id || this.sessionId;
      this.dashboard?.setViewerStatus("In channel");
      this.dashboard?.addLog("Viewer joined voice channel (WebRTC active)", "success");
    } catch (err) {
      this.dashboard?.setViewerStatus("Voice error");
      this.dashboard?.addLog(`Viewer voice connection failed: ${err.message}`, "error");
      throw err;
    }
  }

  checkAndConnectSession(streamKey) {
    if (!this.isRunning) return;
    const data = this.pendingData.get(streamKey);
    if (!data || !data.endpoint || !data.token) return;
    if (this.activeSessions.has(streamKey)) return;

    const rtcServerId = data.rtcServerId
      || this.streamer.voiceConnection?.streamConnection?.serverId
      || this.guildId;
    if (!rtcServerId) return;

    const sessionId = this.sessionId || this.streamer.voiceConnection?.session_id;
    if (!sessionId || !this.client.user) return;

    const session = new StreamWatchSession({
      streamKey,
      endpoint: data.endpoint,
      token: data.token,
      rtcServerId,
      rtcChannelId: data.rtcChannelId || this.channelId,
      userId: this.client.user.id,
      sessionId,
      onReady: (key) => {
        const username = key.split(":").pop();
        this.dashboard?.addLog(`Viewer watching stream: ${username} (RTC connected)`, "success");
        this.updateDashboardStatus();
      },
      onClose: (key) => {
        this.activeSessions.delete(key);
        this.updateDashboardStatus();

        if (this.isRunning && this.pendingData.has(key)) {
          setTimeout(() => {
            if (this.isRunning && this.pendingData.has(key) && !this.activeSessions.has(key)) {
              this.watchStream(key);
            }
          }, 5000);
        }
      },
    });

    this.activeSessions.set(streamKey, session);
    session.start();
    this.updateDashboardStatus();
  }

  watchStream(streamKey) {
    if (!this.isRunning) return;

    if (!this.streamer.voiceConnection || !this.streamer.voiceConnection.status?.started) {
      this.dashboard?.addLog("Viewer not in voice yet, deferring stream watch...", "warn");
      return;
    }

    if (this.activeSessions.has(streamKey) && this.activeSessions.get(streamKey).isReady) {
      return;
    }
    this.client.ws.broadcast({
      op: 20,
      d: {
        stream_key: streamKey,
      },
    });
  }

  stopSession(streamKey) {
    const session = this.activeSessions.get(streamKey);
    if (session) {
      session.stop();
      this.activeSessions.delete(streamKey);
    }
    this.pendingData.delete(streamKey);
    this.updateDashboardStatus();
  }

  updateDashboardStatus() {
    const count = Array.from(this.activeSessions.values()).filter((s) => s.isReady).length;
    if (count > 0) {
      this.dashboard?.setViewerStatus(`Watching (${count} stream${count > 1 ? "s" : ""})`);
    } else if (this.activeSessions.size > 0) {
      this.dashboard?.setViewerStatus(`Connecting to stream (${this.activeSessions.size})...`);
    } else {
      this.dashboard?.setViewerStatus("In channel (Waiting for streams)");
    }
  }

  getWatchingCount() {
    return Array.from(this.activeSessions.values()).filter((s) => s.isReady).length;
  }

  stop() {
    this.isRunning = false;
    for (const [, session] of this.activeSessions.entries()) {
      session.stop();
    }
    this.activeSessions.clear();
    this.pendingData.clear();
    try {
      this.streamer?.leaveVoice();
    } catch {}
  }
}
