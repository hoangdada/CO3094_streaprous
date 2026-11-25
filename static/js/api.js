"use strict";

/**
 * @typedef {object} ApiResult
 * @property {boolean} ok
 * @property {number} status
 * @property {object|null} data
 * @property {string} message
 */

/**
 * Send a JSON HTTP request and return a normalized result object.
 * @param {string} path
 * @param {{ method?: string, body?: unknown }} [options]
 * @returns {Promise<ApiResult>}
 */
async function requestJson(path, options = {}) {
  const requestOptions = options || {};
  const fetchOptions = {
    method: requestOptions.method || "GET",
    credentials: "same-origin",
  };

  if (requestOptions.body !== undefined) {
    fetchOptions.headers = { "Content-Type": "application/json" };
    fetchOptions.body = JSON.stringify(requestOptions.body);
  }

  let response;
  let rawText;
  try {
    response = await fetch(path, fetchOptions);
    rawText = await response.text();
  } catch {
    return {
      ok: false,
      status: 0,
      data: null,
      message: "Cannot connect to server",
    };
  }

  const trimmedText = rawText.trim();
  let data = null;
  if (trimmedText) {
    try {
      const parsed = JSON.parse(trimmedText);
      if (
        parsed !== null &&
        typeof parsed === "object" &&
        !Array.isArray(parsed)
      ) {
        data = parsed;
      }
    } catch {
      data = null;
    }
  }

  const message =
    (data && typeof data.message === "string" && data.message.trim()) ||
    (!data && trimmedText) ||
    (!response.ok ? `HTTP ${response.status}` : "");

  return {
    ok: Boolean(response.ok),
    status: response.status,
    data: data,
    message: message,
  };
}

/**
 * Authenticate a user with username and password.
 * @param {string} username
 * @param {string} password
 * @returns {Promise<ApiResult>}
 */
function apiLogin(username, password) {
  return requestJson("/login", {
    method: "POST",
    body: { username, password },
  });
}

/**
 * Submit the client listening port to register peer info.
 * @param {string} ip
 * @param {number} port
 * @returns {Promise<ApiResult>}
 */
function apiSubmitInfo(ip, port) {
  return requestJson("/submit-info", { method: "POST", body: { ip, port } });
}

/**
 * Fetch the list of currently registered peers.
 * @returns {Promise<ApiResult>}
 */
function apiGetPeerList() {
  return requestJson("/peer-list");
}

/**
 * Log out the current user session.
 * @returns {Promise<ApiResult>}
 */
function apiLogout() {
  return requestJson("/logout", { method: "POST" });
}

/**
 * Fetch message history for the current peer session.
 * @returns {Promise<ApiResult>}
 */
function apiGetMessages() {
  return requestJson("/messages");
}

/**
 * Send a direct P2P message to a target peer.
 * @param {string} targetIp
 * @param {number} targetPort
 * @param {string} targetUsername
 * @param {string} message
 * @returns {Promise<ApiResult>}
 */
function apiSendPeer(targetIp, targetPort, targetUsername, message) {
  return requestJson("/send-peer", {
    method: "POST",
    body: {
      target_ip: targetIp,
      target_port: targetPort,
      target_username: targetUsername,
      message: message,
    },
  });
}

/**
 * Broadcast a message to all active peers.
 * @param {string} message
 * @returns {Promise<ApiResult>}
 */
function apiBroadcastPeer(message) {
  return requestJson("/broadcast-peer", {
    method: "POST",
    body: { message },
  });
}
