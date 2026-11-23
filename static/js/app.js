"use strict";

const currentUserEl = document.getElementById("current-user");
const messageEl = document.getElementById("app-message");
const peerListEl = document.getElementById("peer-list");

/**
 * Clear session and redirect to login.
 */
function redirectToLogin() {
  sessionStorage.removeItem("username");
  window.location.replace("/login.html");
}

/**
 * Render active peers into the list.
 * @param {object|null|undefined} peersMap
 */
function renderPeers(peersMap) {
  peerListEl.textContent = "";
  const peers = Object.values(peersMap || {});
  if (peers.length === 0) {
    const item = document.createElement("li");
    item.textContent = "No active peers online";
    peerListEl.appendChild(item);
    return;
  }

  for (const peer of peers) {
    const item = document.createElement("li");
    item.textContent = `${peer.username} (${peer.ip}:${peer.port})`;
    peerListEl.appendChild(item);
  }
}

/**
 * Verify session and load dashboard.
 * @returns {Promise<void>}
 */
async function bootApp() {
  messageEl.textContent = "";
  messageEl.className = "";

  const res = await apiGetPeerList();
  if (res.status === 401) {
    redirectToLogin();
    return;
  }
  if (!res.ok) {
    messageEl.className = "error";
    messageEl.textContent = res.message;
    return;
  }

  currentUserEl.textContent =
    sessionStorage.getItem("username") || "Authenticated user";
  renderPeers(res.data && res.data.peers);
}

bootApp();
