"use strict";

const currentUserEl = document.getElementById("current-user");
const myPeerInfoEl = document.getElementById("my-peer-info");
const readyBtn = document.getElementById("ready-btn");
const refreshBtn = document.getElementById("refresh-btn");
const logoutBtn = document.getElementById("logout-btn");
const messageEl = document.getElementById("app-message");
const peerListEl = document.getElementById("peer-list");

/**
 * Display a status or error message.
 * @param {string} text
 * @param {boolean} [isError=false]
 */
function setMessage(text, isError = false) {
  messageEl.className = isError ? "error" : "";
  messageEl.textContent = text;
}

/**
 * Clear session state and redirect to login.
 */
function redirectToLogin() {
  sessionStorage.removeItem("username");
  sessionStorage.removeItem("myPeer");
  window.location.replace("/login.html");
}

/**
 * Render active peers into the list.
 * @param {object|null|undefined} peersMap
 */
function renderPeers(peersMap) {
  peerListEl.textContent = "";
  const peers = Object.values(peersMap || {});
  if (typeof renderPeerOptions === "function") {
    renderPeerOptions(peers);
  }
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
 * Verify session and load dashboard state.
 * @returns {Promise<void>}
 */
async function bootApp() {
  setMessage("");

  const res = await apiGetPeerList();
  if (res.status === 401) {
    redirectToLogin();
    return;
  }
  if (!res.ok) {
    setMessage(res.message, true);
    return;
  }

  currentUserEl.textContent =
    sessionStorage.getItem("username") || "Authenticated user";
  const myPeer = sessionStorage.getItem("myPeer");
  if (myPeer) {
    myPeerInfoEl.textContent = myPeer;
  }
  renderPeers(res.data && res.data.peers);
  if (typeof loadMessages === "function") {
    await loadMessages();
  }
}

/**
 * Register the current client peer info.
 * @returns {Promise<void>}
 */
async function handleReadyClick() {
  readyBtn.disabled = true;
  setMessage("");

  const port = Number(window.location.port || 80);
  const res = await apiSubmitInfo(port);
  readyBtn.disabled = false;

  if (res.status === 401) {
    redirectToLogin();
    return;
  }
  if (!res.ok) {
    setMessage(res.message, true);
    return;
  }

  if (res.data && res.data.username) {
    currentUserEl.textContent = res.data.username;
    sessionStorage.setItem("username", res.data.username);
  }
  if (res.data) {
    const peerText = `${res.data.ip}:${res.data.port}`;
    sessionStorage.setItem("myPeer", peerText);
    myPeerInfoEl.textContent = peerText;
  }
  setMessage(res.message);
}

/**
 * Refresh the list of active peers.
 * @returns {Promise<void>}
 */
async function handleRefreshClick() {
  refreshBtn.disabled = true;
  setMessage("");

  const res = await apiGetPeerList();
  refreshBtn.disabled = false;

  if (res.status === 401) {
    redirectToLogin();
    return;
  }
  if (!res.ok) {
    setMessage(res.message, true);
    return;
  }

  renderPeers(res.data && res.data.peers);
}

/**
 * Log out the current user session.
 * @returns {Promise<void>}
 */
async function handleLogoutClick() {
  logoutBtn.disabled = true;
  setMessage("");

  const res = await apiLogout();
  if (res.ok || res.status === 401) {
    redirectToLogin();
    return;
  }

  logoutBtn.disabled = false;
  setMessage(res.message, true);
}

readyBtn.addEventListener("click", handleReadyClick);
refreshBtn.addEventListener("click", handleRefreshClick);
logoutBtn.addEventListener("click", handleLogoutClick);
bootApp();
