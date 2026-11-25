"use strict";

const peerSelectEl = document.getElementById("peer-select");
const chatInputEl = document.getElementById("chat-input");
const sendBtn = document.getElementById("send-btn");
const broadcastBtn = document.getElementById("broadcast-btn");
const loadMsgBtn = document.getElementById("load-msg-btn");
const messageListEl = document.getElementById("message-list");

/**
 * Render peer options into the peer selector while preserving selection.
 * @param {Array<{username: string, ip: string, port: number}>} peers
 */
function renderPeerOptions(peers) {
  const prevValue = peerSelectEl.value;
  peerSelectEl.textContent = "";

  const defaultOption = document.createElement("option");
  defaultOption.value = "";
  defaultOption.textContent = "Select peer";
  peerSelectEl.appendChild(defaultOption);

  for (const peer of peers || []) {
    const option = document.createElement("option");
    option.value = peer.username;
    option.dataset.ip = peer.ip;
    option.dataset.port = String(peer.port);
    option.dataset.username = peer.username;
    option.textContent = `${peer.username} (${peer.ip}:${peer.port})`;
    if (peer.username === prevValue) {
      option.selected = true;
    }
    peerSelectEl.appendChild(option);
  }
}

/**
 * Render chat messages into the message list.
 * @param {Array<object>|null|undefined} messages
 */
function renderMessages(messages) {
  messageListEl.textContent = "";
  if (!Array.isArray(messages) || messages.length === 0) {
    const emptyItem = document.createElement("li");
    emptyItem.textContent = "No messages yet";
    messageListEl.appendChild(emptyItem);
    return;
  }

  for (const msg of messages) {
    const target = msg.scope === "broadcast" ? "ALL" : msg.to_user;
    const item = document.createElement("li");
    item.textContent = `[${msg.from_user} -> ${target}] ${msg.text}`;
    messageListEl.appendChild(item);
  }
}

/**
 * Load message history from the peer daemon.
 * @returns {Promise<void>}
 */
async function loadMessages() {
  loadMsgBtn.disabled = true;
  const res = await apiGetMessages();
  loadMsgBtn.disabled = false;

  if (res.status === 401) {
    redirectToLogin();
    return;
  }
  if (res.status === 404) {
    return;
  }
  if (!res.ok) {
    setMessage(res.message, true);
    return;
  }

  renderMessages(res.data && res.data.messages);
}

/**
 * Send a direct message to the selected peer.
 * @returns {Promise<void>}
 */
async function handleSendClick() {
  const selected = peerSelectEl.options[peerSelectEl.selectedIndex];
  if (!selected || !selected.value) {
    setMessage("Please select a peer", true);
    return;
  }

  sendBtn.disabled = true;
  setMessage("");

  const res = await apiSendPeer(
    selected.dataset.ip,
    Number(selected.dataset.port),
    selected.dataset.username,
    chatInputEl.value,
  );
  sendBtn.disabled = false;

  if (res.status === 401) {
    redirectToLogin();
    return;
  }
  if (!res.ok) {
    setMessage(res.message, true);
    return;
  }

  chatInputEl.value = "";
  setMessage("Message sent");
  await loadMessages();
}

/**
 * Broadcast a message to all active peers.
 * @returns {Promise<void>}
 */
async function handleBroadcastClick() {
  broadcastBtn.disabled = true;
  setMessage("");

  const res = await apiBroadcastPeer(chatInputEl.value);
  broadcastBtn.disabled = false;

  if (res.status === 401) {
    redirectToLogin();
    return;
  }
  if (!res.ok) {
    setMessage(res.message, true);
    return;
  }

  chatInputEl.value = "";
  const count =
    res.data && typeof res.data.count === "number" ? res.data.count : 0;
  setMessage(`Broadcast sent to ${count} peer(s)`);
  await loadMessages();
}

sendBtn.addEventListener("click", handleSendClick);
broadcastBtn.addEventListener("click", handleBroadcastClick);
loadMsgBtn.addEventListener("click", loadMessages);
