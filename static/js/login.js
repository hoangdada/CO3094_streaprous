"use strict";

const loginForm = document.getElementById("login-form");
const usernameInput = document.getElementById("username");
const passwordInput = document.getElementById("password");
const loginBtn = document.getElementById("login-btn");
const messageEl = document.getElementById("login-message");

/**
 * Authenticate user on form submit and redirect on success.
 * @param {SubmitEvent} event
 * @returns {Promise<void>}
 */
async function handleLoginSubmit(event) {
  event.preventDefault();

  const username = usernameInput.value.trim();
  const password = passwordInput.value;

  messageEl.textContent = "";
  messageEl.className = "";
  loginBtn.disabled = true;

  const res = await apiLogin(username, password);
  if (!res.ok) {
    loginBtn.disabled = false;
    messageEl.className = "error";
    messageEl.textContent = res.message;
    return;
  }

  const loggedInUser = (res.data && res.data.username) || username;
  sessionStorage.setItem("username", loggedInUser);
  window.location.href = "/index.html";
}

loginForm.addEventListener("submit", handleLoginSubmit);
