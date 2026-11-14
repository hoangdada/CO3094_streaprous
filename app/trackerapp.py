import sys
import os
import importlib.util
import json
import secrets
import threading
from functools import wraps

from daemon import StreapRous

credentials = {
    "alice": "password123",
    "bob": "password456"
}

sessions = {}
peers = {}
state_lock = threading.Lock()

app = StreapRous()

def json_response(data, status_code=200, headers=None):
    content = json.dumps(data).encode("utf-8")
    return headers or {}, status_code, content

def parse_json_body(body):
    if isinstance(body, bytes):
        body = body.decode("utf-8")

    if not isinstance(body, str) or not body.strip():
        raise ValueError("Request body cannot be empty")

    data = json.loads(body)

    if not isinstance(data, dict):
        raise ValueError("JSON body must be an object")

    return data

def get_session_id(headers):
    """
    Extract the session ID from the Cookie header.
    """
    if not headers:
        return None

    cookie = headers.get("cookie")
    if not cookie:
        return None

    for item in cookie.split(";"):
        key, separator, value = item.strip().partition("=")
        if separator and key == "session_id":
            return value or None

    return None

def get_authenticated_user(headers):
    """
    Retrieve the authenticated username using the session cookie.
    """
    session_id = get_session_id(headers)

    if not session_id:
        return None

    with state_lock:
        return sessions.get(session_id)

def require_authentication(func):
    """
    Decorator that requires a valid session before executing a route.
    """
    @wraps(func)
    def wrapper(*args, **kwargs):
        # Retrieve request headers
        headers = kwargs.get("headers")

        if headers is None and args:
            headers = args[0]

        # Validate session
        username = get_authenticated_user(headers or {})

        if username is None:
            return json_response({"message": "Authentication required"}, 401)

        # Pass authenticated username to route handler
        kwargs["username"] = username

        return func(*args, **kwargs)

    return wrapper

@app.route('/login', methods=['POST'])
def login(headers="guest", body="anonymous",  client_ip=None):
    print("[SampleApp] Logging in {} to {}".format(headers, body))

    response_headers = {}
    try:
        data = parse_json_body(body)
        username = data.get("username")
        password = data.get("password")

    except (AttributeError, UnicodeDecodeError, json.JSONDecodeError):
        return json_response({"message": "Invalid request body"}, 400)

    if not isinstance(username, str) or not isinstance(password, str):
        return json_response({"message": "Username and password are required"}, 400)

    if credentials.get(username) != password:
        return json_response({"message": "Invalid username or password"}, 401)

    session_id = secrets.token_urlsafe(32)
    with state_lock:
        sessions[session_id] = username

    response_headers = {
        "Set-Cookie": f"session_id={session_id}; HttpOnly; Path=/; SameSite=Lax"
    }

    print(f"[TrackerApp] {username} logged in successfully")
    return json_response(
        {"message": "Login successful", "username": username},
        status_code=200,
        headers=response_headers
    )

@app.route('/submit-info', methods=['POST'])
@require_authentication
def submit_info(headers=None, body=None, username=None, client_ip=None):
    try:
        data = parse_json_body(body)
        port = data.get("port")

    except (AttributeError, UnicodeDecodeError, json.JSONDecodeError):
        return json_response({"message": "Invalid request body"}, 400)

    # Validate port
    if type(port) is not int or not (1 <= port <= 65535):
        return json_response({"message": "Invalid listening port"}, 400)

    with state_lock:
        is_new = username not in peers
        peers[username] = {
            "username": username,
            "ip": client_ip,
            "port": port
        }

    # Build response
    if is_new:
        status_code = 201
        message = "Peer registered successfully"
    else:
        status_code = 200
        message = "Peer information updated successfully"

    print("[TrackerApp] Registered peer {} at {}:{}".format(username, client_ip, port))
    return json_response(
        {
            "message": message,
            "username": username,
            "ip": client_ip,
            "port": port
        },
        status_code=status_code
    )

@app.route('/logout', methods=['POST'])
@require_authentication
def logout(headers=None, body=None, username=None, client_ip=None):
    session_id = get_session_id(headers)

    with state_lock:
        # Invalidate session
        if sessions.get(session_id) != username:
            return json_response({"message": "Invalid or expired session"}, 401)

        sessions.pop(session_id, None)
        peers.pop(username, None)

    print(f"[TrackerApp] {username} logged out successfully")
    return json_response({"message": "Logout successful"}, 200)

@app.route('/peer-list', methods=['GET'])
@require_authentication 
def peer_list(headers=None, body=None, username=None, client_ip=None):
    with state_lock:
        peer_snapshot = {
            peer_username: peer_info.copy()
            for peer_username, peer_info in peers.items()
            if peer_username != username
        }

    return json_response({"peers": peer_snapshot}, 200)

def create_trackerapp(ip, port):
    # Prepare and launch the RESTful application
    app.prepare_address(ip, port)
    app.run()