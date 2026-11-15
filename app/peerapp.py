import json

from daemon import StreapRous
from app import peer, tracker_client, p2p_client


app = StreapRous()
current_user = None


def read_body(body):
    if isinstance(body, bytes):
        body = body.decode("utf-8")

    data = json.loads(body)

    if not isinstance(data, dict):
        raise ValueError("JSON body must be an object")

    return data

def get_cookie(headers):
    for key, value in (headers or {}).items():
        if key.lower() == "cookie":
            return value
    return None

def json_response(data, status_code=200, headers=None):
    content = json.dumps(data).encode("utf-8")
    return headers or {}, status_code, content

def error(message, status=400):
    return json_response(
        {"message": message},
        status_code=status,
        headers={}
    )

def tracker_result(result):
    headers = {}
    if result.get("set_cookie"):
        headers["Set-Cookie"] = result["set_cookie"]

    return json_response(
        result.get("data", {}),
        status_code=result["status_code"],
        headers=headers
    )

def valid_port(port):
    port = int(port)
    if port < 1 or port > 65535:
        raise ValueError("Invalid port")
    return port


# -------- Tracker APIs --------

@app.route('/login', methods=['POST'])
def login(headers, body, client_ip=None):
    global current_user

    try:
        data = read_body(body)
        username = data["username"]
        password = data["password"]
    except ValueError, KeyError:
        return error("Invalid username or password payload", 400)

    result = tracker_client.login(username, password)

    if result["status_code"] == 200:
        current_user = username

    return tracker_result(result)

@app.route('/submit-info', methods=['POST'])
def submit_info(headers, body, client_ip=None):
    try:
        data = read_body(body)
        port = valid_port(data["port"])
    except ValueError, KeyError:
        return error("Invalid port", 400)

    result = tracker_client.submit_info(port, get_cookie(headers))
    return tracker_result(result)

@app.route('/logout', methods=['POST'])
def logout(headers, body, client_ip=None):
    global current_user

    result = tracker_client.logout(get_cookie(headers))
    if result["status_code"] == 200:
        current_user = None

    return tracker_result(result)

@app.route('/get-list', methods=['GET'])
def get_list(headers, body, client_ip=None):
    result = tracker_client.get_list(get_cookie(headers))

    if result["status_code"] != 200:
        return tracker_result(result)

    directory = result.get("data", {})
    raw_peers = directory.get("peers", {}) if isinstance(directory, dict) else directory

    if isinstance(raw_peers, dict):
        peers = [
            {"username": uname, **info}
            for uname, info in raw_peers.items()
        ]
    else:
        peers = list(raw_peers)

    connected = peer.get_connected_peers()
    for item in peers:
        item["connected"] = any(
            p["ip"] == item["ip"] and p["port"] == int(item["port"])
            for p in connected
        )

    return json_response({"peers": peers}, 200)

@app.route('/messages', methods=['GET'])
def get_messages(headers, body, client_ip=None):
    return json_response({"messages": peer.drain_inbox_messages()}, 200)

# -------- Peer APIs --------

@app.route('/connect-peer/', methods=['POST'])
def connect_peer(headers, body):
    data = read_body(body)

    ip = data["ip"]
    port = valid_port(data["port"])

    raw_socket, conn_info = p2p_client.connect_to_peer(ip, port)

    from streaprous_socket.streaprous_raw_connection import close_connection
    close_connection(raw_socket, conn_info)

    peer.connect_peer(data.get("username", ip), ip, port)

    return {
        "status": "connected",
        "ip": ip,
        "port": port
    }


@app.route('/send-peer/', methods=['POST'])
def send_peer(headers, body):
    if not current_user:
        return error("Please login first", 401)

    data = read_body(body)
    ip = data["target_ip"]
    port = valid_port(data["target_port"])
    text = data["message"]

    if not isinstance(text, str) or not text.strip():
        return error("Message cannot be empty")

    connected = peer.get_connected_peers()

    target = next(
        (p for p in connected
         if p["ip"] == ip and p["port"] == port),
        None
    )

    if target is None:
        return error("Peer is not connected")

    result = p2p_client.send_message(
        ip, port, current_user, target["username"], text
    )

    peer.save_sent_message(result["message"])

    return {
        "status": "sent",
        "id": result["id"],
        "timestamp": result["timestamp"]
    }


@app.route('/broadcast-peer/', methods=['POST'])
def broadcast_peer(headers, body):
    if not current_user:
        return error("Please login first", 401)

    data = read_body(body)
    text = data["message"]

    if not isinstance(text, str) or not text.strip():
        return error("Message cannot be empty")

    result = p2p_client.broadcast_message(
        peer.get_connected_peers(), current_user, text
    )

    if result["count"] > 0:
        peer.save_sent_message(result["message"])

    return {
        "status": "broadcasted",
        "count": result["count"],
        "id": result["id"],
        "timestamp": result["timestamp"]
    }


# -------- Incoming P2P --------

@app.route('/p2p-message/', methods=['POST'])
def receive_p2p_message(headers, body):
    data = read_body(body)

    for field in ("id", "from_user", "scope", "text", "timestamp"):
        if field not in data:
            return error("Missing field: " + field)

    if data["scope"] not in ("unicast", "broadcast"):
        return error("Invalid message scope")

    if not isinstance(data["text"], str):
        return error("Invalid message")

    message = peer.push_incoming_p2p_message(
        data["from_user"],
        data.get("from_ip"),
        data.get("from_port"),
        data.get("to_user"),
        data["scope"],
        data["text"],
        data["id"],
        data["timestamp"]
    )

    return {"status": "ok", "id": message["id"]}


def create_peerapp(ip, port):
    app.prepare_address(ip, port)
    app.run()
