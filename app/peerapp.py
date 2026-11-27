import json
import time
import uuid

from daemon import StreapRous
from app import peer, connection, tracker_client, p2p_client
from streaprous_socket.streaprous_raw_connection import get_interface_ip

PEER_IP = "127.0.0.1"
PEER_PORT = 8001

TRACKER_IP = "127.0.0.1"
TRACKER_PORT = 8000

app = StreapRous()
current_user = None

def set_tracker(ip, port):
    global TRACKER_IP, TRACKER_PORT
    TRACKER_IP = ip
    TRACKER_PORT = int(port)

def read_body(body):
    if isinstance(body, bytes):
        body = body.decode("utf-8")

    data = json.loads(body)

    if not isinstance(data, dict):
        raise ValueError("JSON body must be an object")

    return data

def json_response(data, status_code=200, headers=None):
    content = json.dumps(data).encode("utf-8")
    return headers or {}, status_code, content

def error(message, status=400):
    return json_response(
        {"message": message},
        status_code=status,
        headers={}
    )

def valid_port(port):
    port = int(port)
    if port < 1 or port > 65535:
        raise ValueError("Invalid port")
    return port

def create_message(from_user, to_user, text, scope="unicast"):
    return {
        "id": "msg-" + uuid.uuid4().hex[:12],
        "from_user": from_user,
        "from_ip": PEER_IP,
        "from_port": PEER_PORT,
        "to_user": to_user,
        "scope": scope,
        "text": text,
        "timestamp": int(time.time())
    }

# -------- Tracker APIs --------

@app.route('/login', methods=['POST'])
def login(headers, body):
    global current_user

    try:
        resp_headers, status_code, content = connection.send_request(
            method='POST',
            path='/login',
            headers=headers,
            body=body,
            ip=TRACKER_IP,
            port=TRACKER_PORT
        )
    except Exception as e:
        return error(f"Cannot communicate with Tracker: {e}", 502)

    if status_code == 200:
        resp_data = json.loads(content.decode("utf-8"))
        current_user = resp_data.get("username")

    return resp_headers, status_code, content

@app.route('/submit-info', methods=['POST'])
def submit_info(headers, body):
    try:
        return connection.send_request(
            method='POST',
            path='/submit-info',
            headers=headers,
            body=body,
            ip=TRACKER_IP,
            port=TRACKER_PORT
        )
    except Exception as e:
        return error(f"Cannot communicate with Tracker: {e}", 502)

@app.route('/logout', methods=['POST'])
def logout(headers, body):
    global current_user

    try:
        resp_headers, status_code, content = connection.send_request(
            method='POST',
            path='/logout',
            headers=headers,
            body=body,
            ip=TRACKER_IP,
            port=TRACKER_PORT
        )
    except Exception as e:
        return error(f"Cannot communicate with Tracker: {e}", 502)

    if status_code == 200:
        current_user = None
        peer.clear_connected_peers()

    return resp_headers, status_code, content

@app.route('/peer-list', methods=['GET'])
def get_list(headers, body):
    try:
        resp_headers, status_code, content = connection.send_request(
            method='GET',
            path='/peer-list',
            headers=headers,
            ip=TRACKER_IP,
            port=TRACKER_PORT
        )
    except Exception as e:
        return error(f"Cannot communicate with Tracker: {e}", 502)

    if status_code != 200:
        return resp_headers, status_code, content

    try:
        directory = json.loads(content.decode("utf-8"))
        raw_peers = directory.get("peers", {}) if isinstance(directory, dict) else directory
        synced_peers = peer.sync_peer(raw_peers)
    except (UnicodeDecodeError, json.JSONDecodeError, ValueError, TypeError) as e:
        return error(f"Invalid peer list from Tracker: {e}", 502)

    for item in synced_peers:
        item["connected"] = True

    return json_response({"peers": synced_peers}, 200)

# -------- Browser APIs --------

@app.route('/messages', methods=['GET'])
def get_messages(headers, body):
    return json_response({"messages": peer.drain_inbox_messages()}, 200)

# -------- Peer APIs --------

@app.route('/send-peer', methods=['POST'])
def send_peer(headers, body):
    if not current_user:
        return error("Please login first", 401)

    try:
        data = read_body(body)
        ip = data["target_ip"]
        port = valid_port(data["target_port"])
        text = data["message"]
    except (KeyError, ValueError, TypeError, json.JSONDecodeError) as e:
        return error(f"Invalid request: {e}", 400)

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

    message_body = create_message(current_user, target["username"], text, "unicast")

    try:
        _, status_code, content = connection.send_request(
            method='POST',
            path='/p2p-message',
            body=message_body,
            ip=ip,
            port=port
        )
    except Exception as e:
        return error(f"Cannot communicate with Peer: {e}", 502)

    resp_data = read_body(content)

    if status_code != 200 or resp_data.get("status") != "ok":
        return error(resp_data.get("message", "Peer rejected the message"), 502)

    peer.save_sent_message(message_body)

    return json_response({
        "status": "sent",
        "id": message_body["id"],
        "timestamp": message_body["timestamp"]
    }, 200)


@app.route('/broadcast-peer', methods=['POST'])
def broadcast_peer(headers, body):
    if not current_user:
        return error("Please login first", 401)

    try:
        data = read_body(body)
        text = data["message"]
    except (KeyError, ValueError, TypeError, json.JSONDecodeError) as e:
        return error(f"Invalid request: {e}", 400)

    if not isinstance(text, str) or not text.strip():
        return error("Message cannot be empty")

    message_body = create_message(current_user, None, text, "broadcast")
    count = 0

    for target in peer.get_connected_peers():
        try:
            _, status_code, content = connection.send_request(
                method='POST',
                path='/p2p-message',
                body=message_body,
                ip=target["ip"],
                port=target["port"]
            )
            resp_data = read_body(content)
            if status_code == 200 and resp_data.get("status") == "ok":
                count += 1
        except Exception as e:
            print(f"[PeerApp] Cannot broadcast to {target['username']}: {e}")

    if count > 0:
        peer.save_sent_message(message_body)

    return json_response({
        "status": "broadcasted",
        "count": count,
        "id": message_body["id"],
        "timestamp": message_body["timestamp"]
    }, 200)


# -------- Incoming P2P --------

@app.route('/p2p-message', methods=['POST'])
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

    return json_response({"status": "ok", "id": message["id"]})


def create_peerapp(ip, port, tracker_ip=None, tracker_port=None):
    global PEER_IP, PEER_PORT
    PEER_IP = get_interface_ip() if ip in (None, "", "0.0.0.0") else ip
    PEER_PORT = int(port)

    if tracker_ip is not None and tracker_port is not None:
        set_tracker(tracker_ip, tracker_port)

    app.prepare_address(ip, port)
    app.run()
