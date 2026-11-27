import threading
import time
import uuid

connections = {}
INBOX_MESSAGES = []
MESSAGE_HISTORY = []

lock = threading.Lock()

def sync_peer(peers_data):
    if isinstance(peers_data, dict):
        items = [
            {"username": uname, **info} if isinstance(info, dict) else {}
            for uname, info in peers_data.items()
        ]
    elif isinstance(peers_data, list):
        items = peers_data
    else:
        raise ValueError("peers_data must be a dict or list")

    new_connections = {}
    for entry in items:
        if not isinstance(entry, dict):
            raise ValueError("Each peer entry must be a dict")

        ip = entry.get("ip")
        port = entry.get("port")
        username = entry.get("username")

        if not isinstance(ip, str) or not ip.strip():
            raise ValueError("Invalid peer IP")
        if port is None:
            raise ValueError("Missing peer port")

        port_int = int(port)
        if not (1 <= port_int <= 65535):
            raise ValueError("Invalid peer port")

        new_connections[(ip, port_int)] = {
            "username": username or ip,
            "ip": ip,
            "port": port_int,
        }

    with lock:
        connections.clear()
        connections.update(new_connections)
        return [p.copy() for p in connections.values()]

def get_connected_peers():
    with lock:
        return [p.copy() for p in connections.values()]

def clear_connected_peers():
    with lock:
        connections.clear()

def push_incoming_p2p_message(from_user, from_ip, from_port,
                              to_user, scope, text, message_id=None,
                              timestamp=None):
    message = {
        "id": message_id or "msg-" + uuid.uuid4().hex[:12],
        "from_user": from_user,
        "from_ip": from_ip,
        "from_port": from_port,
        "to_user": to_user,
        "scope": scope,
        "text": text,
        "timestamp": timestamp if timestamp is not None else int(time.time())
    }

    with lock:
        if any(m["id"] == message["id"] for m in MESSAGE_HISTORY):
            return message

        INBOX_MESSAGES.append(message)
        MESSAGE_HISTORY.append(message)

    return message

def drain_inbox_messages():
    with lock:
        messages = INBOX_MESSAGES.copy()
        INBOX_MESSAGES.clear()

    return messages

def get_message_history():
    with lock:
        return [m.copy() for m in MESSAGE_HISTORY]

def save_sent_message(message):
    with lock:
        if not any(m["id"] == message["id"] for m in MESSAGE_HISTORY):
            MESSAGE_HISTORY.append(message.copy())
