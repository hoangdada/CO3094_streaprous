import threading
import time
import uuid


connections = {}
INBOX_MESSAGES = []
MESSAGE_HISTORY = []

lock = threading.Lock()


def connect_peer(username, ip, port):
    with lock:
        connections[(ip, int(port))] = {
            "username": username,
            "ip": ip,
            "port": int(port)
        }

    return True


def disconnect_peer(ip, port):
    with lock:
        return connections.pop((ip, int(port)), None) is not None


def get_connected_peers():
    with lock:
        return [p.copy() for p in connections.values()]


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
