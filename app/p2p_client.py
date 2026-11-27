import json
import time
import uuid

from streaprous_socket.streaprous_raw_connection import (
    run_conn_recv,
    run_conn_send,
    close_connection
)


LOCAL_IP = "127.0.0.1"
LOCAL_PORT = 8001


def set_local_address(ip, port):
    global LOCAL_IP, LOCAL_PORT
    LOCAL_IP = ip
    LOCAL_PORT = int(port)


def create_message(from_user, to_user, message, scope="unicast"):
    return {
        "id": "msg-" + uuid.uuid4().hex[:12],
        "from_user": from_user,
        "from_ip": LOCAL_IP,
        "from_port": LOCAL_PORT,
        "to_user": to_user,
        "scope": scope,
        "text": message,
        "timestamp": int(time.time())
    }


def build_request(path, data):
    body = json.dumps(data).encode("utf-8")

    headers = (
        f"POST {path} HTTP/1.1\r\n"
        "Content-Type: application/json\r\n"
        f"Content-Length: {len(body)}\r\n"
        "Connection: close\r\n"
        "\r\n"
    )

    return headers.encode("utf-8") + body


def parse_response(response):
    if not response:
        raise ConnectionError("Empty response from peer")

    if isinstance(response, str):
        response = response.encode("utf-8")

    header, separator, body = response.partition(b"\r\n\r\n")

    if not separator:
        raise ValueError("Invalid HTTP response")

    status_line = header.split(b"\r\n", 1)[0].decode("utf-8")
    status = int(status_line.split()[1])

    result = json.loads(body.decode("utf-8")) if body else {}

    if status != 200 or result.get("status") != "ok":
        raise ConnectionError("Peer rejected the message")

    return result


def connect_to_peer(ip, port):
    # TODO (Subteam 1):
    # Use StreapRous outbound connection API.
    # Return (raw_socket, conn_info).
    raise NotImplementedError("Waiting for StreapRous connection API")


def send_request(ip, port, path, data):
    raw_socket, conn_info = connect_to_peer(ip, port)

    try:
        request = build_request(path, data)

        run_conn_send(raw_socket, conn_info, request)
        response = run_conn_recv(raw_socket, conn_info)

        return parse_response(response)

    finally:
        close_connection(raw_socket, conn_info)


def send_message(ip, port, from_user, to_user, message):
    data = create_message(from_user, to_user, message)

    send_request(ip, port, "/p2p-message/", data)

    return {
        "status": "sent",
        "id": data["id"],
        "timestamp": data["timestamp"],
        "message": data
    }


def broadcast_message(peers, from_user, message):
    data = create_message(from_user, None, message, "broadcast")
    count = 0

    for peer in peers:
        try:
            send_request(
                peer["ip"],
                peer["port"],
                "/p2p-message/",
                data
            )
            count += 1

        except Exception as e:
            print("[P2PClient] Cannot send to", peer["username"], e)

    return {
        "status": "broadcasted",
        "count": count,
        "id": data["id"],
        "timestamp": data["timestamp"],
        "message": data
    }
