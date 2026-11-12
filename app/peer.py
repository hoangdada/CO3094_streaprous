
import socket
import threading

connections = {}
lock = threading.Lock()


def receive_messages(conn, addr):
    try:
        while True:
            data = conn.recv(4096)

            if not data:
                break

            message = data.decode("utf-8")
            print("[P2P] From {}: {}".format(addr, message))

    except OSError as e:
        print("[P2P] Receive error:", e)

    finally:
        with lock:
            for username, sock in list(connections.items()):
                if sock is conn:
                    del connections[username]

        conn.close()


def start_peer_server(ip, port):
    server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)

    server.bind((ip, port))
    server.listen(5)

    print("[P2P] Listening on {}:{}".format(ip, port))

    while True:
        conn, addr = server.accept()

        thread = threading.Thread(
            target=receive_messages,
            args=(conn, addr),
            daemon=True
        )
        thread.start()


def connect_peer(username, ip, port):
    try:
        sock = socket.create_connection((ip, port), timeout=5)
        sock.settimeout(None)

        with lock:
            old = connections.get(username)
            connections[username] = sock

        if old:
            old.close()

        print("[P2P] Connected to", username)
        return True

    except OSError as e:
        print("[P2P] Connection error:", e)
        return False


def send_peer(username, message):
    with lock:
        sock = connections.get(username)

        if sock is None:
            return False

        try:
            sock.sendall(message.encode("utf-8"))
            print("[P2P] Message sent to", username)
            return True

        except OSError as e:
            print("[P2P] Send error:", e)

            del connections[username]
            sock.close()
            return False


def broadcast_peer(message):
    results = {}

    with lock:
        names = list(connections.keys())

    for username in names:
        results[username] = send_peer(username, message)

    return results
