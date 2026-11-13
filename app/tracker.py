
from threading import Lock

peers = {}
contacts = {}
lock = Lock()


def login(username):
    with lock:
        if username not in peers:
            peers[username] = {
                "ip": None,
                "port": None,
                "online": False
            }

        return peers[username].copy()


def submit_info(username, ip, port):
    with lock:
        if username not in peers:
            return False

        peers[username] = {
            "ip": ip,
            "port": port,
            "online": True
        }

        return True


def get_list():
    with lock:
        return {
            name: info.copy()
            for name, info in peers.items()
            if info["online"]
        }


def find_peer(username):
    with lock:
        peer = peers.get(username)

        if peer and peer["online"]:
            return peer.copy()

        return None


def add_contact(username, peer):
    with lock:
        if username not in peers or peer not in peers:
            return False

        if username not in contacts:
            contacts[username] = []

        if peer not in contacts[username]:
            contacts[username].append(peer)

        return True
