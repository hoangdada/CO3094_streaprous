import argparse

from app.peerapp import create_peerapp
from app import tracker_client
from app import tracker_client, p2p_client

def main():
    parser = argparse.ArgumentParser(description="StreapRous Peer Server")

    parser.add_argument("--ip", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=8001)
    parser.add_argument("--tracker-ip", default="127.0.0.1")
    parser.add_argument("--tracker-port", type=int, default=8000)

    args = parser.parse_args()

    tracker_client.set_tracker(args.tracker_ip, args.tracker_port)
    p2p_client.set_local_address(args.ip, args.port)

    print("[Peer] Starting server on {}:{}".format(args.ip, args.port))
    print("[Peer] Tracker: {}:{}".format(args.tracker_ip, args.tracker_port))

    create_peerapp(args.ip, args.port)


if __name__ == "__main__":
    main()
