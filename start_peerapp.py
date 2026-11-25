#
# Copyright (C) 2026 pdnguyen of HCMC University of Technology VNU-HCM.
# All rights reserved.
# This file is part of the CO3093/CO3094 course,
# and is released under the LICENSE. Please see the LICENSE
# file that should have been included as part of this package.
#
# AsynapRous release
#
# The authors hereby grant to Licensee personal permission to use
# and modify the Licensed Source Code for the sole purpose of studying
# while attending the course
#


"""
start_sampleapp
~~~~~~~~~~~~~~~~~

This module provides a sample RESTful web application using the AsynapRous framework.

It defines basic route handlers and launches a TCP-based backend server to serve
HTTP requests. The application includes a login endpoint and a greeting endpoint,
and can be configured via command-line arguments.
"""

import json
import socket
import argparse

from app import create_peerapp
from start_backend import setup_rst_block

PORT = 8001
DEFAULT_TRACKER_IP = "127.0.0.1"
DEFAULT_TRACKER_PORT = 2026

if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        prog="Backend",
        description="Start peer backend daemon",
        epilog="Backend daemon",
    )
    parser.add_argument("--server-ip", default="0.0.0.0")
    parser.add_argument("--server-port", type=int, default=PORT)
    parser.add_argument("--tracker-ip", default=DEFAULT_TRACKER_IP)
    parser.add_argument("--tracker-port", type=int, default=DEFAULT_TRACKER_PORT)
    parser.add_argument("--interface", default=None)

    args = parser.parse_args()
    ip = args.server_ip
    port = args.server_port

    is_localhost = ip in ("127.0.0.1", "::1", "localhost") or ip.startswith("127.")
    if not is_localhost:
        setup_rst_block(ip, port)

    create_peerapp(
        ip=ip,
        port=port,
        tracker_ip=args.tracker_ip,
        tracker_port=args.tracker_port,
        interface=args.interface,
    )
