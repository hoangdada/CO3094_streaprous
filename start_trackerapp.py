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

from app.trackerapp import app as tracker_app, create_trackerapp
from daemon.backend import create_backend
from start_backend import setup_rst_block

PORT = 2026  # Default port

if __name__ == "__main__":
    # Parse command-line arguments to configure server IP and port
    parser = argparse.ArgumentParser(prog='Backend', description='', epilog='Beckend daemon')
    parser.add_argument('--server-ip', default='0.0.0.0')
    parser.add_argument('--server-port', type=int, default=PORT)
    parser.add_argument('--interface', default=None)

    args = parser.parse_args()
    ip = args.server_ip
    port = args.server_port

    is_localhost = (
        ip in ('127.0.0.1', '::1', 'localhost') or ip.startswith('127.')
    )
    if not is_localhost:
        setup_rst_block(ip, port)

    if args.interface:
        create_backend(ip, port, tracker_app.routes, args.interface)
    else:
        create_trackerapp(ip, port)
