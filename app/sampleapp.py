#
# Copyright (C) 2026 pdnguyen of HCMC University of Technology VNU-HCM.
# All rights reserved.
# This file is part of the CO3093/CO3094 course,
# and is released under the "MIT License Agreement". Please see the LICENSE
# file that should have been included as part of this package.
#
# StreapRous release
#
# The authors hereby grant to Licensee personal permission to use
# and modify the Licensed Source Code for the sole purpose of studying
# while attending the course
#


"""
app.sampleapp
~~~~~~~~~~~~~~~~~

"""

import sys
import os
import importlib.util
import json

from daemon import StreapRous
from . import tracker, peer

app = StreapRous()


@app.route('/login', methods=['POST'])
def login(headers="guest", body="anonymous"):
    """
    Handle user login via POST request.

    This route simulates a login process and prints the provided headers and body
    to the console.

    :param headers (str): The request headers or user identifier.
    :param body (str): The request body or login payload.
    """
    print("[SampleApp] Logging in {} to {}".format(headers, body))
    try:
        username = json.loads(body).get("username")
        if not username:
            data = {"error": "Username is required"}
        else:
            tracker.login(username)
            data = {"status": "success", "username": username}
    except (ValueError, AttributeError, TypeError):
        data = {"error": "Invalid JSON"}

    # Convert to JSON string
    json_str = json.dumps(data)
    return (json_str.encode("utf-8"))


@app.route('/echo', methods=['POST'])
def echo(headers="guest", body="anonymous"):
    print("[SampleApp] received body {}".format(body))

    try:
        message = json.loads(body)
        data = {"received": message }
        # Convert to JSON string
        json_str = json.dumps(data)
        return json_str.encode("utf-8")
    except json.JSONDecodeError:
        data = {"error": "Invalid JSON"}
        # Convert to JSON string
        json_str = json.dumps(data)
        return (json_str.encode("utf-8"))


@app.route('/hello', methods=['PUT'])
async def hello(headers, body):
    """
    Handle greeting via PUT request.

    This route prints a greeting message to the console using the provided headers
    and body.

    :param headers (str): The request headers or user identifier.
    :param body (str): The request body or message payload.
    """
    print("[SampleApp] ['PUT'] **ASYNC** Hello in {} to {}".format(headers, body))
    data = {"id": 1, "name": "Alice", "email": "alice@example.com"}

    # Convert to JSON string
    json_str = json.dumps(data)
    return (json_str.encode("utf-8"))


@app.route('/add-list/', methods=['POST'])
def add_list(headers, body):
    try:
        data = json.loads(body)
        success = tracker.add_contact(data.get("username"), data.get("peer"))
        result = {"status": "success"} if success else {"error": "User or peer not found"}
    except (ValueError, AttributeError, TypeError):
        data = {"error": "Invalid JSON"}
        
    return json.dumps(result).encode("utf-8")


@app.route('/get-list/', methods=['GET'])
def get_list(headers, body):
    return json.dumps({"peers": tracker.get_list()}).encode("utf-8")


@app.route('/connect-peer/', methods=['POST'])
def connect_peer(headers, body):
    try:
        data = json.loads(body)
        target = data.get("peer")
        info = tracker.find_peer(target)
        if not info:
            result = {"error": "Peer not found or offline"}
        elif peer.connect_peer(target, info["ip"], info["port"]):
            result = {"status": "connected", "peer": target}
        else:
            result = {"error": "Connection failed"}
    except (ValueError, AttributeError, TypeError):
        result = {"error": "Invalid JSON"}
    return json.dumps(result).encode("utf-8")


@app.route('/send-peer/', methods=['POST'])
def send_peer(headers, body):
    try:
        data = json.loads(body)
        target, message = data.get("peer"), data.get("message")
        if not target or not isinstance(message, str):
            result = {"error": "Peer and message are required"}
        elif peer.send_peer(target, message):
            result = {"status": "sent"}
        else:
            result = {"error": "Message not sent"}
    except (ValueError, AttributeError, TypeError):
        result = {"error": "Invalid JSON"}
    return json.dumps(result).encode("utf-8")


@app.route('/broadcast-peer/', methods=['POST'])
def broadcast_peer(headers, body):
    try:
        data = json.loads(body)
        message = data.get("message")
        if not isinstance(message, str):
            result = {"error": "Message is required"}
        else:
            result = {"status": "completed", "results": peer.broadcast_peer(message)}
    except (ValueError, AttributeError, TypeError):
        result = {"error": "Invalid JSON"}
    return json.dumps(result).encode("utf-8")


@app.route('/submit-info/', methods=['POST'])
def submit_info(headers, body):
    try:
        data = json.loads(body)

        username = data.get("username")
        ip = data.get("ip")
        port = data.get("port")

        if not username or not ip or not port:
            return json.dumps({
                "error": "Missing username, ip or port"
            }).encode("utf-8")

        result = tracker.submit_info(username, ip, port)

        if result:
            return json.dumps({
                "status": "success"
            }).encode("utf-8")

        return json.dumps({
            "error": "User not found"
        }).encode("utf-8")

    except (json.JSONDecodeError, TypeError, ValueError):
        return json.dumps({
            "error": "Invalid JSON"
        }).encode("utf-8")

    
def create_sampleapp(ip, port):
    # Prepare and launch the RESTful application
    app.prepare_address(ip, port)
    app.run()

