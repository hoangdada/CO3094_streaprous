#
# Copyright (C) 2026 pdnguyen of HCMC University of Technology VNU-HCM.
# All rights reserved.
# This file is part of the CO3093/CO3094 course.
#
# StreapRous release
#
# The authors hereby grant to Licensee personal permission to use
# and modify the Licensed Source Code for the sole purpose of studying
# while attending the course
#

"""
daemon.httpadapter
~~~~~~~~~~~~~~~~~

This module provides a http adapter object to manage and persist 
http settings (headers, bodies). The adapter supports both
raw URL paths and RESTful route definitions, and integrates with
Request and Response objects to handle client-server communication.
"""

from .request import Request
from .response import Response
from .dictionary import CaseInsensitiveDict

import asyncio
import inspect
import time

# Import streaprous raw connection functions
from streaprous_socket.streaprous_raw_connection import (
    run_conn_recv,
    run_conn_send,
    close_connection
)

class HttpAdapter:
    """
    A mutable :class:`HTTP adapter <HTTP adapter>` for managing client connections
    and routing requests.

    The `HttpAdapter` class encapsulates the logic for receiving HTTP requests,
    dispatching them to appropriate route handlers, and constructing responses.
    It supports RESTful routing via hooks and integrates with :class:`Request <Request>` 
    and :class:`Response <Response>` objects for full request lifecycle management.

    Attributes:
        ip (str): IP address of the client.
        port (int): Port number of the client.
        conn (socket): Active socket connection.
        connaddr (tuple): Address of the connected client.
        routes (dict): Mapping of route paths to handler functions.
        request (Request): Request object for parsing incoming data.
        response (Response): Response object for building and sending replies.
        conn_info (conn_struct): streaprous connection info for raw socket.
        raw_socket (socket): Raw socket for streaprous communication.
    """

    __attrs__ = [
        "ip",
        "port",
        "conn",
        "connaddr",
        "routes",
        "request",
        "response",
        "conn_info",
        "raw_socket",
    ]

    def __init__(self, ip, port, conn, connaddr, routes):
        """
        Initialize a new HttpAdapter instance.

        :param ip (str): IP address of the client.
        :param port (int): Port number of the client.
        :param conn (socket): Active socket connection.
        :param connaddr (tuple): Address of the connected client.
        :param routes (dict): Mapping of route paths to handler functions.
        """

        #: IP address.
        self.ip = ip
        #: Port.
        self.port = port
        #: Connection
        self.conn = conn
        #: Conndection address
        self.connaddr = connaddr
        #: Routes
        self.routes = routes
        #: Request
        self.request = Request()
        #: Response
        self.response = Response()
        #: streaprous connection info
        self.conn_info = None
        #: raw socket for streaprous communication
        self.raw_socket = None

    def handle_client(self, conn, addr, routes):
        """
        Handle an incoming client connection.

        This method reads the request from the socket, prepares the request object,
        invokes the appropriate route handler if available, builds the response,
        and sends it back to the client.

        :param conn (socket): The client socket connection.
        :param addr (tuple): The client's address.
        :param routes (dict): The route mapping for dispatching requests.
        """

        # Connection handler.
        self.conn = conn
        # Connection address.
        self.connaddr = addr
        # Request handler
        req = self.request
        # Response handler
        resp = self.response

        # Handle the request
        msg = conn.recv(1024).decode()
        req.prepare(msg, routes)
        print("[HttpAdapter] Invoke handle_client connection {}".format(addr))

        # Handle request hook
        response = b""
        try:
            if req.hook:
                #
                # TODO: handle for App hook here
                #
                response = b""
            else:
                # Build response
                response = resp.build_response(req)
        except Exception as e:
            print("[HttpAdapter] Error building response: {}".format(e))
            response = b"HTTP/1.1 500 Internal Server Error\r\nContent-Length: 0\r\n\r\n"

        #print("[HttpAdapter] Response content {}".format(response))
        conn.sendall(response)
        conn.close()

    def handle_client_raw(self, raw_socket, conn_info, routes):
        """
        Handle an incoming client connection using streaprous raw connection.

        This method reads the request from the raw socket, prepares the request object,
        invokes the appropriate route handler if available, builds the response,
        and sends it back to the client using raw sockets.

        :param raw_socket (socket): The raw socket for communication.
        :param conn_info (conn_struct): The connection info for the client.
        :param routes (dict): The route mapping for dispatching requests.
        """
        # Request handler
        req = self.request
        # Response handler
        resp = self.response

        print("[HttpAdapter] Invoke handle_client_raw for {}:{}".format(conn_info.client_ip, conn_info.client_port))

        data = bytearray()

        while True:
            chunk = run_conn_recv(raw_socket, conn_info)

            if not chunk:
                print("[HttpAdapter] Connection closed before request completion")
                return

            data.extend(chunk)

            if is_request_complete(data):
                break

        # Receive data using streaprous raw connection
        # data = run_conn_recv(raw_socket, conn_info)
        
        if not data:
            print("[HttpAdapter] No data received or connection closed")
            return

        # Prepare request
        msg = data.decode("utf-8")
        req.prepare(msg, routes)
        
        # Handle request hook
        if req.hook:
            response_data = req.hook(
                headers=req.headers,
                body=req.body
            )

            response = resp.build_response(req, response_data)
        else:
            # Build response - returns bytes
            response = resp.build_response(req)
        
        # Send response using streaprous raw connection
        response = response if isinstance(response, bytes) else response.encode("utf-8")
        print(f"[HttpAdapter] HTTP Response ({len(response)} bytes): {response[:200]}")
        #
        # TODO run_conn_send packet
        run_conn_send(raw_socket, conn_info, response)
        
        # Wait longer to ensure response is transmitted, client processes it, and sends ACK
        import time
        time.sleep(5.0)
        
        # Close connection with FIN packet
        #
        # TODO run_conn_send packet
        close_connection(raw_socket, conn_info)

    async def handle_client_coroutine(self, reader, writer):
        """
        Handle an incoming client connection using stream reader writer asynchronously.

        This method reads the request from the socket, prepares the request object,
        invokes the appropriate route handler if available, builds the response,
        and sends it back to the client.

        :param conn (socket): The client socket connection.
        :param addr (tuple): The client's address.
        :param routes (dict): The route mapping for dispatching requests.
        """
        # Request handler
        req = self.request
        # Response handler
        resp = self.response

        print("[HttpAdapter] Invoke handle_client_coroutine connection {})".format(addr))
        addr = writer.get_extra_info("peername")

        # TODO Handle the request asynchronously
        msg = await reader.read(1024)


        req.prepare(msg.decode("utf-8"), routes={})

        # Handle request hook
        if req.hook:
            #
            # TODO: handle for App hook here
            #
            response = ""

        # Build response
        #print("[HttpAdapter] Start **ASYNC** build_response with type {}".format(type(req)))
        response = resp.build_response(req)

        # Send all the response asynchronously
        writer.write(response)
        await writer.drain()

def is_request_complete(request_buffer: bytearray) -> bool:
    """
    Check whether a complete HTTP request has been received.

    Supports:
        - Requests without a body
        - Requests with Content-Length
    """

    # 1. Find the end of HTTP headers.
    header_end = request_buffer.find(b"\r\n\r\n")

    if header_end == -1:
        return False

    # 2. Extract headers.
    header_data = request_buffer[:header_end]
    content_length = 0

    for line in header_data.split(b"\r\n")[1:]:
        key, separator, value = line.partition(b":")

        if not separator:
            continue

        if key.strip().lower() == b"content-length":
            content_length = int(value.strip())
            break

    # 4. Calculate received body bytes.
    body_start = header_end + 4
    received_body_length = len(request_buffer) - body_start

    # 5. Check completeness.
    return received_body_length >= content_length