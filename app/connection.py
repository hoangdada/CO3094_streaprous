import json
import queue
import random
import socket
import struct
import threading
import time

_outbound_lock = threading.Lock()
_outbound_raw_socket = None
_outbound_guard_socket = None
_outbound_local_port = None
_outbound_dispatcher_thread = None

from streaprous_socket.streaprous_packet_dispatcher import (
    _packet_dispatcher_lock,
    _packet_queues,
    get_packet_queue,
    remove_packet_queue,
)

from streaprous_socket.streaprous_raw_connection import (
    build_ip_header,
    build_tcp_packet,
    close_connection,
    conn_struct,
    connections,
    connections_lock,
    create_raw_socket,
    get_interface_ip,
    run_conn_recv,
    run_conn_send,
)


def json_response(data, status_code=200, headers=None):
    content = json.dumps(data).encode("utf-8")
    return headers or {}, status_code, content

def parse_response(response):
    if isinstance(response, bytes):
        response = response.decode("utf-8")

    header_block, _, body = response.partition("\r\n\r\n")
    lines = header_block.split("\r\n")

    status_parts = lines[0].split()
    try:
        status_code = int(status_parts[1])
    except (IndexError, ValueError):
        status_code = 500

    headers = {}
    for line in lines[1:]:
        key, sep, val = line.partition(":")
        if sep and key.strip().lower() == "set-cookie":
            headers["Set-Cookie"] = val.strip()

    if body.strip():
        try:
            data = json.loads(body)
        except json.JSONDecodeError:
            data = {"message": body.strip()}
    else:
        data = {}

    return json_response(data, status_code, headers)

def build_request(method, path, headers=None, body=b"", host_ip=None, host_port=None):
    target_ip = host_ip
    target_port = int(host_port)
    method = method.upper()

    if body is None:
        body_bytes = b""
    elif isinstance(body, bytes):
        body_bytes = body
    elif isinstance(body, str):
        body_bytes = body.encode("utf-8")
    else:
        body_bytes = json.dumps(body).encode("utf-8")

    # Start with request line and updated Host header
    lines = [
        f"{method} {path} HTTP/1.1",
        f"Host: {target_ip}:{target_port}",
    ]

    if body_bytes:
        lines.append("Content-Type: application/json")
        lines.append(f"Content-Length: {len(body_bytes)}")
    elif method in ("POST", "PUT"):
        lines.append("Content-Length: 0")

    # Check for cookie header
    for key, value in (headers or {}).items():
        if key.lower() == "cookie" and value:
            lines.append(f"Cookie: {value}")
            break

    header_block = ("\r\n".join(lines) + "\r\n\r\n").encode("utf-8")
    return header_block + body_bytes

def _outbound_dispatcher(raw_sock, local_port):
    """
    Background dispatcher thread reading from the dedicated outbound raw socket
    and routing packets destined for local_port into the shared _packet_queues.
    """
    raw_sock.settimeout(0.2)
    while True:
        try:
            packet, addr = raw_sock.recvfrom(65535)
        except socket.timeout:
            continue
        except OSError:
            break

        try:
            ihl = (packet[0] & 0x0F) * 4
            tcp = packet[ihl:]
            if len(tcp) < 20:
                continue

            src_port, dest_port = struct.unpack("!HH", tcp[:4])
            if dest_port != local_port:
                continue

            key = (socket.inet_ntoa(packet[12:16]), src_port)
            with _packet_dispatcher_lock:
                target_q = _packet_queues.get(key)

            if target_q is not None:
                try:
                    target_q.put_nowait((packet, addr))
                except queue.Full:
                    pass
        except Exception as e:
            print(f"[Connection] Outbound dispatcher error: {e}")

def _get_outbound_endpoint():
    """
    Initialize and reuse a single raw socket, an OS-reserved
    ephemeral TCP port, and its packet dispatcher thread.
    """
    global _outbound_raw_socket, _outbound_guard_socket, _outbound_local_port, _outbound_dispatcher_thread

    with _outbound_lock:
        if _outbound_raw_socket is None:
            guard_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            guard_sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            guard_sock.bind(("0.0.0.0", 0))
            local_port = guard_sock.getsockname()[1]

            raw_sock = create_raw_socket()
            dispatcher_thread = threading.Thread(
                target=_outbound_dispatcher,
                args=(raw_sock, local_port),
                daemon=True,
                name=f"outbound-packet-dispatcher-{local_port}",
            )
            dispatcher_thread.start()

            _outbound_guard_socket = guard_sock
            _outbound_raw_socket = raw_sock
            _outbound_local_port = local_port
            _outbound_dispatcher_thread = dispatcher_thread

        return _outbound_raw_socket, _outbound_local_port

def connect_to_peer(target_ip, target_port):
    if not target_ip or target_port is None:
        raise ValueError("Target IP and port are required")

    target_port = int(target_port)
    raw_socket, local_port = _get_outbound_endpoint()
    client_key = (target_ip, target_port)
    local_ip = get_interface_ip()

    # Clear any stale connection record or queued packets before sending SYN
    with connections_lock:
        connections.pop(client_key, None)
    remove_packet_queue(client_key)
    pkt_queue = get_packet_queue(client_key)

    local_isn = random.randint(1000, 100000)
    syn_tcp = build_tcp_packet(
        local_ip, target_ip, local_port, target_port,
        seq=local_isn, ack_seq=0, syn=1, ack=0
    )
    syn_ip = build_ip_header(local_ip, target_ip, socket.IPPROTO_TCP, len(syn_tcp))
    syn_packet = syn_ip + syn_tcp

    raw_socket.sendto(syn_packet, (target_ip, 0))
    deadline = time.time() + 10

    while time.time() < deadline:
        try:
            packet, _ = pkt_queue.get(timeout=0.1)
        except queue.Empty:
            continue

        ihl = (packet[0] & 0x0F) * 4
        tcp_packet = packet[ihl:]
        if len(tcp_packet) < 20:
            continue

        src_port, dest_port, pkt_seq, pkt_ack, _, flags = struct.unpack(
            "!HHLLBB", tcp_packet[:14]
        )
        if src_port != target_port or dest_port != local_port:
            continue

        syn_flag = (flags & 0x02) >> 1
        ack_flag = (flags & 0x10) >> 4
        rst_flag = (flags & 0x04) >> 2

        if rst_flag:
            remove_packet_queue(client_key)
            raise ConnectionRefusedError(f"Connection reset by {target_ip}:{target_port}")

        if syn_flag == 1 and ack_flag == 1 and pkt_ack == local_isn + 1:
            remote_isn = pkt_seq
            local_next_seq = local_isn + 1
            remote_next_seq = remote_isn + 1

            # Step 3: Send final ACK of 3-way handshake
            ack_tcp = build_tcp_packet(
                local_ip, target_ip, local_port, target_port,
                seq=local_next_seq, ack_seq=remote_next_seq, syn=0, ack=1
            )
            ack_ip = build_ip_header(
                local_ip, target_ip, socket.IPPROTO_TCP, len(ack_tcp)
            )
            raw_socket.sendto(ack_ip + ack_tcp, (target_ip, 0))

            conn_info = conn_struct(
                client_ip=target_ip,
                client_port=target_port,
                server_ip=local_ip,
                server_port=local_port,
                state="ESTABLISHED",
                server_seq=local_next_seq,
                client_seq=remote_next_seq,
                expected_final_ack=local_next_seq,
                last_activity=time.time(),
            )
            with connections_lock:
                connections[client_key] = conn_info

            return raw_socket, conn_info

    remove_packet_queue(client_key)
    raise TimeoutError(f"Handshake timed out connecting to {target_ip}:{target_port}")

def send_request(method, path, headers=None, body=b"", ip=None, port=None): 
    if not ip or port is None:
        raise ValueError("Target IP and port are required")

    target_ip = ip
    target_port = int(port)
    raw_socket, conn_info = connect_to_peer(target_ip, target_port)

    try:
        request = build_request(
            method, path, headers=headers, body=body,
            host_ip=target_ip, host_port=target_port
        )

        bytes_sent = run_conn_send(raw_socket, conn_info, request)
        if bytes_sent < 0:
            raise ConnectionError(f"Failed to send request to {target_ip}:{target_port}")

        response_buffer = bytearray()
        while True:
            chunk = run_conn_recv(raw_socket, conn_info)
            if not chunk:
                break

            response_buffer.extend(chunk)
            if is_response_complete(response_buffer):
                break

        if not response_buffer:
            raise ConnectionError(f"Empty response from {target_ip}:{target_port}")

        return parse_response(bytes(response_buffer))

    finally:
        close_connection(raw_socket, conn_info)

def is_response_complete(response_buffer: bytearray) -> bool:
    """
    Check whether a complete HTTP response has been received.
    """
    header_end = response_buffer.find(b"\r\n\r\n")
    if header_end == -1:
        return False

    # 2. Extract headers.
    header_data = response_buffer[:header_end]
    content_length = 0

    for line in header_data.split(b"\r\n")[1:]:
        key, separator, value = line.partition(b":")

        if not separator:
            continue

        if key.strip().lower() == b"content-length":
            content_length = int(value.strip())
            if content_length < 0:
                raise ValueError("Invalid Content-Length header")
            break

    # 4. Calculate received body bytes.
    body_start = header_end + 4
    received_body_length = len(response_buffer) - body_start
    return received_body_length >= content_length