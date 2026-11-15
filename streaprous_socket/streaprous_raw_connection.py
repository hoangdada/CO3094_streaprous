#
# Copyright (C) 2026 pdnguyen of HCMC University of Technology VNU-HCM.
# All rights reserved.
# This file is part of the CO3093/CO3094 course,
# and is released under the LICENSE. Please see the LICENSE
# file that should have been included as part of this package.
#
# StreapRous release
#
# The authors hereby grant to Licensee personal permission to use
# and modify the Licensed Source Code for the sole purpose of studying
# while attending the course
#

"""
StreapRous Raw Connection Library
A library for handling TCP connections with raw sockets.
Based on raw_connection_server.py implementation.
"""

import socket
import struct
import sys
import threading
import time
import queue
from typing import Dict, List, Tuple
from collections import namedtuple
import netifaces

# Import streaprous packet dispatcher functions
from .streaprous_packet_dispatcher import (
    start_packet_dispatcher,
    stop_packet_dispatcher,
    get_packet_queue,
    remove_packet_queue,
    _packet_dispatcher_lock,
    _SYN_KEY,
)


# Connection structure 
# This structure holds connection and its state information
conn_struct = namedtuple('conn_struct', [
    'client_ip',
    'client_port',
    'server_ip',
    'server_port',
    'state',
    'server_seq',
    'client_seq',
    'expected_final_ack',
    'last_activity'
])


# Global table to manage multiple connection
# The mapping include {key: value} as { (client_ip, client_port): conn_struct }
connections = {}
connections_lock = threading.Lock()

# Management structure to serialize the 3-way handshake
# Each hand shake is manage by a unique ID client_key
# including client_ip client_port
handshake_lock = threading.Lock()
handshake_in_progress = {}


# Default raw port listener
DEFAULT_LISTEN_IP = "0.0.0.0"
DEFAULT_LISTEN_PORT = 22239
DEFAULT_INTERFACE_NAME = "lo0"

RECEIVER_IDLE_TIMEOUT = 60
RECEIVE_CAP = 5840       # Receiver buffer capacity (Flow Receive Window)
MESSAGE_SIZE = 1000      # Segment MSS
SEND_WINDOW = 4          # Max segments in flight
RETRANSMISSION_TIMEOUT = 2.0
MAX_RETRIES = 5

FLOW_CONTROL_ZWP_INTERVAL = 0.5  
FLOW_CONTROL_MAX_ZWP_INTERVAL = 60.0

_SEQ_MOD = 1 << 32    

# Selective Repeat receive buffer per connection: client_key -> {seq: payload}
_early_segments: Dict[Tuple[str, int], Dict[int, bytes]] = {}
_early_segments_lock = threading.Lock()

# Flow Control state: peer receive window tracking per (client_ip, client_port)
_peer_rwnd: Dict[Tuple[str, int], int] = {}
_max_peer_rwnd: Dict[Tuple[str, int], int] = {}
_peer_rwnd_lock = threading.Lock()

# Flow Control state: receiver right window edge tracking (RFC 1122 Do Not Shrink Window)
_rcv_wnd_edge: Dict[Tuple[str, int], int] = {}
_rcv_wnd_edge_lock = threading.Lock()


def _get_peer_rwnd(client_key: Tuple[str, int]) -> int:
    """Get the current peer receive window (rwnd) for client_key."""
    with _peer_rwnd_lock:
        return _peer_rwnd.get(client_key, RECEIVE_CAP)


def _get_max_peer_rwnd(client_key: Tuple[str, int]) -> int:
    """Get the maximum observed peer receive window for client_key."""
    with _peer_rwnd_lock:
        return _max_peer_rwnd.get(client_key, RECEIVE_CAP)


def _set_peer_rwnd(client_key: Tuple[str, int], rwnd: int) -> None:
    """Update peer receive window for client_key."""
    with _peer_rwnd_lock:
        clamped_rwnd = max(0, min(65535, int(rwnd)))
        _peer_rwnd[client_key] = clamped_rwnd
        current_max = _max_peer_rwnd.get(client_key, 0)
        if clamped_rwnd > current_max:
            _max_peer_rwnd[client_key] = clamped_rwnd


def _free_receive_window(client_key: Tuple[str, int] = None, rcv_nxt: int = None) -> int:
    if client_key is not None:
        with _early_segments_lock:
            buf = _early_segments.get(client_key, {})
            buffered = sum(len(p) for p in buf.values())
    else:
        buffered = 0

    free_space = max(0, RECEIVE_CAP - buffered)

    # Clark's algorithm: if buffer nearly full, keep rwnd = 0 until space >= min(MSS, RcvBuffer // 2)
    sws_threshold = min(MESSAGE_SIZE, RECEIVE_CAP // 2)
    if buffered > 0 and free_space < sws_threshold:
        adv_rwnd = 0
    else:
        adv_rwnd = free_space

    # RFC 1122: Do not shrink window edge
    if client_key is not None and rcv_nxt is not None:
        with _rcv_wnd_edge_lock:
            adv_rwnd = max(0, min(65535, adv_rwnd))
            _rcv_wnd_edge[client_key] = (rcv_nxt + adv_rwnd) % _SEQ_MOD
    else:
        adv_rwnd = max(0, min(65535, adv_rwnd))

    return adv_rwnd


def _cleanup_connection_flow_control(client_key: Tuple[str, int]) -> None:
    """Clean up connection state."""
    with _early_segments_lock:
        _early_segments.pop(client_key, None)
    with _peer_rwnd_lock:
        _peer_rwnd.pop(client_key, None)
        _max_peer_rwnd.pop(client_key, None)
    with _rcv_wnd_edge_lock:
        _rcv_wnd_edge.pop(client_key, None)


def _load_connection_record(connection_key):
    with connections_lock:
        return connections.get(connection_key)


def _forget_connection(connection_key):
    with connections_lock:
        connections.pop(connection_key, None)
    _cleanup_connection_flow_control(connection_key)
    remove_packet_queue(connection_key)


def _send_pure_ack(raw_socket, connection_record, ack_number):
    client_key = (connection_record.client_ip, connection_record.client_port)
    window = _free_receive_window(client_key, ack_number)
    tcp_header = build_tcp_packet(
        connection_record.server_ip, connection_record.client_ip,
        connection_record.server_port, connection_record.client_port,
        connection_record.server_seq, ack_seq=ack_number, ack=1, window=window
    )
    ip_header = build_ip_header(connection_record.server_ip, connection_record.client_ip, socket.IPPROTO_TCP, len(tcp_header))
    raw_socket.sendto(ip_header + tcp_header, (connection_record.client_ip, 0))


def register_keep_port_alive(listen_ip="0.0.0.0", listen_port=22239, retry_interval=10):
    """
    Periodically register and maintain TCP port.
    Retry registration whenever binding/listening fails.
    Never accept incoming connections.
    
    Args:
        listen_ip: IP address to bind to (default: "0.0.0.0")
        listen_port: Port to bind to (default: 22239)
        retry_interval: Seconds to wait before retrying after failure (default: 10)
    
    Returns:
        threading.Thread: The background thread running the port keep-alive
    """
    def _keep_alive():
        print(f"[Streap_raw]  Start port keep-alive thread for {listen_ip}:{listen_port}")
        attempt = 0
        while True:
            attempt += 1
            standard_sock = None

            try:
                print(f"[Streap_raw] register_keep_port_alive: Attempt #{attempt} - Create socket...")
                standard_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                standard_sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)

                print(f"[Streap_raw] register_keep_port_alive: Bind to {listen_ip}:{listen_port}...")
                standard_sock.bind((listen_ip, listen_port))
                
                print(f"[Streap_raw] register_keep_port_alive: Listen on port {listen_port}...")
                standard_sock.listen(0)  # No backlog, we won't accept connections
                print(f"[Streap_raw] register_keep_port_alive: Done port {listen_port} registration.")

                # Keep socket alive until an error occurs.
                while True:
                    time.sleep(3600)
            except Exception as e:
                print(f"[Streap_raw] register_keep_port_alive: Fail port registration with error {e}")
            finally:
                if standard_sock:
                    print(f"[Streap_raw] register_keep_port_alive: Close socket...")
                    standard_sock.close()

            print(f"[Streap_raw] register_keep_port_alive: Retry port registration in {retry_interval}s... (attempt #{attempt + 1})")
            time.sleep(retry_interval)
    
    print(f"[Streap_raw] register_keep_port_alive: Starting background thread for port {listen_port}")
    thread = threading.Thread(target=_keep_alive, daemon=True, name=f"port-keep-alive-{listen_port}")
    thread.start()
    print(f"[Streap_raw] register_keep_port_alive: Thread started (name={thread.name})")
    return thread


def checksum(msg):
    """Calculates a checksum and returns computed values.

    Args:
         msg message data
    """
    if len(msg) % 2 == 1:
        msg += b'\x00'
    s = 0
    for i in range(0, len(msg), 2):
        w = struct.unpack('!H', msg[i:i+2])[0]
        s += w
    s = (s >> 16) + (s & 0xffff)
    s += (s >> 16)
    return ~s & 0xffff


def build_tcp_packet(src_ip, dest_ip, src_port, dst_port, seq, ack_seq, syn=0, ack=0, fin=0, window=None):
    """Stitches a structurally flawless 20-byte TCP header with dynamic window advertisement.

    Args:
         src_ip:   Source IP address
         dest_ip:  Destination IP address
         src_port: Source port
         dst_port: Destination port
         seq:      Sequence number
         ack_seq:  ACK for sequence
         syn:      SYN flag
         ack:      ACK flag
         fin:      FIN flag
         window:   Advertised receive window
    """
    flags = (ack << 4) + (syn << 1) + fin
    offset_res = (5 << 4) + 0  # 20 bytes data offset (5 * 4)
    urg_ptr = 0
    draft_checksum = 0

    if window is None:
        window = RECEIVE_CAP
    window = max(0, min(65535, int(window)))

    # Build a draft (20 byte) header with no checksum
    tcp_header_temp = struct.pack('!HHLLBBHHH',
        src_port, dst_port, seq, ack_seq,
        offset_res, flags, window, draft_checksum, urg_ptr
    )
    
    # Build addressing-header
    address_header = struct.pack('!4s4sBBH',
        socket.inet_aton(src_ip), socket.inet_aton(dest_ip),
        0, socket.IPPROTO_TCP, len(tcp_header_temp)
    )
    
    # Calculate mathematical checksum integer
    tcp_checksum = checksum(address_header + tcp_header_temp)
    
    # Build the final TCP packet
    tcp_header = struct.pack('!HHLLBBHHH',
        src_port, dst_port, seq, ack_seq,
        offset_res, flags, window, tcp_checksum, urg_ptr
    )
    
    return tcp_header


def build_ip_header(src_ip, dest_ip, protocol, payload_len):
    """Builds a structurally flawless 20-byte IP header.

    Args:
         src_ip:      Source IP address
         dest_ip:     Destination IP address
         protocol:    Protocol number
         payload_len: Payload length
    """
    version_ihl = (4 << 4) | 5  # Version 4, IHL 5 (20 bytes)
    tos = 0
    total_len = 20 + payload_len  # IP header + payload

    identification = 0x1234
    flags_fragment = 0x4000
    flags_offset = 0 
    ttl = 64
    draft_checksum = 0
    
    # Build draft header with no checksum
    ip_header_temp = struct.pack('!BBHHHBBH4s4s',
        version_ihl, tos, total_len, identification,
        flags_offset, ttl, protocol, draft_checksum,
        socket.inet_aton(src_ip), socket.inet_aton(dest_ip)
    )
    
    # Calculate the checksum
    ip_checksum = checksum(ip_header_temp)
    
    # Build the final IP header
    ip_header = struct.pack('!BBHHHBBH4s4s',
        version_ihl, tos, total_len, identification,
        flags_offset, ttl, protocol, ip_checksum,
        socket.inet_aton(src_ip), socket.inet_aton(dest_ip)
    )
    
    return ip_header


def send_tcp_data_segment(s, src_ip, dest_ip, src_port, dst_port, seq, ack_seq, payload, window=None, psh=True):
    """Assembles and transmits a single TCP segment containing data payload.

    Args:
         s:        Raw socket
         src_ip:   Source IP address
         dest_ip:  Destination IP address
         src_port: Source port
         dst_port: Destination port
         seq:      Sequence number
         ack_seq:  ACK for sequence
         payload:  Payload bytes
         window:   Advertised receive window
         psh:      PSH flag (True for final segment or push data)
    """
    flags = 0x18 if psh else 0x10  # 0x18 = PSH+ACK, 0x10 = ACK
    offset_res = (5 << 4) + 0
    if window is None:
        window = RECEIVE_CAP
    window = max(0, min(65535, int(window)))
    urg_ptr = 0
    draft_checksum = 0
    
    # Build draft header with no checksum
    tcp_header_temp = struct.pack('!HHLLBBHHH',
        src_port, dst_port, seq, ack_seq,
        offset_res, flags, window, draft_checksum, urg_ptr
    )
    
    tcp_len = len(tcp_header_temp) + len(payload)
    pseudo = struct.pack('!4s4sBBH',
        socket.inet_aton(src_ip), socket.inet_aton(dest_ip),
        0, socket.IPPROTO_TCP, tcp_len
    )

    # Calculate the checksum    
    tcp_checksum = checksum(pseudo + tcp_header_temp + payload)

    # Build the full TCP packet header    
    tcp_header = struct.pack('!HHLLBBHHH',
        src_port, dst_port, seq, ack_seq,
        offset_res, flags, window, tcp_checksum, urg_ptr
    )

    # Build the full IP packet header     
    ip_header = build_ip_header(src_ip, dest_ip, socket.IPPROTO_TCP, len(tcp_header) + len(payload))

    # Build the full packet header     
    full_packet = ip_header + tcp_header + payload

    # Send packet
    print(f"[Streap_raw] send_tcp_data_segment: seq={seq}, ack_seq={ack_seq}, payload_len={len(payload)}, flags=0x{flags:02x}, window={window}")
    s.sendto(full_packet, (dest_ip, 0))
    
    return len(payload)


def clean_expired_connections(timeout=60):
    """Background thread to clean up stale or timed-out connections.

    Args:
         timeout: Time expiration for clean-up working
    """
    while True:
        time.sleep(10)
        now = time.time()
        with connections_lock:
            expired = [k for k, session in connections.items() if now - session.last_activity > timeout]
            for k in expired:
                print(f"[Streap_raw] clean_expired_connections: Session [{k[0]}:{k[1]}] Connection timed out ({timeout}s inactivity). Remove connection.")
                del connections[k]
                remove_packet_queue(k)
                _cleanup_connection_flow_control(k)


def create_raw_socket() -> socket.socket:
    """
    Create a raw socket for TCP protocol. The raw socket requires root privileges.
    It is raw with listen all packet for incoming port.
    
    Returns:
        Raw socket object
    """
    try:
        sock = socket.socket(socket.AF_INET, socket.SOCK_RAW, socket.IPPROTO_TCP)
        sock.setsockopt(socket.IPPROTO_IP, socket.IP_HDRINCL, 1)

        print(f"[Streap_raw] Create raw socket binding for 0.0.0.0 listening")
        return sock
    except PermissionError:
        print("Error: Root privileges required (sudo).")
        sys.exit(1)
    except OSError as e:
        print(f"Error creating raw socket: {e}")
        sys.exit(1)


def get_interface_ip(interface_name: str = None) -> str:
    """
    Get the IPv4 address of a network interface.
    
    Args:
        interface_name: Network interface name (e.g., 'enp0s8', 'eth0', 'en0')
                        If None, tries to find the default route interface.
    
    Returns:
        IPv4 address string
    """
    try:
        if interface_name:
            addrs = netifaces.ifaddresses(interface_name)
            if netifaces.AF_INET in addrs:
                return addrs[netifaces.AF_INET][0]['addr']
        else:
            # Try to get default route interface
            gateways = netifaces.gateways()
            default_gateway = gateways.get('default', {}).get(netifaces.AF_INET)
            if default_gateway:
                iface = default_gateway[1]
                addrs = netifaces.ifaddresses(iface)
                if netifaces.AF_INET in addrs:
                    return addrs[netifaces.AF_INET][0]['addr']
    except Exception:
        pass
    
    # Fallback: try to get any non-loopback IP
    try:
        s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        s.connect(("8.8.8.8", 80))
        ip = s.getsockname()[0]
        s.close()
        return ip
    except Exception:
        return "127.0.0.1"


def accept(s: socket.socket, listen_ip: str, listen_port: int, interface_name: str = None) -> conn_struct:
    """
    Accept a new connection by performing the 3-way handshake using raw sockets.
    Returns a conn_struct tuple for the established connection.
    Handshakes are serialized to ensure only one 3-way handshake is processed at a time.
    
    Args:
        s: Raw socket
        listen_ip: Server IP to listen on (e.g., '0.0.0.0')
        listen_port: Server port to listen on
        interface_name: Network interface name for sending packets (optional)
    
    Returns:
        conn_struct: Established connection information
    """
    # Start packet dispatcher for this port
    start_packet_dispatcher(s, listen_port)
    
    # Get actual source IP for sending packets
    if listen_ip == "0.0.0.0":
        send_ip = get_interface_ip(interface_name)
        print(f"[Streap_raw] accept: listen_ip=0.0.0.0, using send_ip={send_ip} for outgoing packets")
    else:
        send_ip = listen_ip
    
    server_seq = 2000
    client_ip = None
    client_port = None

    print(f"[Streap_raw] accept: Waiting for incoming connections on {listen_ip}:{listen_port} (send_ip={send_ip})...")
    try:
        while True:
            # Get packet from dispatcher queue (for SYN packets)
            syn_q = get_packet_queue(_SYN_KEY)
            try:
                packet, addr = syn_q.get(timeout=1.0)
                print(f"[Streap_raw] accept: Got packet from SYN queue")
            except queue.Empty:
                continue

            # Extract IP Header Length
            ihl = (packet[0] & 0x0F) * 4
            tcp_packet = packet[ihl:]
            
            if len(tcp_packet) < 20:
                continue

            # Unpack the received TCP header
            tcph = struct.unpack('!HHLLBBHHH', tcp_packet[:20])
            src_port, dest_port, pkt_seq, pkt_ack, flags = (
                tcph[0],
                tcph[1],
                tcph[2],
                tcph[3],
                tcph[5],
            )

            # Bypass non-listen port
            if dest_port != listen_port:
                continue

            syn_flag = (flags & 0x02) >> 1
            ack_flag = (flags & 0x10) >> 4

            # Build client_key from client IP and client port
            client_ip = socket.inet_ntoa(packet[12:16])
            client_port = src_port
            client_key = (client_ip, client_port)
            print(f"\n[Streap_raw] accept: Receive packet from Client ({client_ip}:{client_port}) (Seq={pkt_seq}, SYN={syn_flag}, ACK={ack_flag})")

            # Create queue for each connection associated 
            # with tuple client_key=(client_ip, client_port)
            q = get_packet_queue(client_key)
            print(f"[Streap_raw] accept: Created per-connection queue for {client_key}")
            with connections_lock:
                # Declare initialized session if not available
                if client_key not in connections:
                    connections[client_key] = conn_struct(
                        client_ip=client_ip,
                        client_port=client_port,
                        server_ip=send_ip,
                        server_port=listen_port,
                        state="LISTEN",
                        server_seq=2000,
                        client_seq=0,
                        expected_final_ack=0,
                        last_activity=time.time()
                    )
                session = connections[client_key]

            # STEP 1: Obtain client SYN 
            if syn_flag == 1 and ack_flag == 0:
                print(f"\nHandshake1: Received SYN from Client ({client_ip}:{client_port}) (Seq={pkt_seq})")

                client_win = tcph[6]
                _set_peer_rwnd(client_key, client_win)
                print(f"[Streap_raw] accept: Initial peer_rwnd from SYN: {_get_peer_rwnd(client_key)}")

                # Quick duplicate handshake check (fast, no heavy operations)
                with handshake_lock:
                    if handshake_in_progress.get(client_key, False):
                        print(f"[Streap_raw] accept: Handshake already in progress for {client_ip}:{client_port}, dropping SYN")
                        continue
                    handshake_in_progress[client_key] = True

                # STEP 2: Reply SYN-ACK immediately
                next_ack = pkt_seq + 1
                syn_ack_packet = build_tcp_packet(send_ip, client_ip, listen_port, client_port,
                                                  seq=server_seq, ack_seq=next_ack, syn=1, ack=1,
                                                  window=RECEIVE_CAP)
                ip_header = build_ip_header(send_ip, client_ip, socket.IPPROTO_TCP, len(syn_ack_packet))
                full_packet = ip_header + syn_ack_packet

                # Send SYN-ACK packet
                print(f"[Streap_raw] accept: Sending SYN-ACK to {client_ip}:{client_port} from {send_ip}:{listen_port}")
                print(f"[Streap_raw] accept: SYN-ACK packet size: {len(full_packet)} bytes (IP={len(ip_header)}, TCP={len(syn_ack_packet)})")
                bytes_sent = s.sendto(full_packet, (client_ip, 0))
                print(f"Handshake2: Sent SYN-ACK to Client (Seq={server_seq}, Ack={next_ack}, bytes_sent={bytes_sent})")

                # Register connection to the associated queue
                with connections_lock:
                    connections[client_key] = conn_struct(
                        client_ip=client_ip,
                        client_port=client_port,
                        server_ip=send_ip,  # Use actual sending IP
                        server_port=listen_port,
                        state="SYN_RCVD",
                        server_seq=server_seq,
                        client_seq=pkt_seq,
                        expected_final_ack=server_seq + 1,
                        last_activity=time.time()
                    )
                    session = connections[client_key]

                # STEP 3: Wait for client FIN-ACK
                deadline = time.time() + 10.0
                while time.time() < deadline:
                    try:
                        packet, addr = q.get(timeout=0.5)
                        print(f"[Streap_raw] accept: Got packet from per-connection queue for {client_ip}:{client_port}")
                    except queue.Empty:
                        print(f"[Streap_raw] accept: Per-connection queue empty for {client_ip}:{client_port}, waiting...")
                        continue

                    ihl = (packet[0] & 0x0F) * 4
                    tcp_packet = packet[ihl:]
                    
                    if len(tcp_packet) < 20:
                        continue

                    tcph = struct.unpack('!HHLLBBHHH', tcp_packet[:20])
                    src_port, dest_port, pkt_seq, pkt_ack, flags = (
                        tcph[0],
                        tcph[1],
                        tcph[2],
                        tcph[3],
                        tcph[5],
                    )

                    syn_flag = (flags & 0x02) >> 1
                    ack_flag = (flags & 0x10) >> 4
                    print(f"[Streap_raw] accept: Received packet in handshake wait - SYN={syn_flag}, ACK={ack_flag}, Seq={pkt_seq}, Ack={pkt_ack}")

                    # Check if this is the final ACK (ACK only, no SYN)
                    if syn_flag == 0 and ack_flag == 1:
                        print(f"[Streap_raw] accept: handshake3 Received FIN-ACK from Client (Seq={pkt_seq}, Ack={pkt_ack})")
                        _set_peer_rwnd(client_key, tcph[6])

                        with connections_lock:
                            if session.state != "SYN_RCVD":
                                print(f"[Streap_raw] accept: Non handshake state for {client_ip}:{client_port}")
                                break

                            # Update connection to ESTABLISHED
                            connections[client_key] = conn_struct(
                                client_ip=client_ip,
                                client_port=client_port,
                                server_ip=send_ip,
                                server_port=listen_port,
                                state="ESTABLISHED",
                                server_seq=pkt_ack,
                                client_seq=pkt_seq,
                                expected_final_ack=session.expected_final_ack,
                                last_activity=time.time()
                            )
                            session = connections[client_key]

                        # Clear handshake in progress flag
                        with handshake_lock:
                            handshake_in_progress[client_key] = False

                        print(f"[Streap_raw] Etablished handshak with connection: ({client_ip}:{client_port}) ESTABLISHED")
                        return session
                
                # Fallback of the handshake timeout
                print(f"[Streap_raw] Handshake timeout for {client_ip}:{client_port}")
                with handshake_lock:
                    handshake_in_progress[client_key] = False
                remove_packet_queue(client_key)
                with connections_lock:
                    connections.pop(client_key, None)
                _cleanup_connection_flow_control(client_key)
                continue

            # STEP 3 Exception handling: Capture FIN ACK on SYN queue
            if session.state == "SYN_RCVD" and syn_flag == 0 and ack_flag == 1:
                print(f"[Streap_raw] accept: handshake3 received Final ACK from Client on SYN queue (Seq={pkt_seq}, Ack={pkt_ack})")
                _set_peer_rwnd(client_key, tcph[6])

                with connections_lock:
                    if session.state != "SYN_RCVD":
                        print(f"[Streap_raw] accept: Non handshake state for {client_ip}:{client_port}")
                        continue

                    connections[client_key] = conn_struct(
                        client_ip=client_ip,
                        client_port=client_port,
                        server_ip=send_ip,
                        server_port=listen_port,
                        state="ESTABLISHED",
                        server_seq=pkt_ack,
                        client_seq=pkt_seq,
                        expected_final_ack=session.expected_final_ack,
                        last_activity=time.time()
                    )
                    session = connections[client_key]

                with handshake_lock:
                    handshake_in_progress[client_key] = False

                print(f"[Streap_raw] Etablished handshak with connection: ({client_ip}:{client_port}) ESTABLISHED")
                return session

    except KeyboardInterrupt:
        print("\n[-] Server stopped.")
        with handshake_lock:
            handshake_in_progress.clear()
        raise


def run_conn_recv(s: socket.socket, conn_info: conn_struct) -> bytes:
    """
    Receive data on an established connection using raw sockets.
    This function handles incoming packets for a specific connection by reading
    from the packet dispatcher's per-connection queue.
    
    Args:
        s: Raw socket
        conn_info: conn_struct tuple for the connection
    
    Returns:
        bytes: The received data payload, or empty bytes if connection closed
    """
    client_ip = conn_info.client_ip
    client_port = conn_info.client_port
    client_key = (client_ip, client_port)
    listen_ip = conn_info.server_ip
    listen_port = conn_info.server_port
    
    if _load_connection_record(connection_key=client_key) is None:
        return None
    
    packet_queue = get_packet_queue(client_key)
    idle_deadline = time.time() + RECEIVER_IDLE_TIMEOUT
    
    print(f"[Streap_raw] run_conn_recv: Starting receive loop for {client_ip}:{client_port} (waiting for data...)")
    
    try:
        while True:
            try:
                packet, addr = packet_queue.get(timeout=1.0)
            except queue.Empty:
                if time.time() > idle_deadline:
                    print("idle time out")
                    _forget_connection(client_key)
                    return None
                continue

            ihl = (packet[0] & 0x0F) * 4
            tcp_packet = packet[ihl:]
            
            if len(tcp_packet) < 20:
                continue

            tcph = struct.unpack('!HHLLBBHHH', tcp_packet[:20])
            src_port, dest_port, pkt_seq, pkt_ack, flags = (
                tcph[0],
                tcph[1],
                tcph[2],
                tcph[3],
                tcph[5],
            )

            if dest_port != listen_port or src_port != client_port:
                continue
            
            syn_flag = (flags & 0x02) >> 1
            ack_flag = (flags & 0x10) >> 4
            psh_flag = (flags & 0x08) >> 3
            fin_flag = (flags & 0x01)
            rst_flag = (flags & 0x04) >> 2

            print(f"[Streap_raw] run_conn_recv: Received packet from {client_ip}:{client_port} (SYN={syn_flag}, ACK={ack_flag}, PSH={psh_flag}, FIN={fin_flag}, RST={rst_flag}, Seq={pkt_seq}, Ack={pkt_ack})")
            with connections_lock:
                if client_key not in connections:
                    continue
                session = connections[client_key]

            # Handle FIN (connection close)
            if fin_flag:
                if pkt_seq != session.client_seq:
                    _send_pure_ack(s, session, session.client_seq)
                    continue

                print(f"[Streap_raw] Received FIN from {client_ip}:{client_port}")
                adv_rwnd = _free_receive_window(client_key, pkt_seq + 1)
                ack_packet = build_tcp_packet(listen_ip, client_ip, listen_port, client_port,
                                              seq=session.server_seq, ack_seq=pkt_seq + 1, ack=1,
                                              window=adv_rwnd)
                ip_header = build_ip_header(listen_ip, client_ip, socket.IPPROTO_TCP, len(ack_packet))
                s.sendto(ip_header + ack_packet, (client_ip, 0))
                
                with connections_lock:
                    if client_key in connections:
                        del connections[client_key]
                _cleanup_connection_flow_control(client_key)
                remove_packet_queue(client_key)
                return b""

            # Handle RST (connection reset)
            if rst_flag:
                print(f"[Streap_raw] Received RST from {client_ip}:{client_port}")
                with connections_lock:
                    if client_key in connections:
                        del connections[client_key]
                _cleanup_connection_flow_control(client_key)
                remove_packet_queue(client_key)
                return b""

            # Update last activity
            with connections_lock:
                if client_key in connections:
                    connections[client_key] = session._replace(last_activity=time.time())

            # Handle data payload
            has_data = len(tcp_packet) > ((tcp_packet[12] >> 4) & 0x0F) * 4
            
            if (psh_flag and ack_flag) or (ack_flag and has_data and not syn_flag and not fin_flag and not rst_flag):
                tcp_header_len = ((tcp_packet[12] >> 4) & 0x0F) * 4
                app_payload = tcp_packet[tcp_header_len:]
                
                if len(app_payload) > 0:
                    print(f"[Streap_raw] Received {len(app_payload)} bytes of data from {client_ip}:{client_port}")
                    print(f"[Streap_raw] Received payload: {app_payload[:100]}...")

                    # Flow control: update peer_rwnd if pkt_ack >= SND.UNA
                    peer_window = tcph[6]
                    if pkt_ack >= session.server_seq:
                        _set_peer_rwnd(client_key, peer_window)

                    expected_seq = session.client_seq
                    bytes_ahead = (pkt_seq - expected_seq)
                    current_rwnd = _free_receive_window(client_key)

                    # Flow control rule: Reject data arriving beyond right edge [RCV.NXT, RCV.NXT + RCV.WND)
                    if bytes_ahead > 0 and bytes_ahead + len(app_payload) > current_rwnd:
                        print(f"[Streap_raw] Dropping out-of-window data from {client_ip}:{client_port} (seq={pkt_seq}, ahead={bytes_ahead}, len={len(app_payload)}, rwnd={current_rwnd})")
                        _send_pure_ack(s, session, expected_seq)
                        idle_deadline = time.time() + RECEIVER_IDLE_TIMEOUT
                        continue

                    if bytes_ahead != 0:
                        # Out of order segment within window: buffer it
                        with _early_segments_lock:
                            buf = _early_segments.setdefault(client_key, {})
                            if bytes_ahead > 0 and bytes_ahead < current_rwnd:
                                buf[pkt_seq] = app_payload
                        _send_pure_ack(s, session, expected_seq)
                        idle_deadline = time.time() + RECEIVER_IDLE_TIMEOUT
                        continue

                    # In-order segment received
                    next_ack = pkt_seq + len(app_payload)

                    with _early_segments_lock:
                        buf = _early_segments.setdefault(client_key, {})
                        while next_ack in buf:
                            buffered_payload = buf.pop(next_ack)
                            app_payload += buffered_payload
                            next_ack += len(buffered_payload)

                    adv_rwnd = _free_receive_window(client_key, next_ack)
                    ack_packet = build_tcp_packet(session.server_ip, client_ip, listen_port, client_port,
                                                  seq=session.server_seq, ack_seq=next_ack, ack=1,
                                                  window=adv_rwnd)
                    ip_header = build_ip_header(session.server_ip, client_ip, socket.IPPROTO_TCP, len(ack_packet))
                    s.sendto(ip_header + ack_packet, (client_ip, 0))
                    print(f"[Streap_raw] Sent ACK for data (Ack={next_ack}, rwnd={adv_rwnd})")
                    
                    with connections_lock:
                        if client_key in connections:
                            connections[client_key] = session._replace(
                                server_seq=session.server_seq,
                                client_seq=next_ack,
                                last_activity=time.time()
                            )
                        
                    return app_payload

            # Handle pure ACK (no data)
            elif ack_flag and not syn_flag and not psh_flag and not has_data:
                print(f"[Streap_raw] Received ACK from {client_ip}:{client_port} (Ack={pkt_ack})")
                peer_window = tcph[6]
                if pkt_ack >= session.server_seq:
                    _set_peer_rwnd(client_key, peer_window)
                with connections_lock:
                    if client_key in connections:
                        connections[client_key] = session._replace(
                            server_seq=pkt_ack,
                            last_activity=time.time()
                        )

    except KeyboardInterrupt:
        print("\n[Streap_raw] Receive loop interrupted.")
        return b""
    except Exception as e:
        print(f"[Streap_raw] Error in run_conn_recv: {e}")
        return b""


def run_conn_send(s: socket.socket, conn_info: conn_struct, payload: bytes) -> int:
    """
    Args:
        s: Raw socket
        conn_info: conn_struct tuple for the connection
        payload: Bytes payload to send
    
    Returns:
        int: Number of bytes sent, or -1 on error
    """
    client_ip = conn_info.client_ip
    client_port = conn_info.client_port
    client_key = (client_ip, client_port)
    listen_ip = conn_info.server_ip
    listen_port = conn_info.server_port
    
    with connections_lock:
        if client_key not in connections:
            print(f"[Streap_raw] run_conn_send Connection not found for {client_ip}:{client_port}")
            return -1
        session = connections[client_key]
    
    if not payload:
        return 0

    try:
        packet_queue = get_packet_queue(client_key)
        total_bytes = len(payload)
        base_seq = session.server_seq
        bytes_dispatched = 0

        # seq -> (segment, send_time, retries)
        outstanding: Dict[int, Tuple[bytes, float, int]] = {}
        deferred_packets: List[Tuple[bytes, Tuple[str, int]]] = []

        # Persist timer for Zero-Window Probing (ZWP) (RFC 1122 / RFC 9293)
        zwp_last_sent = 0.0
        zwp_interval = FLOW_CONTROL_ZWP_INTERVAL

        def transmit(seq: int, segment: bytes, retries: int, psh: bool = True) -> None:
            adv_w = _free_receive_window(client_key)
            sent = send_tcp_data_segment(
                s, listen_ip, client_ip, listen_port, client_port,
                seq=seq, ack_seq=session.client_seq, payload=segment,
                window=adv_w, psh=psh
            )
            if sent != len(segment):
                raise OSError(f"short send: expected {len(segment)} bytes, sent {sent}")
            outstanding[seq] = (segment, time.time(), retries)

        while bytes_dispatched < total_bytes or outstanding:
            peer_rwnd = _get_peer_rwnd(client_key)
            in_flight_bytes = sum(len(state[0]) for state in outstanding.values())
            usable_win = max(0, peer_rwnd - in_flight_bytes)

            # 1. Dispatch segments within available flow control window
            while (
                bytes_dispatched < total_bytes
                and len(outstanding) < SEND_WINDOW
                and usable_win > 0
            ):
                remaining = total_bytes - bytes_dispatched

                # Sender Silly Window Syndrome Avoidance 
                # Send data if:
                # (a) A full segment can be sent: usable_win >= MSS and remaining >= MSS
                # (b) usable_win >= floor(max_peer_rwnd / 2)
                # (c) in_flight_bytes == 0 (no unacknowledged data in flight)
                max_rwnd = _get_max_peer_rwnd(client_key)
                can_send = (
                    (usable_win >= MESSAGE_SIZE and remaining >= MESSAGE_SIZE)
                    or (usable_win >= (max_rwnd // 2))
                    or (in_flight_bytes == 0)
                )
                if not can_send:
                    break

                chunk_len = min(MESSAGE_SIZE, remaining, usable_win)
                if chunk_len <= 0:
                    break

                segment = payload[bytes_dispatched : bytes_dispatched + chunk_len]
                seq = (base_seq + bytes_dispatched) % _SEQ_MOD
                is_last = (bytes_dispatched + chunk_len == total_bytes)
                transmit(seq, segment, 0, psh=is_last)

                bytes_dispatched += chunk_len
                in_flight_bytes += chunk_len
                usable_win = max(0, peer_rwnd - in_flight_bytes)

            # 2. Zero-Window Persist Timer & Zero-Window Probe (ZWP) (RFC 1122 / RFC 9293)
            # When receiver window is zero and no data is in flight, probe with 1 byte
            now = time.time()
            if bytes_dispatched < total_bytes and peer_rwnd == 0 and not outstanding:
                if now - zwp_last_sent >= zwp_interval:
                    zwp_byte = payload[bytes_dispatched : bytes_dispatched + 1]
                    seq = (base_seq + bytes_dispatched) % _SEQ_MOD
                    adv_w = _free_receive_window(client_key)
                    send_tcp_data_segment(
                        s, listen_ip, client_ip, listen_port, client_port,
                        seq=seq, ack_seq=session.client_seq, payload=zwp_byte,
                        window=adv_w, psh=False
                    )
                    print(f"[Streap_raw] Sent Zero-Window Probe (1 byte) seq={seq} to {client_ip}:{client_port} (interval={zwp_interval}s)")
                    zwp_last_sent = now
                    zwp_interval = min(FLOW_CONTROL_MAX_ZWP_INTERVAL, zwp_interval * 2)

            # 3. Retransmit timed out segments
            now = time.time()
            for seq, state in list(outstanding.items()):
                if now - state[1] < RETRANSMISSION_TIMEOUT:
                    continue
                # Do not count zero-window pause as fatal retries
                if state[2] >= MAX_RETRIES:
                    if peer_rwnd > 0:
                        print(f"[Streap_raw] run_conn_send Segment at seq={seq} gave up to {client_ip}:{client_port}")
                        return -1
                    else:
                        retries = max(state[2], 1)
                else:
                    retries = state[2] + 1

                is_last = ((seq + len(state[0]) - base_seq) % _SEQ_MOD == total_bytes)
                transmit(seq, state[0], retries, psh=is_last)

            # 4. Wait for incoming ACK packets
            now = time.time()
            if outstanding:
                wait_timeout = max(0.001, min(RETRANSMISSION_TIMEOUT - (now - state[1]) for state in outstanding.values()))
            elif bytes_dispatched < total_bytes and peer_rwnd == 0:
                wait_timeout = max(0.001, zwp_interval - (now - zwp_last_sent))
            else:
                wait_timeout = 0.5
            wait_timeout = min(0.5, max(0.001, wait_timeout))

            try:
                packet, addr = packet_queue.get(timeout=wait_timeout)
            except queue.Empty:
                continue

            ihl = (packet[0] & 0x0F) * 4
            tcp_packet = packet[ihl:]
            if len(tcp_packet) < 20:
                continue

            tcph = struct.unpack('!HHLLBBHHH', tcp_packet[:20])
            src_port, dest_port, pkt_seq, pkt_ack, flags = tcph[0], tcph[1], tcph[2], tcph[3], tcph[5]
            peer_window = tcph[6]

            if src_port != client_port or dest_port != listen_port:
                deferred_packets.append((packet, addr))
                continue

            if flags & 0x04:  # RST
                print(f"[Streap_raw] run_conn_send Connection reset by peer {client_ip}:{client_port}")
                deferred_packets.append((packet, addr))
                return -1

            if not (flags & 0x10):  # Must have ACK flag
                continue

            # Update peer receive window if valid ACK (pkt_ack >= SND.UNA)
            if outstanding:
                snd_una = min(outstanding.keys())
            else:
                snd_una = (base_seq + bytes_dispatched) % _SEQ_MOD

            if pkt_ack >= snd_una:
                _set_peer_rwnd(client_key, peer_window)
                if peer_window > 0:
                    zwp_interval = FLOW_CONTROL_ZWP_INTERVAL

            # Acknowledge segments
            for seq in list(outstanding.keys()):
                seg_end = (seq + len(outstanding[seq][0])) % _SEQ_MOD
                if seg_end <= pkt_ack:
                    del outstanding[seq]

            # Advance bytes_dispatched if probe byte or higher was acknowledged
            if pkt_ack > base_seq:
                acked_offset = pkt_ack - base_seq
                if acked_offset > bytes_dispatched:
                    bytes_dispatched = min(total_bytes, acked_offset)

            # Update last activity
            with connections_lock:
                if client_key in connections:
                    connections[client_key] = session._replace(last_activity=time.time())

            tcp_header_len = ((tcp_packet[12] >> 4) & 0x0F) * 4
            has_payload = len(tcp_packet) > tcp_header_len
            is_pure_ack = bool(flags & 0x10) and not (flags & 0x03) and not has_payload
            if not is_pure_ack:
                deferred_packets.append((packet, addr))

        bytes_sent = total_bytes
        print(f"[Streap_raw] run_conn_send Sent {bytes_sent} bytes to {client_ip}:{client_port}")

        # Update sequence numbers
        with connections_lock:
            if client_key in connections:
                connections[client_key] = session._replace(
                    server_seq=(base_seq + bytes_sent) % _SEQ_MOD,
                    last_activity=time.time()
                )

        return bytes_sent

    except Exception as e:
        print(f"[Streap_raw] run_conn_send Error in run_conn_send: {e}")
        return -1
    finally:
        for packet, addr in deferred_packets:
            try:
                packet_queue.put_nowait((packet, addr))
            except queue.Full:
                pass


def close_connection(s: socket.socket, conn_info: conn_struct):
    """Close a specific connection by sending FIN and removing from connection table."""
    client_ip = conn_info.client_ip
    client_port = conn_info.client_port
    client_key = (client_ip, client_port)
    listen_ip = conn_info.server_ip
    listen_port = conn_info.server_port
    session = None
    
    with connections_lock:
        if client_key in connections:
            session = connections[client_key]
            adv_rwnd = _free_receive_window(client_key)
            # Send FIN packet with advertised window
            fin_packet = build_tcp_packet(listen_ip, client_ip, listen_port, client_port,
                                          seq=session.server_seq, ack_seq=session.client_seq, ack=1, fin=1,
                                          window=adv_rwnd)
            ip_header = build_ip_header(listen_ip, client_ip, socket.IPPROTO_TCP, len(fin_packet))
            s.sendto(ip_header + fin_packet, (client_ip, 0))
            print(f"[Streap_raw] run_conn_send: Send FIN to {client_ip}:{client_port}")
            del connections[client_key]

    if session is not None:
        packet_queue = get_packet_queue(client_key=client_key)
        fin_acked = False
        for attempt in range(0, MAX_RETRIES):
            deadline = time.time() + RETRANSMISSION_TIMEOUT
            while time.time() < deadline and not fin_acked:
                try: 
                    packet, addr = packet_queue.get(timeout=0.1)
                except queue.Empty:
                    continue
                ihl = (packet[0] & 0x0F) * 4
                tcp_packet = packet[ihl:]
                if len(tcp_packet) < 20:
                    continue
                tcph = struct.unpack('!HHLLBBHHH', tcp_packet[:20])
                pkt_ack, flags = tcph[3], tcph[5]
                if (flags & 0x04) or ((flags & 0x10) and pkt_ack == session.server_seq + 1):
                    fin_acked = True
                if fin_acked:
                    break
            if fin_acked:
                break
            adv_rwnd = _free_receive_window(client_key)
            fin_packet = build_tcp_packet(listen_ip, client_ip, listen_port, client_port,
                                          seq=session.server_seq, ack_seq=session.client_seq, ack=1, fin=1,
                                          window=adv_rwnd)
            ip_header = build_ip_header(listen_ip, client_ip, socket.IPPROTO_TCP, len(fin_packet))
            s.sendto(ip_header + fin_packet, (client_ip, 0))
            print(f"[Streap_raw] run_conn_send: Resend FIN to {client_ip}:{client_port}")
        if not fin_acked:
            print(f"[Streap_raw] run_conn_send: FIN not acked by {client_ip}:{client_port}")

    # Clean up packet queue and flow control state
    with _early_segments_lock:
        _early_segments.pop(client_key, None)
    _cleanup_connection_flow_control(client_key)
    remove_packet_queue(client_key)


def close_all_connections():
    """Close all connections and clean up."""
    with connections_lock:
        connections.clear()
    with _early_segments_lock:
        _early_segments.clear()
    with _peer_rwnd_lock:
        _peer_rwnd.clear()
        _max_peer_rwnd.clear()
    with _rcv_wnd_edge_lock:
        _rcv_wnd_edge.clear()
