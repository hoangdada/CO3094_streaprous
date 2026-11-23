import http.client
import json


TRACKER_IP = "127.0.0.1"
TRACKER_PORT = 8000


def set_tracker(ip, port):
    global TRACKER_IP, TRACKER_PORT
    TRACKER_IP = ip
    TRACKER_PORT = int(port)


def send_request(method, path, data=None, cookie=None):
    conn = http.client.HTTPConnection(TRACKER_IP, TRACKER_PORT, timeout=5)

    try:
        headers = {"Content-Type": "application/json"}

        if cookie:
            headers["Cookie"] = cookie

        body = json.dumps(data) if data is not None else None
        conn.request(method, path, body=body, headers=headers)

        response = conn.getresponse()
        status = response.status
        set_cookie = response.getheader("Set-Cookie")
        content = response.read().decode("utf-8")

        result = json.loads(content) if content else {}

        return {
            "status_code": status,
            "data": result,
            "set_cookie": set_cookie
        }

    except (OSError, http.client.HTTPException, ValueError) as e:
        print("[TrackerClient] Error:", e)

        return {
            "status_code": 502,
            "data": {
                "status": "error",
                "message": "Cannot communicate with Tracker"
            },
            "set_cookie": None
        }

    finally:
        conn.close()


def login(username, password):
    return send_request("POST", "/login", {
        "username": username,
        "password": password
    })


def submit_info(port, cookie=None):
    return send_request("POST", "/submit-info", {
        "port": int(port)
    }, cookie=cookie)


def get_peer_list(cookie=None):
    return send_request("GET", "/peer-list", cookie=cookie)


def logout(cookie=None):
    return send_request("POST", "/logout", cookie=cookie)
