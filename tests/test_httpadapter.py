
import unittest
import json
from unittest.mock import MagicMock

from daemon.httpadapter import HttpAdapter


class TestHttpAdapter(unittest.TestCase):

    def setUp(self):
        self.adapter = HttpAdapter(
            "127.0.0.1", 8001, None, None, {}
        )

    def test_json_response(self):
        response = self.adapter.make_json_response(
            {"status": "ok"}, 200
        )

        header, body = response.split(b"\r\n\r\n", 1)

        self.assertIn(b"HTTP/1.1 200 OK", header)
        self.assertIn(b"Content-Type: application/json", header)
        self.assertIn(
            f"Content-Length: {len(body)}".encode(),
            header
        )

        self.assertEqual(json.loads(body), {"status": "ok"})

    def test_tracker_response_wrapper(self):
        result = {
            "_http_response": True,
            "status_code": 401,
            "data": {"status": "error", "message": "Unauthorized"},
            "set_cookie": "session=abc"
        }

        response = self.adapter.format_route_result(result)

        self.assertIn(b"HTTP/1.1 401 Unauthorized", response)
        self.assertIn(b"Set-Cookie: session=abc", response)

    def test_invalid_json(self):
        req = MagicMock()
        req.headers = {}
        req.body = "invalid"
        req.hook = lambda headers, body: json.loads(body)

        response = self.adapter.handle_route(req)

        self.assertIn(b"HTTP/1.1 400 Bad Request", response)


if __name__ == "__main__":
    unittest.main()
