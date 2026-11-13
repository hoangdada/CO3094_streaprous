
import unittest
from unittest.mock import patch, AsyncMock

from daemon import backend


class TestBackend(unittest.IsolatedAsyncioTestCase):

    @patch("daemon.backend.HttpAdapter")
    async def test_coroutine_routes(self, mock_adapter):
        routes = {
            ("GET", "/get-list/"): lambda: {}
        }

        reader = object()
        writer = unittest.mock.MagicMock()
        writer.get_extra_info.return_value = ("127.0.0.1", 5000)

        mock_adapter.return_value.handle_client_coroutine = AsyncMock()

        await backend.handle_client_coroutine(
            reader, writer, routes
        )

        mock_adapter.assert_called_once_with(
            None, None, None,
            ("127.0.0.1", 5000),
            routes
        )

        mock_adapter.return_value.handle_client_coroutine.assert_awaited_once_with(
            reader, writer
        )


if __name__ == "__main__":
    unittest.main()
