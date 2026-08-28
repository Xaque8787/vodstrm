import asyncio
import unittest
from datetime import timedelta
from unittest.mock import Mock, patch

from fastapi import Response

from app.routes.auth import login_submit


class LoginTests(unittest.TestCase):
    user = {
        "id": 1,
        "username": "admin",
        "password_hash": "unused",
        "is_admin": 1,
    }

    def _login(self, remember):
        with (
            patch("app.routes.auth._get_user_by_username", return_value=self.user),
            patch("app.routes.auth._hash_password", return_value="unused"),
            patch("app.routes.auth.create_access_token", return_value="token") as create,
        ):
            result = asyncio.run(
                login_submit(
                    request=Mock(),
                    response=Response(),
                    username="admin",
                    password="password",
                    remember=remember,
                )
            )
        return result, create

    def test_login_without_remember_me_uses_session_cookie(self):
        response, create = self._login(False)

        cookie = response.headers["set-cookie"]
        self.assertNotIn("Max-Age", cookie)
        self.assertNotIn("expires=", cookie.lower())
        self.assertEqual(
            create.call_args.kwargs["expires_delta"],
            timedelta(minutes=60),
        )

    def test_login_with_remember_me_uses_persistent_cookie(self):
        response, create = self._login(True)

        cookie = response.headers["set-cookie"]
        self.assertIn("Max-Age=2592000", cookie)
        self.assertEqual(
            create.call_args.kwargs["expires_delta"],
            timedelta(days=30),
        )


if __name__ == "__main__":
    unittest.main()