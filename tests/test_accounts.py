"""
Tests for the admin accounts API — verify endpoint, CRUD with
verify_credentials, delete consistency, and verify-update.

These are integration tests that hit the real API on localhost:9000.
Requires: API container running, trader_main running, DB accessible.
"""

from __future__ import annotations

import httpx
import pytest

BASE = "http://localhost:9000/api/v1/admin/accounts"
TIMEOUT = httpx.Timeout(90.0)  # verify can take up to 60s


@pytest.fixture
def client():
    return httpx.AsyncClient(timeout=TIMEOUT)


# ---------------------------------------------------------------
# POST /verify — success & failure
# ---------------------------------------------------------------

class TestVerify:
    """Credential verification without creating an account."""

    @pytest.mark.asyncio
    async def test_verify_valid_credentials(self, client: httpx.AsyncClient):
        """Known good account should return ok=true with account info."""
        resp = await client.post(f"{BASE}/verify", json={
            "mt5_login": 5052841,
            "mt5_password": "!p27ed4U",
            "mt5_server": "OneRoyal-Server",
        })
        assert resp.status_code == 200
        data = resp.json()
        assert data["ok"] is True
        assert data["account_name"]  # non-empty
        assert data["leverage"] > 0
        assert data["balance"] >= 0
        assert data["message"] == "MT5 login successful"

    @pytest.mark.asyncio
    async def test_verify_invalid_password(self, client: httpx.AsyncClient):
        """Wrong password should return 400."""
        resp = await client.post(f"{BASE}/verify", json={
            "mt5_login": 5052841,
            "mt5_password": "WRONG_PASSWORD",
            "mt5_server": "OneRoyal-Server",
        })
        assert resp.status_code == 400
        assert "Invalid MT5 credentials" in resp.json()["detail"]

    @pytest.mark.asyncio
    async def test_verify_invalid_login(self, client: httpx.AsyncClient):
        """Non-existent login should return 400."""
        resp = await client.post(f"{BASE}/verify", json={
            "mt5_login": 9999999,
            "mt5_password": "anything",
            "mt5_server": "OneRoyal-Server",
        })
        assert resp.status_code == 400

    @pytest.mark.asyncio
    async def test_verify_no_account_created(self, client: httpx.AsyncClient):
        """Verify should NOT create any account record in the DB."""
        # Get current account count
        before = await client.get(BASE)
        count_before = len(before.json())

        # Run verify (valid creds)
        await client.post(f"{BASE}/verify", json={
            "mt5_login": 5052841,
            "mt5_password": "!p27ed4U",
            "mt5_server": "OneRoyal-Server",
        })

        # Account count should be unchanged
        after = await client.get(BASE)
        assert len(after.json()) == count_before

    @pytest.mark.asyncio
    async def test_verify_with_account_id_context(self, client: httpx.AsyncClient):
        """Passing account_id is accepted (context only, no side effects)."""
        resp = await client.post(f"{BASE}/verify", json={
            "mt5_login": 5052841,
            "mt5_password": "!p27ed4U",
            "mt5_server": "OneRoyal-Server",
            "account_id": 1,
        })
        assert resp.status_code == 200
        assert resp.json()["ok"] is True


# ---------------------------------------------------------------
# PATCH with verify_credentials — password required
# ---------------------------------------------------------------

class TestPatchVerify:
    """PATCH with verify_credentials flag."""

    @pytest.mark.asyncio
    async def test_patch_change_login_without_password_rejected(
        self, client: httpx.AsyncClient,
    ):
        """Changing mt5_login with verify_credentials=true but no password → 400."""
        # Use account_id=1 (known to exist)
        resp = await client.patch(f"{BASE}/1", json={
            "mt5_login": 9999999,
            "verify_credentials": True,
        })
        assert resp.status_code == 400
        assert "mt5_password is required" in resp.json()["detail"]

    @pytest.mark.asyncio
    async def test_patch_change_server_without_password_rejected(
        self, client: httpx.AsyncClient,
    ):
        """Changing mt5_server with verify_credentials=true but no password → 400."""
        resp = await client.patch(f"{BASE}/1", json={
            "mt5_server": "SomeOtherServer",
            "verify_credentials": True,
        })
        assert resp.status_code == 400
        assert "mt5_password is required" in resp.json()["detail"]


# ---------------------------------------------------------------
# PATCH — password-only & unchanged login/label
# ---------------------------------------------------------------

class TestPatchUniqueness:
    """PATCH uniqueness check must exclude the account being updated."""

    @pytest.mark.asyncio
    async def test_password_only_update_succeeds(
        self, client: httpx.AsyncClient,
    ):
        """Changing only mt5_password should never trigger a duplicate error."""
        # Create a temp account
        create_resp = await client.post(BASE, json={
            "label": "PwdOnlyTest",
            "mt5_login": 22222222,
            "mt5_password": "old_pass",
            "mt5_server": "TestServer",
        })
        assert create_resp.status_code == 201
        acct_id = create_resp.json()["id"]

        try:
            # PATCH only the password
            patch_resp = await client.patch(f"{BASE}/{acct_id}", json={
                "mt5_password": "new_pass_123",
            })
            assert patch_resp.status_code == 200
            assert patch_resp.json()["mt5_login"] == 22222222
        finally:
            await client.delete(f"{BASE}/{acct_id}")

    @pytest.mark.asyncio
    async def test_unchanged_login_label_on_patch(
        self, client: httpx.AsyncClient,
    ):
        """Sending the same login/label values should not raise a conflict."""
        create_resp = await client.post(BASE, json={
            "label": "SameFieldsTest",
            "mt5_login": 33333333,
            "mt5_password": "test123",
            "mt5_server": "TestServer",
        })
        assert create_resp.status_code == 201
        acct_id = create_resp.json()["id"]

        try:
            # PATCH with the same login + label + new password
            patch_resp = await client.patch(f"{BASE}/{acct_id}", json={
                "label": "SameFieldsTest",
                "mt5_login": 33333333,
                "mt5_password": "new_pass",
            })
            assert patch_resp.status_code == 200
        finally:
            await client.delete(f"{BASE}/{acct_id}")

    @pytest.mark.asyncio
    async def test_duplicate_login_on_patch_returns_409(
        self, client: httpx.AsyncClient,
    ):
        """Changing login to one that belongs to another account → 409."""
        # Create two accounts
        resp_a = await client.post(BASE, json={
            "label": "DupTestA",
            "mt5_login": 44444444,
            "mt5_password": "test",
            "mt5_server": "TestServer",
        })
        resp_b = await client.post(BASE, json={
            "label": "DupTestB",
            "mt5_login": 55555555,
            "mt5_password": "test",
            "mt5_server": "TestServer",
        })
        assert resp_a.status_code == 201
        assert resp_b.status_code == 201
        id_a = resp_a.json()["id"]
        id_b = resp_b.json()["id"]

        try:
            # Try to set B's login to A's login
            patch_resp = await client.patch(f"{BASE}/{id_b}", json={
                "mt5_login": 44444444,
            })
            assert patch_resp.status_code == 409
            assert "already exists" in patch_resp.json()["detail"]
        finally:
            await client.delete(f"{BASE}/{id_a}")
            await client.delete(f"{BASE}/{id_b}")


# ---------------------------------------------------------------
# POST /{account_id}/verify-update
# ---------------------------------------------------------------

class TestVerifyUpdate:
    """Verify merged credentials for an existing account without saving."""

    @pytest.mark.asyncio
    async def test_verify_update_password_change_valid(
        self, client: httpx.AsyncClient,
    ):
        """Verify-update with correct new password returns ok=true."""
        # Account 1 is the known good account (5052841)
        resp = await client.post(f"{BASE}/1/verify-update", json={
            "mt5_password": "!p27ed4U",  # same valid password
        })
        assert resp.status_code == 200
        assert resp.json()["ok"] is True

    @pytest.mark.asyncio
    async def test_verify_update_password_change_invalid(
        self, client: httpx.AsyncClient,
    ):
        """Verify-update with wrong new password returns 400."""
        resp = await client.post(f"{BASE}/1/verify-update", json={
            "mt5_password": "WRONG_PASSWORD",
        })
        assert resp.status_code == 400

    @pytest.mark.asyncio
    async def test_verify_update_nonexistent_account(
        self, client: httpx.AsyncClient,
    ):
        """Verify-update on non-existent account → 404."""
        resp = await client.post(f"{BASE}/99999/verify-update", json={
            "mt5_password": "anything",
        })
        assert resp.status_code == 404

    @pytest.mark.asyncio
    async def test_verify_update_does_not_save(
        self, client: httpx.AsyncClient,
    ):
        """Verify-update must NOT change the account record."""
        # Get current state
        before = await client.get(f"{BASE}/1")
        assert before.status_code == 200
        before_data = before.json()

        # Verify-update with a different label
        await client.post(f"{BASE}/1/verify-update", json={
            "label": "ChangedLabel",
            "mt5_password": "!p27ed4U",
        })

        # Account should be unchanged
        after = await client.get(f"{BASE}/1")
        assert after.json()["label"] == before_data["label"]


# ---------------------------------------------------------------
# DELETE consistency
# ---------------------------------------------------------------

class TestDeleteConsistency:
    """Deleted account must vanish from the list immediately."""

    @pytest.mark.asyncio
    async def test_delete_immediately_reflected_in_list(
        self, client: httpx.AsyncClient,
    ):
        """Create → delete → list should not contain the deleted account."""
        # Create a temporary account
        create_resp = await client.post(BASE, json={
            "label": "TempTestAcct",
            "mt5_login": 11111111,
            "mt5_password": "test",
            "mt5_server": "TestServer",
        })
        assert create_resp.status_code == 201
        acct_id = create_resp.json()["id"]

        # Delete it
        del_resp = await client.delete(f"{BASE}/{acct_id}")
        assert del_resp.status_code == 204

        # List should NOT contain it
        list_resp = await client.get(BASE)
        ids = [a["id"] for a in list_resp.json()]
        assert acct_id not in ids

        # GET by ID should be 404
        get_resp = await client.get(f"{BASE}/{acct_id}")
        assert get_resp.status_code == 404
