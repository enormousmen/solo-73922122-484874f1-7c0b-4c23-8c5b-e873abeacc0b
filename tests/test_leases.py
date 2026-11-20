"""End-to-end tests for the build-lease API.

Real origin + proxy processes, real HTTP. Run from the repo root:

    python3 -m pytest tests/test_leases.py -v
"""

from __future__ import annotations

import threading
import time
from concurrent.futures import ThreadPoolExecutor

import httpx

from tests.test_e2e import (
    PKG_BIG,
    PKG_SMALL,
    client,
    download_bytes,
    origin,  # noqa: F401  (re-exported session fixture)
    origin_reset,
    origin_stats,
    proxy,  # noqa: F401  (re-exported fixture)
    set_behavior,
    sha256,
)


def create_lease(base: str, paths: list[str], ttl: int) -> httpx.Response:
    with client() as c:
        return c.post(f"{base}/leases", json={"paths": paths, "ttl_seconds": ttl})


def lease_get(base: str, lease_id: str, path: str) -> httpx.Response:
    with client() as c:
        return c.get(f"{base}/leases/{lease_id}/packages/{path}")


def wait_lease_state(base: str, lease_id: str, state: str, timeout: float = 10.0) -> dict:
    deadline = time.monotonic() + timeout
    last: dict = {}
    while time.monotonic() < deadline:
        with client() as c:
            r = c.get(f"{base}/leases/{lease_id}")
        assert r.status_code == 200
        last = r.json()
        if last["state"] == state:
            return last
        time.sleep(0.05)
    raise AssertionError(f"lease {lease_id} did not reach {state}: {last}")


# --------------------------------------------------------------------- #
# grant semantics                                                         #
# --------------------------------------------------------------------- #


def test_lease_granted_only_when_all_paths_cached(proxy, origin):
    origin_reset(origin.url)
    download_bytes(proxy.url, PKG_SMALL)  # warm one path only

    r = create_lease(proxy.url, [PKG_SMALL, PKG_BIG], 60)
    assert r.status_code == 409
    body = r.json()
    reasons = {u["path"]: u["reason"] for u in body["unavailable"]}
    assert reasons == {PKG_BIG: "not_cached"}
    assert PKG_SMALL not in reasons
    with client() as c:
        assert c.get(f"{proxy.url}/leases").json() == {}  # nothing granted

    download_bytes(proxy.url, PKG_BIG)
    r = create_lease(proxy.url, [PKG_SMALL, PKG_BIG], 60)
    assert r.status_code == 201
    lease = r.json()
    assert lease["state"] == "active"
    assert len(lease["pinned"]) == 2
    assert lease["seconds_remaining"] <= 60


def test_lease_refused_while_fill_in_progress(proxy, origin):
    set_behavior(origin.url, delay_ms=100, chunk_size=64 * 1024)  # ~1.6s fill
    try:
        origin_reset(origin.url)
        with client() as c:
            c.stream("GET", f"{proxy.url}/packages/{PKG_SMALL}").__enter__()
            time.sleep(0.3)  # fill is running
            r = create_lease(proxy.url, [PKG_SMALL], 60)
        assert r.status_code == 409
        assert r.json()["unavailable"][0]["reason"] == "still_filling"
    finally:
        set_behavior(origin.url)


def test_lease_request_validation(proxy, origin):
    origin_reset(origin.url)
    download_bytes(proxy.url, PKG_SMALL)
    with client() as c:
        # duplicate paths
        r = c.post(
            f"{proxy.url}/leases",
            json={"paths": [PKG_SMALL, PKG_SMALL], "ttl_seconds": 60},
        )
        assert r.status_code == 422
        # TTL out of range
        r = c.post(f"{proxy.url}/leases", json={"paths": [PKG_SMALL], "ttl_seconds": 301})
        assert r.status_code == 422
        # empty path list
        r = c.post(f"{proxy.url}/leases", json={"paths": [], "ttl_seconds": 60})
        assert r.status_code == 422


def test_lease_max_16_paths_enforced(proxy, origin):
    origin_reset(origin.url)
    download_bytes(proxy.url, PKG_SMALL)
    with client() as c:
        r = c.post(
            f"{proxy.url}/leases",
            json={"paths": [f"p{i}.bin" for i in range(17)], "ttl_seconds": 10},
        )
    assert r.status_code == 422


# --------------------------------------------------------------------- #
# lease downloads                                                         #
# --------------------------------------------------------------------- #


def test_lease_download_repeatable_and_serves_pinned_bytes(proxy, origin):
    origin_reset(origin.url)
    body = download_bytes(proxy.url, PKG_SMALL)
    lease = create_lease(proxy.url, [PKG_SMALL], 60).json()
    lid = lease["lease_id"]

    for _ in range(3):  # same lease can fetch the package repeatedly
        r = lease_get(proxy.url, lid, PKG_SMALL)
        assert r.status_code == 200
        assert r.headers["x-cache"] == "LEASE"
        assert r.headers["x-lease"] == lid
        assert r.content == body

    with client() as c:
        info = c.get(f"{proxy.url}/leases/{lid}").json()
    assert info["downloads_started"] == 3
    assert info["in_flight_downloads"] == 0


def test_lease_download_unknown_id_and_uncovered_path(proxy, origin):
    origin_reset(origin.url)
    download_bytes(proxy.url, PKG_SMALL)
    download_bytes(proxy.url, PKG_BIG)
    lid = create_lease(proxy.url, [PKG_SMALL], 60).json()["lease_id"]

    with client() as c:
        r1 = c.get(f"{proxy.url}/leases/lease-nope/packages/{PKG_SMALL}")
        r2 = c.get(f"{proxy.url}/leases/{lid}/packages/{PKG_BIG}")
    assert r1.status_code == 404
    assert r2.status_code == 403


def test_lease_keeps_old_bytes_after_purge_and_refill(proxy, origin):
    origin_reset(origin.url)
    old = download_bytes(proxy.url, PKG_SMALL)
    old_sha = sha256(old)
    lid = create_lease(proxy.url, [PKG_SMALL], 120).json()["lease_id"]

    with client() as c:
        new = c.post(f"{origin.url}/control/content/{PKG_SMALL}").json()
        c.post(f"{proxy.url}/purge/{PKG_SMALL}")
    assert new["sha256"] != old_sha

    # Ordinary traffic re-fills and serves the new generation.
    fresh = download_bytes(proxy.url, PKG_SMALL)
    assert sha256(fresh) == new["sha256"]
    assert origin_stats(origin.url)["paths"][PKG_SMALL] == 2

    # The lease still serves the pinned old bytes.
    r = lease_get(proxy.url, lid, PKG_SMALL)
    assert r.status_code == 200
    assert sha256(r.content) == old_sha

    # The old generation survives as a .keep file and the lease still holds it.
    keep_files = list(proxy.cache_dir.glob(f"{PKG_SMALL}.keep-*"))
    assert len(keep_files) == 1
    with client() as c:
        info = c.get(f"{proxy.url}/leases/{lid}").json()
    pinned = {p["path"]: p for p in info["pinned"]}
    assert pinned[PKG_SMALL]["sha256"] == old_sha
    assert keep_files[0].read_bytes() == old


def test_purge_then_lease_expiry_releases_pinned_bytes(proxy, origin):
    origin_reset(origin.url)
    download_bytes(proxy.url, PKG_SMALL)
    lid = create_lease(proxy.url, [PKG_SMALL], 1).json()["lease_id"]

    with client() as c:
        c.post(f"{proxy.url}/purge/{PKG_SMALL}")
    # While the lease lives, the old file stays on disk.
    leftovers = list(proxy.cache_dir.glob(f"{PKG_SMALL}*"))
    assert leftovers, "pinned file must survive purge"

    info = wait_lease_state(proxy.url, lid, "released", timeout=5)
    assert info["in_flight_downloads"] == 0

    def gone() -> bool:
        return not list(proxy.cache_dir.glob(f"{PKG_SMALL}*"))

    deadline = time.monotonic() + 5
    while time.monotonic() < deadline and not gone():
        time.sleep(0.05)
    assert gone(), "released lease should let the purged generation be reaped"


# --------------------------------------------------------------------- #
# expiry: two-phase, in-flight drain, disconnect                          #
# --------------------------------------------------------------------- #


def test_expired_lease_refuses_new_but_finishes_in_flight(proxy, origin):
    set_behavior(origin.url, delay_ms=20, chunk_size=64 * 1024)
    try:
        origin_reset(origin.url)
        pinned = download_bytes(proxy.url, PKG_BIG)
        lid = create_lease(proxy.url, [PKG_BIG], 1).json()["lease_id"]
        result: dict = {}

        def slow_reader():
            with client(timeout_read=60).stream(
                "GET", f"{proxy.url}/leases/{lid}/packages/{PKG_BIG}"
            ) as r:
                assert r.status_code == 200
                chunks = []
                for chunk in r.iter_bytes(256 * 1024):
                    chunks.append(chunk)
                    time.sleep(0.05)
                result["body"] = b"".join(chunks)

        t = threading.Thread(target=slow_reader)
        t.start()
        time.sleep(0.4)  # download in flight; let the TTL elapse

        wait_lease_state(proxy.url, lid, "expired")
        with client() as c:
            r = c.get(f"{proxy.url}/leases/{lid}/packages/{PKG_BIG}")
        assert r.status_code == 410  # new downloads refused after expiry

        t.join(30)
        assert result["body"] == pinned  # accepted download read to the end

        info = wait_lease_state(proxy.url, lid, "released")
        assert info["in_flight_downloads"] == 0
    finally:
        set_behavior(origin.url)


def test_disconnect_ends_in_flight_and_allows_release(proxy, origin):
    origin_reset(origin.url)
    download_bytes(proxy.url, PKG_BIG)
    lid = create_lease(proxy.url, [PKG_BIG], 1).json()["lease_id"]

    # A slow client keeps the lease download in flight past the 1s TTL.
    with client(timeout_read=60).stream(
        "GET", f"{proxy.url}/leases/{lid}/packages/{PKG_BIG}"
    ) as r:
        assert r.status_code == 200
        it = r.iter_bytes(64 * 1024)
        next(it)
        time.sleep(0.2)
        with client() as c:
            in_flight = c.get(f"{proxy.url}/leases/{lid}").json()["in_flight_downloads"]
        assert in_flight == 1
        time.sleep(1.0)  # TTL elapses while the download is still open
        with client() as c:
            expired = c.get(f"{proxy.url}/leases/{lid}").json()["state"]
        assert expired == "expired"
        with client() as c:
            refused = c.get(f"{proxy.url}/leases/{lid}/packages/{PKG_BIG}")
        assert refused.status_code == 410
    # exiting the context closes the connection -> download ends -> release

    info = wait_lease_state(proxy.url, lid, "released", timeout=6)
    assert info["in_flight_downloads"] == 0


def test_concurrent_lease_downloads_share_pinned_generation(proxy, origin):
    origin_reset(origin.url)
    pinned = download_bytes(proxy.url, PKG_SMALL)
    lid = create_lease(proxy.url, [PKG_SMALL], 60).json()["lease_id"]

    def fetch(_):
        r = lease_get(proxy.url, lid, PKG_SMALL)
        assert r.status_code == 200
        return r.content

    with ThreadPoolExecutor(6) as ex:
        bodies = list(ex.map(fetch, range(6)))
    assert all(b == pinned for b in bodies)
    # No extra origin traffic: lease reads the pinned complete generation.
    assert origin_stats(origin.url)["paths"][PKG_SMALL] == 1


# --------------------------------------------------------------------- #
# restart                                                                 #
# --------------------------------------------------------------------- #


def test_leases_vanish_on_restart_but_cache_remains(proxy, origin):
    origin_reset(origin.url)
    body = download_bytes(proxy.url, PKG_SMALL)
    lid = create_lease(proxy.url, [PKG_SMALL], 300).json()["lease_id"]

    proxy.restart()

    with client() as c:
        assert c.get(f"{proxy.url}/leases").json() == {}
        assert c.get(f"{proxy.url}/leases/{lid}").status_code == 404
        r = c.get(f"{proxy.url}/packages/{PKG_SMALL}")
    assert r.status_code == 200 and r.headers["x-cache"] == "HIT"
    assert r.content == body
    assert origin_stats(origin.url)["paths"][PKG_SMALL] == 1  # no re-fetch
