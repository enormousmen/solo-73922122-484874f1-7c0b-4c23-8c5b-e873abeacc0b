"""Single-process HTTP caching proxy for fixed-origin package downloads.

FastAPI (HTTP surface) + aiohttp (origin client). One uvicorn worker, all
state in this process; completed packages live on disk under CACHE_DIR.

Design notes
------------
* Single-flight: one fill task per path generation; concurrent misses attach
  to the same fill and read the growing ``.part-<gen>`` file from offset 0.
* Slow clients never block the fill or other clients: the fill only writes to
  disk; every reader tails the file at its own pace. Per-client memory is one
  read buffer, independent of package size.
* Purge unregisters the current generation. In-flight fills keep feeding
  their existing readers but are never re-registered; old files are unlinked
  once the last reader detaches.
* Origin failure fails every attached download and never leaves a cached
  half-package. Startup deletes leftover ``.part-*`` files and keeps complete
  packages.

Build leases
------------
A build lease pins one concrete, *complete* generation per path for a bounded
time window, so a build keeps downloading the exact bytes it started with
even if the path is purged and re-filled meanwhile.

* ``POST /leases`` is atomic: every requested path must resolve to a complete
  local cache entry at the same instant, otherwise the whole request gets
  ``409`` listing every unavailable path. Nothing is pinned on failure.
* A granted lease increments a ``holders`` counter on each pinned entry; the
  reaper gate keeps purged files on disk until the lease releases them.
* Lease downloads stream the pinned generation only. After expiry, new lease
  downloads are refused (``410``); downloads already accepted run to the end.
* When an expired lease has no in-flight downloads left it is released
  automatically (background reaper + immediate check when a download ends);
  disconnects count as download endings.
* Lease state is in-memory only: a process restart invalidates every lease.
"""

from __future__ import annotations

import asyncio
import json
import os
import re
import time
import uuid
from contextlib import asynccontextmanager
from pathlib import Path

import aiohttp
from fastapi import FastAPI
from fastapi.responses import PlainTextResponse, StreamingResponse
from pydantic import BaseModel, Field, model_validator

ORIGIN_BASE_URL = os.environ.get("ORIGIN_BASE_URL", "http://127.0.0.1:9000").rstrip("/")
CACHE_DIR = Path(os.environ.get("CACHE_DIR", "./cache")).resolve()
CHUNK = 256 * 1024  # bytes per origin read / client write / disk read

# Reaper cadence for expiring leases and releasing drained ones.
LEASE_REAP_INTERVAL = 0.2

_SEGMENT_RE = re.compile(r"\A[A-Za-z0-9._~!$&'()*+,;=:@-]+\Z")


def validate_key(raw: str) -> str:
    """Map a URL path to a safe relative cache key; reject traversal."""
    if not raw or raw.endswith("/"):
        raise ValueError("empty package path")
    parts = raw.split("/")
    for seg in parts:
        if seg in ("", ".", ".."):
            raise ValueError(f"illegal path segment: {seg!r}")
        if not _SEGMENT_RE.match(seg):
            raise ValueError(f"illegal characters in segment: {seg!r}")
    return "/".join(parts)


def _unlink_quiet(path: Path) -> None:
    try:
        path.unlink()
    except OSError:
        pass


class FillFailed(Exception):
    """Raised inside a client stream when the origin fill failed."""


class Entry:
    """One generation of a cached object.

    state: filling -> complete          (registered, file renamed into place)
                 \\-> closed             (purged mid-fill; fill finished, file
                                          kept only until readers drain)
                 \\-> failed             (origin error; part file removed)

    ``holders`` counts build leases pinning this generation. A purged entry
    with holders keeps its bytes on disk until every holding lease releases.
    """

    def __init__(self, key: str) -> None:
        self.key = key
        self.gen = uuid.uuid4().hex[:12]
        self.state = "filling"
        self.purged = False
        self.readers = 0
        self.holders = 0
        # When a complete generation is purged while still referenced, its
        # file is renamed here so the next fill can claim ``final_path``
        # without overwriting the pinned bytes.
        self.retained_path: Path | None = None
        self.written = 0          # bytes flushed to the part file so far
        self.size = -1            # expected total bytes (-1 = not known yet)
        self.error: str | None = None
        self.origin_status: int | None = None
        self.headers_known = False
        self.meta: dict = {}
        self.cond = asyncio.Condition()
        self.fill_task: asyncio.Task | None = None

    @property
    def final_path(self) -> Path:
        return CACHE_DIR / self.key

    @property
    def part_path(self) -> Path:
        return CACHE_DIR / f"{self.key}.part-{self.gen}"

    @property
    def keep_path(self) -> Path:
        return CACHE_DIR / f"{self.key}.keep-{self.gen}"

    @property
    def meta_path(self) -> Path:
        return CACHE_DIR / f"{self.key}.meta"

    def info(self) -> dict:
        return {
            "state": self.state,
            "purged": self.purged,
            "readers": self.readers,
            "holders": self.holders,
            "written": self.written,
            "size": self.size,
            "gen": self.gen,
            "error": self.error,
        }


class Cache:
    def __init__(self) -> None:
        self.entries: dict[str, Entry] = {}
        self.lock = asyncio.Lock()
        self.session: aiohttp.ClientSession | None = None

    # ------------------------------------------------------------------ #
    # registry                                                            #
    # ------------------------------------------------------------------ #

    def _entry_from_disk(self, key: str) -> Entry | None:
        """Adopt a complete on-disk package (e.g. after a restart)."""
        final = CACHE_DIR / key
        try:
            st = final.stat()
        except OSError:
            return None
        if not final.is_file():
            return None
        e = Entry(key)
        e.state = "complete"
        e.size = st.st_size
        e.written = st.st_size
        e.headers_known = True
        try:
            e.meta = json.loads(e.meta_path.read_text())
        except (OSError, ValueError):
            e.meta = {}
        e.meta.setdefault("content_type", "application/octet-stream")
        self.entries[key] = e
        return e

    async def attach(self, key: str) -> Entry:
        """Return the current generation for key, starting a fill if needed."""
        async with self.lock:
            e = self.entries.get(key)
            if e is None:
                e = self._entry_from_disk(key)
            if e is None:
                e = Entry(key)
                self.entries[key] = e
                e.fill_task = asyncio.create_task(self._fill(e))
            e.readers += 1
            return e

    async def detach(self, e: Entry) -> None:
        async with self.lock:
            e.readers -= 1
            self._maybe_reap_locked(e)

    async def purge(self, key: str) -> bool:
        """Unregister the current generation; files go away with last reader.

        Entries pinned by build leases (or still being read) keep their bytes
        on disk: the data file is renamed to a generation-specific ``.keep-*``
        path so a subsequent fill can republish ``final_path`` without
        clobbering the pinned bytes. The keep file is reaped once readers and
        lease holders drain.
        """
        async with self.lock:
            e = self.entries.pop(key, None)
            if e is not None:
                e.purged = True
                if e.readers > 0 or e.holders > 0:
                    keep = e.keep_path
                    if e.retained_path is None and e.final_path.exists():
                        e.final_path.parent.mkdir(parents=True, exist_ok=True)
                        os.replace(e.final_path, keep)
                        e.retained_path = keep
                    _unlink_quiet(e.meta_path)
                self._maybe_reap_locked(e)
                return True
            # Not in memory (e.g. never requested since restart): clean disk.
            existed = False
            final = CACHE_DIR / key
            for p in (final, CACHE_DIR / f"{key}.meta"):
                if p.exists():
                    _unlink_quiet(p)
                    existed = True
            try:
                for p in final.parent.glob(final.name + ".part-*"):
                    _unlink_quiet(p)
                    existed = True
            except OSError:
                pass
            return existed

    def _maybe_reap_locked(self, e: Entry) -> None:
        """Delete files once nobody references the generation anymore."""
        if e.readers > 0 or e.holders > 0 or e.state == "filling":
            return
        if e.state == "failed":
            _unlink_quiet(e.part_path)  # never keep a half package
        elif e.purged:  # complete or closed
            if e.retained_path is not None:
                _unlink_quiet(e.retained_path)
            else:
                _unlink_quiet(e.part_path)
                _unlink_quiet(e.final_path)
            _unlink_quiet(e.meta_path)

    # ------------------------------------------------------------------ #
    # origin fill                                                         #
    # ------------------------------------------------------------------ #

    async def _fill(self, e: Entry) -> None:
        assert self.session is not None
        url = f"{ORIGIN_BASE_URL}/packages/{e.key}"
        try:
            async with self.session.get(url) as resp:
                e.origin_status = resp.status
                if resp.status != 200:
                    e.error = f"origin responded with status {resp.status}"
                    await self._finish(e, "failed")
                    return
                cl = resp.content_length
                e.size = cl if cl is not None else -1
                e.meta = {
                    "content_type": resp.headers.get("Content-Type")
                    or "application/octet-stream",
                    "sha256": resp.headers.get("X-Package-Sha256"),
                    "fetched_at": time.time(),
                }
                e.part_path.parent.mkdir(parents=True, exist_ok=True)
                f = open(e.part_path, "wb")
                try:
                    # Part file exists before readers are allowed to open it.
                    e.headers_known = True
                    async with e.cond:
                        e.cond.notify_all()
                    written = 0
                    async for chunk in resp.content.iter_chunked(CHUNK):
                        await asyncio.to_thread(f.write, chunk)
                        await asyncio.to_thread(f.flush)
                        written += len(chunk)
                        e.written = written
                        async with e.cond:
                            e.cond.notify_all()
                finally:
                    await asyncio.to_thread(f.close)
                if e.size != -1 and written != e.size:
                    raise aiohttp.ClientPayloadError(
                        f"short read from origin: {written}/{e.size} bytes"
                    )
            async with self.lock:
                e.size = written if e.size == -1 else e.size
                e.meta["size"] = written
                if e.purged:
                    # Purged mid-fill: keep the part file for current readers
                    # only; never register this generation.
                    e.state = "closed"
                else:
                    os.replace(e.part_path, e.final_path)  # atomic publish
                    try:
                        e.meta_path.write_text(json.dumps(e.meta))
                    except OSError:
                        pass
                    e.state = "complete"
                async with e.cond:
                    e.cond.notify_all()
                self._maybe_reap_locked(e)
        except asyncio.CancelledError:
            raise
        except Exception as exc:  # noqa: BLE001 - any origin/IO error fails the fill
            e.error = f"{type(exc).__name__}: {exc}"
            await self._finish(e, "failed")

    async def _finish(self, e: Entry, state: str) -> None:
        async with self.lock:
            e.state = state
            if state == "failed" and self.entries.get(e.key) is e:
                del self.entries[e.key]  # next GET retries the origin
            async with e.cond:
                e.cond.notify_all()
            self._maybe_reap_locked(e)

    # ------------------------------------------------------------------ #
    # client streaming                                                    #
    # ------------------------------------------------------------------ #

    @staticmethod
    def _open_current(e: Entry):
        if e.retained_path is not None:
            return open(e.retained_path, "rb")
        try:
            return open(e.part_path, "rb")
        except FileNotFoundError:
            return open(e.final_path, "rb")

    async def stream(self, e: Entry):
        """Yield the package from byte 0, tailing the fill file if needed."""
        offset = 0
        f = await asyncio.to_thread(self._open_current, e)
        try:
            while True:
                async with e.cond:
                    await e.cond.wait_for(
                        lambda: e.written > offset or e.state != "filling"
                    )
                if e.state == "failed":
                    raise FillFailed(e.error or "origin fill failed")
                if e.state == "filling":
                    limit = CHUNK
                else:  # complete | closed: drain exactly to the final size
                    limit = min(CHUNK, e.size - offset)
                    if limit <= 0:
                        break
                data = await asyncio.to_thread(f.read, limit)
                if not data:
                    if e.state == "filling":
                        continue  # defensive; written>offset implies data
                    break
                offset += len(data)
                yield data
        finally:
            try:
                f.close()
            finally:
                await self.detach(e)


# ---------------------------------------------------------------------- #
# Build leases                                                            #
# ---------------------------------------------------------------------- #


class LeaseRequest(BaseModel):
    paths: list[str] = Field(
        ...,
        min_length=1,
        max_length=16,
        description="Distinct package paths (1-16), relative under /packages.",
    )
    ttl_seconds: int = Field(
        ..., ge=1, le=300, description="Lease lifetime in seconds (1-300)."
    )

    @model_validator(mode="after")
    def _check_paths(self) -> "LeaseRequest":
        seen: set[str] = set()
        for raw in self.paths:
            try:
                key = validate_key(raw)
            except ValueError as exc:
                raise ValueError(f"invalid path {raw!r}: {exc}") from None
            if key in seen:
                raise ValueError(f"duplicate path: {key}")
            seen.add(key)
        return self


class Lease:
    """A pinned set of complete package generations for one build."""

    def __init__(self, keys: list[str], entries: dict[str, Entry], ttl: int) -> None:
        self.id = "lease-" + uuid.uuid4().hex
        self.paths = list(keys)
        self.ttl_seconds = ttl
        now = time.monotonic()
        self.expires_at_mono = now + ttl
        self.created_at = time.time()
        self.expires_at = self.created_at + ttl
        # state: active -> expired (TTL elapsed) -> released (holders dropped)
        self.state = "active"
        self.in_flight = 0
        self.downloads = 0
        self.pinned: dict[str, Entry] = dict(entries)

    def public(self) -> dict:
        return {
            "lease_id": self.id,
            "state": self.state,
            "paths": list(self.paths),
            "ttl_seconds": self.ttl_seconds,
            "created_at": self.created_at,
            "expires_at": self.expires_at,
            "seconds_remaining": max(0, round(self.expires_at_mono - time.monotonic(), 3)),
            "in_flight_downloads": self.in_flight,
            "downloads_started": self.downloads,
            "pinned": [
                {
                    "path": key,
                    "gen": e.gen,
                    "size": e.size,
                    "sha256": e.meta.get("sha256"),
                }
                for key, e in self.pinned.items()
            ],
        }


class LeaseManager:
    """Grant, expire and release build leases; all state is in-memory."""

    def __init__(self, cache: Cache) -> None:
        self.cache = cache
        self.leases: dict[str, Lease] = {}
        self._reaper_task: asyncio.Task | None = None

    async def grant(self, req: LeaseRequest) -> tuple[Lease | None, list[dict]]:
        """Atomically pin complete generations; return (lease, unavailable).

        On failure no entry is touched and ``unavailable`` explains every path
        that could not be pinned (missing / still filling).
        """
        keys = [validate_key(raw) for raw in req.paths]
        async with self.cache.lock:
            unavailable: list[dict] = []
            resolved: dict[str, Entry] = {}
            for key in keys:
                e = self.cache.entries.get(key)
                if e is None:
                    e = self.cache._entry_from_disk(key)
                if e is None:
                    unavailable.append({"path": key, "reason": "not_cached"})
                elif e.state != "complete":
                    unavailable.append(
                        {"path": key, "reason": "still_filling", "state": e.state}
                    )
                else:
                    resolved[key] = e
            if unavailable:
                return None, unavailable

            lease = Lease(keys, resolved, req.ttl_seconds)
            for e in resolved.values():
                e.holders += 1
            self.leases[lease.id] = lease
            return lease, []

    async def begin_download(self, lease_id: str, path: str):
        """Validate a lease download and attach a reader.

        Returns ``(lease, entry)`` after incrementing reader/in-flight counts,
        or ``(status_code, message)`` when the download must be refused.
        """
        try:
            key = validate_key(path)
        except ValueError as exc:
            return 400, str(exc)
        async with self.cache.lock:
            lease = self.leases.get(lease_id)
            if lease is None:
                return 404, f"unknown lease id: {lease_id}"
            if lease.state != "active":
                return 410, f"lease {lease_id} is {lease.state}; new downloads are refused"
            entry = lease.pinned.get(key)
            if entry is None:
                return 403, f"path {key} is not covered by lease {lease_id}"
            # Defensive: a pinned generation must stay complete and on disk.
            if entry.state != "complete":
                return 410, f"pinned generation for {key} is no longer available"
            lease.in_flight += 1
            lease.downloads += 1
            entry.readers += 1
            return lease, entry

    async def end_download(self, lease: Lease, entry: Entry) -> None:
        """Bookkeeping when an accepted download finishes or disconnects."""
        async with self.cache.lock:
            lease.in_flight -= 1
            entry.readers -= 1
            self.cache._maybe_reap_locked(entry)
            if lease.state == "expired":
                self._release_locked(lease)

    async def leased_stream(self, lease: Lease, entry: Entry):
        """Stream the pinned generation; always balance begin/end bookkeeping."""
        try:
            async for chunk in self.cache.stream(entry):
                yield chunk
        finally:
            await self.end_download(lease, entry)

    def _mark_expired_locked(self, lease: Lease) -> None:
        if lease.state == "active":
            lease.state = "expired"
        if lease.in_flight == 0:
            self._release_locked(lease)

    def _release_locked(self, lease: Lease) -> None:
        """Drop the pin on every entry; purged files can now be reaped."""
        if lease.state == "released":
            return
        lease.state = "released"
        for entry in lease.pinned.values():
            entry.holders -= 1
            self.cache._maybe_reap_locked(entry)

    async def _reap(self) -> None:
        now = time.monotonic()
        async with self.cache.lock:
            for lease in list(self.leases.values()):
                if lease.state == "active" and now >= lease.expires_at_mono:
                    self._mark_expired_locked(lease)

    async def _reaper_loop(self) -> None:
        while True:
            await asyncio.sleep(LEASE_REAP_INTERVAL)
            try:
                await self._reap()
            except asyncio.CancelledError:
                raise
            except Exception:  # noqa: BLE001 - reaper must never die
                pass

    async def start(self) -> None:
        if self._reaper_task is None:
            self._reaper_task = asyncio.create_task(self._reaper_loop())

    async def stop(self) -> None:
        if self._reaper_task is not None:
            self._reaper_task.cancel()
            try:
                await self._reaper_task
            except (asyncio.CancelledError, Exception):  # noqa: BLE001
                pass
            self._reaper_task = None

    async def status(self) -> dict:
        async with self.cache.lock:
            return {
                lid: lease.public()
                for lid, lease in sorted(self.leases.items())
            }


cache = Cache()
leases = LeaseManager(cache)


def cleanup_cache_dir() -> None:
    """On boot: drop partial fills, pinned keeps and orphan meta.

    Leases are in-memory only, so any ``.keep-*`` file belongs to a lease
    that died with the process and must not survive the restart; complete
    packages (plain data files) are kept.
    """
    for p in CACHE_DIR.rglob("*"):
        if not p.is_file():
            continue
        name = p.name
        if ".part-" in name or ".keep-" in name:
            _unlink_quiet(p)
        elif name.endswith(".meta"):
            final = p.with_name(name[: -len(".meta")])
            if not final.exists():
                _unlink_quiet(p)


@asynccontextmanager
async def lifespan(app: FastAPI):
    CACHE_DIR.mkdir(parents=True, exist_ok=True)
    cleanup_cache_dir()
    # Lease state is intentionally in-memory only: a restart starts with an
    # empty lease table and the usual part/orphan cleanup above.
    timeout = aiohttp.ClientTimeout(total=None, connect=10, sock_connect=10, sock_read=300)
    cache.session = aiohttp.ClientSession(timeout=timeout)
    await leases.start()
    try:
        yield
    finally:
        await leases.stop()
        await cache.session.close()


app = FastAPI(title="pkg-cache-proxy", version="1.1.0", lifespan=lifespan)


@app.get("/healthz")
async def healthz():
    return {"ok": True}


@app.get("/packages/{path:path}")
async def get_package(path: str):
    """Download a complete package by path (streamed from byte 0)."""
    try:
        key = validate_key(path)
    except ValueError as exc:
        return PlainTextResponse(str(exc), status_code=400)

    entry = await cache.attach(key)
    hit = entry.state == "complete"
    try:
        if not entry.headers_known:
            async with entry.cond:
                await entry.cond.wait_for(
                    lambda: entry.headers_known or entry.state == "failed"
                )
    except asyncio.CancelledError:
        await cache.detach(entry)
        raise

    if entry.state == "failed":
        await cache.detach(entry)
        status = (
            entry.origin_status
            if entry.origin_status and entry.origin_status != 200
            else 502
        )
        return PlainTextResponse(f"origin fetch failed: {entry.error}", status_code=status)

    headers = {"X-Cache": "HIT" if hit else "MISS"}
    if entry.size >= 0:
        headers["Content-Length"] = str(entry.size)
    sha256 = entry.meta.get("sha256")
    if sha256:
        headers["X-Package-Sha256"] = sha256
    return StreamingResponse(
        cache.stream(entry),
        media_type=entry.meta.get("content_type", "application/octet-stream"),
        headers=headers,
    )


@app.post("/purge/{path:path}")
async def purge_package(path: str):
    """Purge one path. The next download re-fills from the origin; downloads
    already in progress keep reading their old generation to completion.

    Generations pinned by active leases remain on disk for lease downloads
    until the leases release them."""
    try:
        key = validate_key(path)
    except ValueError as exc:
        return PlainTextResponse(str(exc), status_code=400)
    was_cached = await cache.purge(key)
    return {"path": key, "purged": True, "was_cached": was_cached}


@app.get("/cache/status")
async def cache_status():
    async with cache.lock:
        return {k: e.info() for k, e in cache.entries.items()}


# ---------------------------------------------------------------------- #
# Build lease API                                                          #
# ---------------------------------------------------------------------- #


@app.post(
    "/leases",
    status_code=201,
    responses={409: {"description": "one or more paths not fully cached"}},
)
async def create_lease(req: LeaseRequest):
    """Request a build lease.

    Submit 1-16 distinct package paths and a 1-300 second TTL. The whole
    group must hit complete local cache entries at the same instant; on any
    miss (package absent or still filling) the request fails atomically with
    ``409`` and lists every unavailable path. A granted lease pins the exact
    current generations: purge + re-fill does not change lease bytes until
    the lease is released.
    """
    lease, unavailable = await leases.grant(req)
    if lease is None:
        return PlainTextResponse(
            json.dumps(
                {
                    "error": "lease refused: not all paths are fully cached",
                    "unavailable": unavailable,
                },
                ensure_ascii=False,
            ),
            status_code=409,
            media_type="application/json",
        )
    return lease.public()


@app.get("/leases")
async def list_leases():
    """List all leases (active, expired and recently released)."""
    return await leases.status()


@app.get("/leases/{lease_id}")
async def get_lease(lease_id: str):
    """Inspect one lease: state, TTL, in-flight count and pinned generations."""
    async with cache.lock:
        lease = leases.leases.get(lease_id)
        if lease is None:
            return PlainTextResponse(f"unknown lease id: {lease_id}", status_code=404)
        return lease.public()


@app.get("/leases/{lease_id}/packages/{path:path}")
async def lease_download(lease_id: str, path: str):
    """Download one path through a lease (repeatable while the lease is active).

    Serves the pinned generation regardless of purges or newer fills. After
    expiry the endpoint returns ``410`` for new requests; downloads already
    streaming are allowed to finish and the lease releases once they end.
    """
    result = await leases.begin_download(lease_id, path)
    if isinstance(result[0], int):
        status, message = result
        return PlainTextResponse(message, status_code=status)
    lease, entry = result

    headers = {
        "X-Cache": "LEASE",
        "X-Lease": lease.id,
        "X-Lease-State": lease.state,
    }
    if entry.size >= 0:
        headers["Content-Length"] = str(entry.size)
    digest = entry.meta.get("sha256")
    if digest:
        headers["X-Package-Sha256"] = digest
    return StreamingResponse(
        leases.leased_stream(lease, entry),
        media_type=entry.meta.get("content_type", "application/octet-stream"),
        headers=headers,
    )
