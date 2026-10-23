#!/usr/bin/env python3
"""
deed_fetch.py — pull-queue worker for Luxembourg (RESA) publication PDFs.

Runs on a GitHub-hosted runner (public repository, free minutes). One job:

    claim  ≤ BATCH_SIZE publications from the server queue
    for each journal in the batch (newest first):
        open the register's journal page  → one FriendlyCaptcha solve (list)
        for each claimed publication on that page:
            click the row's own "Download the publication" control
                                          → one captcha solve per deed
            POST the PDF bytes to the server (base64) with the timings
    release whatever was not tried, post a run summary, exit cleanly

Every limit that protects the register is enforced on the SERVER (daily
ceiling, batch cap, pauses) — this script only honours what it is given and
stops early when its own wall-clock budget cannot fit another solve.

What this script never does: write a PDF to disk here, upload an artifact,
print an entity name or deed text, disguise the browser, call a captcha
service, or route through a proxy. The browser solves the proof-of-work the
page hands it, exactly as a visitor's would.

Environment (set by the workflow; secrets by the repository owner):
    SERVER_URL, PIPELINE_INGEST_TOKEN            required (run mode)
    BATCH_SIZE=10  CAPTCHA_TIMEOUT_S=720  JOB_BUDGET_MIN=95  SOLVE_ESTIMATE_S=300
    HEARTBEAT_S=120  NAV_TIMEOUT_S=60  POLL_S=5
    GITHUB_RUN_ID / GITHUB_REPOSITORY            worker id

Modes:
    python deed_fetch.py            # one queue job
    python deed_fetch.py --probe    # no server: newest journal, one Registration
                                    # row, report captcha times + byte count only
"""
from __future__ import annotations

import argparse
import base64
import hashlib
import json
import logging
import os
import re
import sys
import threading
import time
from datetime import datetime, timedelta, timezone
from typing import Optional

import requests

log = logging.getLogger("deed_fetch")

LBR_PORTAL = "https://www.lbr.lu/mjrcs-web-front/"
LBR_API = "https://www.lbr.lu/mjrcs-web-api/v1"
JOURNAL_PAGE = "https://www.lbr.lu/mjrcs-web-front/publication-journal/{jid}"
DOWNLOAD_ICON = "svg[aria-label='Download the publication']"
CAPTCHA_ERROR_TEXT = "An error occurred while validating the captcha"
TYPE_FILTER = "select[id$='_pub-type']"
NEXT_PAGE_ENABLED = "svg.chevron-enabled[data-icon='chevron-right']"
CURRENT_PAGE = "span.page.text-decoration-underline"
USER_AGENT = ("Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) "
              "Chrome/130 Safari/537.36 ii-deed-fetch/1.0")

# Register-side failure classes strike the publication on the server; the
# machine-side ones ("list-…", "journal-missing", "worker-crash…") never do.
REGISTER_SIDE = ("download-captcha-timeout", "download-blocked", "not-a-pdf")


def env_int(name: str, default: int) -> int:
    try:
        return int(os.getenv(name, "") or default)
    except ValueError:
        return default


# ── Pure helpers (unit-tested in the private repository) ─────────────────────

def journal_key(journal_id: str, publication_number: str) -> str:
    """'RESA-2026_222_1_0' / 'RESA_2026_222' / 'ID_RESA_2026_222' → '2026_222'."""
    for cand in (journal_id, publication_number):
        m = re.search(r"(\d{4})[_-](\d+)", cand or "")
        if m:
            return f"{m.group(1)}_{m.group(2)}"
    return publication_number


def group_by_journal(items: list[dict]) -> list[tuple[str, list[dict]]]:
    """Claimed rows → [(journal key, rows)] in the server's order (priority first)."""
    groups: dict[str, list[dict]] = {}
    for row in items:
        groups.setdefault(journal_key(row.get("journal_id") or "", row["publication_number"]), []).append(row)
    return list(groups.items())


def row_pattern(publication_number: str) -> re.Pattern:
    """Matches the table row for exactly this publication (".87" must not hit ".877")."""
    return re.compile(re.escape(publication_number) + r"(?!\d)")


def budget_allows(started: float, now: float, budget_min: int, estimate_s: int) -> bool:
    """True when another solve of `estimate_s` seconds still fits before the job budget ends."""
    return (now - started) + estimate_s <= budget_min * 60


def report_body(row: dict, worker_id: str, pdf: Optional[bytes], *, error: Optional[str] = None,
                list_captcha_s: Optional[float] = None, download_captcha_s: Optional[float] = None) -> dict:
    """The POST /deed-queue/report body. OK carries the bytes (base64) and their sha256."""
    body = {"queue_id": row["id"], "worker_id": worker_id,
            "outcome": "OK" if pdf is not None and error is None else "FAILED",
            "error": error, "list_captcha_s": list_captcha_s, "download_captcha_s": download_captcha_s,
            "fetched_at": datetime.now(timezone.utc).isoformat()}
    if pdf is not None and error is None:
        body["pdf_b64"] = base64.b64encode(pdf).decode("ascii")
        body["sha256"] = hashlib.sha256(pdf).hexdigest()
        body["byte_size"] = len(pdf)
    return body


# ── Server client ────────────────────────────────────────────────────────────

class Queue:
    def __init__(self, server_url: str, token: str, worker_id: str):
        self.base = server_url.rstrip("/") + "/api/v1/deed-queue"
        self.worker_id = worker_id
        self.sess = requests.Session()
        self.sess.headers.update({"X-Pipeline-Token": token, "User-Agent": USER_AGENT})

    def _post(self, path: str, body: dict, timeout: int = 120) -> dict:
        r = self.sess.post(f"{self.base}/{path}", json=body, timeout=timeout)
        r.raise_for_status()
        return r.json()

    def claim(self, batch_size: int) -> dict:
        return self._post("claim", {"worker_id": self.worker_id, "batch_size": batch_size}, timeout=60)

    def heartbeat(self, ids: list[int]) -> None:
        if ids:
            self._post("heartbeat", {"worker_id": self.worker_id, "queue_ids": ids}, timeout=30)

    def report(self, body: dict) -> dict:
        return self._post("report", body, timeout=300)

    def release(self, ids: list[int], reason: str) -> None:
        if ids:
            self._post("release", {"worker_id": self.worker_id, "queue_ids": ids, "reason": reason}, timeout=60)

    def run_finished(self, body: dict) -> None:
        body["worker_id"] = self.worker_id
        self._post("run-finished", body, timeout=60)


# ── Register (JSON API, no captcha) ──────────────────────────────────────────

def journals_between(start: datetime, end: datetime) -> list[dict]:
    r = requests.get(f"{LBR_API}/journals",
                     params={"journalType": "RESA", "startDate": start.strftime("%d%m%Y"),
                             "endDate": end.strftime("%d%m%Y")},
                     headers={"Accept": "application/json", "User-Agent": USER_AGENT, "Referer": LBR_PORTAL},
                     timeout=30)
    r.raise_for_status()
    return (r.json() or {}).get("items", [])


def journal_page_id(publication_date: str, key: str) -> Optional[str]:
    """Journal record id (RESA-2026_222_1_0) for the page route, by the publication's date."""
    if not publication_date:
        return None
    d = datetime.strptime(publication_date[:10], "%Y-%m-%d")
    try:
        items = journals_between(d, d)
    except (requests.RequestException, ValueError) as exc:
        log.warning("journals API failed for %s: %s", publication_date, exc)
        return None
    for it in items:
        jid = str(it.get("id") or "")
        if journal_key(jid, "") == key:
            return jid
    return str(items[0]["id"]) if items else None


# ── Browser ──────────────────────────────────────────────────────────────────

def _decline_cookies(page) -> None:
    from playwright.sync_api import TimeoutError as PWTimeout
    for sel in ("button.orejime-Button--decline", "text=I refuse"):
        try:
            page.locator(sel).first.click(timeout=3000)
            return
        except PWTimeout:
            continue


def wait_for_list(page, timeout_s: int, poll_s: int) -> tuple[str, float]:
    """Wait for the publication table (list captcha solved). ('ok'|'captcha'|'timeout', seconds)."""
    t0 = time.time()
    last_log = 0.0
    while time.time() - t0 < timeout_s:
        if page.locator(DOWNLOAD_ICON).count() > 0:
            return "ok", time.time() - t0
        if time.time() - last_log >= 60:
            log.info("list captcha: t+%ds", int(time.time() - t0))
            last_log = time.time()
        time.sleep(poll_s)
    status = "captcha" if CAPTCHA_ERROR_TEXT in page.inner_text("body") else "timeout"
    return status, time.time() - t0


def apply_type_filter(page, deposit_type: str) -> bool:
    sel = page.locator(TYPE_FILTER)
    if sel.count() == 0:
        return False
    values = sel.first.evaluate("s => Array.from(s.options).map(o => o.value)")
    if deposit_type not in values:
        return False
    before = page.locator("table tr").count()
    sel.first.select_option(deposit_type)
    for _ in range(40):
        time.sleep(0.5)
        if page.locator(DOWNLOAD_ICON).count() > 0 and page.locator("table tr").count() != before:
            break
    return True


def _current_page_number(page) -> Optional[int]:
    loc = page.locator(CURRENT_PAGE)
    if loc.count() == 0:
        return None
    try:
        return int(loc.first.inner_text().strip())
    except ValueError:
        return None


def go_next_page(page) -> bool:
    before = _current_page_number(page)
    nxt = page.locator(NEXT_PAGE_ENABLED)
    if nxt.count() == 0:
        return False
    nxt.first.click()
    for _ in range(20):
        time.sleep(0.25)
        after = _current_page_number(page)
        if after is not None and after != before:
            return True
    return False


def download_row(page, tr, timeout_ms: int, blocked: dict) -> tuple[Optional[bytes], Optional[str], float]:
    """Click the row's download control; (pdf bytes | None, error class | None, seconds)."""
    from playwright.sync_api import TimeoutError as PWTimeout
    from pathlib import Path
    t0 = time.time()
    try:
        with page.expect_download(timeout=timeout_ms) as dl:
            tr.locator(DOWNLOAD_ICON).first.click()
        data = Path(dl.value.path()).read_bytes()
    except PWTimeout:
        return None, ("download-blocked" if blocked.get("hit") else "download-captcha-timeout"), time.time() - t0
    if not data.startswith(b"%PDF"):
        return None, f"not-a-pdf ({len(data)} bytes)", time.time() - t0
    return data, None, time.time() - t0


def launch(p, nav_timeout_ms: int):
    browser = p.chromium.launch(headless=True, args=["--disable-background-timer-throttling",
                                                     "--disable-backgrounding-occluded-windows",
                                                     "--disable-renderer-backgrounding"])
    ctx = browser.new_context(user_agent=USER_AGENT, accept_downloads=True, locale="en-GB")
    page = ctx.new_page()
    blocked = {"hit": False}
    page.on("response", lambda r: blocked.__setitem__("hit", True)
            if "/publications/" in r.url and r.status in (403, 429) else None)
    page.goto(LBR_PORTAL, wait_until="domcontentloaded", timeout=nav_timeout_ms)
    _decline_cookies(page)
    return browser, page, blocked


# ── Probe mode ───────────────────────────────────────────────────────────────

def probe(captcha_timeout_s: int, nav_timeout_s: int, poll_s: int) -> int:
    """Newest journal → list captcha → first Registration row → download; print timings; discard."""
    from playwright.sync_api import sync_playwright
    today = datetime.now(timezone.utc)
    items = journals_between(today - timedelta(days=7), today)
    if not items:
        log.error("no RESA journal in the last 7 days")
        return 1
    newest = sorted(items, key=lambda it: it.get("date", ""))[-1]
    jid = str(newest["id"])
    log.info("probe: journal %s (%s)", jid, newest.get("date"))
    with sync_playwright() as p:
        browser, page, blocked = launch(p, nav_timeout_s * 1000)
        try:
            page.goto(JOURNAL_PAGE.format(jid=jid), wait_until="domcontentloaded", timeout=nav_timeout_s * 1000)
            status, list_s = wait_for_list(page, captcha_timeout_s, poll_s)
            print(f"PROBE list_status={status} list_captcha_s={list_s:.0f}", flush=True)
            if status != "ok":
                return 1
            apply_type_filter(page, "INS")
            rows = page.locator("table tr").filter(has_text=re.compile(r"\bRegistration\b"))
            n = rows.count()
            print(f"PROBE registration_rows_on_page={n}", flush=True)
            tr = rows.first if n else page.locator("table tr").filter(has=page.locator(DOWNLOAD_ICON)).first
            pub = re.search(r"RESA_\d{4}_\d+\.\d+", tr.inner_text())
            print(f"PROBE publication={pub.group(0) if pub else '?'}", flush=True)
            pdf, err, dl_s = download_row(page, tr, captcha_timeout_s * 1000, blocked)
            if pdf is None:
                print(f"PROBE download_status={err} download_captcha_s={dl_s:.0f}", flush=True)
                return 1
            print(f"PROBE download_status=ok download_captcha_s={dl_s:.0f} pdf_bytes={len(pdf)} "
                  f"sha256={hashlib.sha256(pdf).hexdigest()[:16]}", flush=True)
            return 0
        finally:
            browser.close()


# ── Run mode ─────────────────────────────────────────────────────────────────

def run(args: argparse.Namespace) -> int:
    from playwright.sync_api import Error as PWError, sync_playwright
    server = os.getenv("SERVER_URL", "")
    token = os.getenv("PIPELINE_INGEST_TOKEN", "")
    if not server or not token:
        log.error("SERVER_URL and PIPELINE_INGEST_TOKEN are required")
        return 2
    repo = os.getenv("GITHUB_REPOSITORY", "local").split("/")[-1]
    worker_id = f"{repo}-{os.getenv('GITHUB_RUN_ID', str(int(time.time())))}"
    batch = env_int("BATCH_SIZE", 10)
    captcha_timeout_s = env_int("CAPTCHA_TIMEOUT_S", 720)
    budget_min = env_int("JOB_BUDGET_MIN", 95)
    solve_estimate_s = env_int("SOLVE_ESTIMATE_S", 300)
    heartbeat_s = env_int("HEARTBEAT_S", 120)
    nav_timeout_s = env_int("NAV_TIMEOUT_S", 60)
    poll_s = env_int("POLL_S", 5)

    q = Queue(server, token, worker_id)
    started = time.time()
    summary = {"started_at": datetime.now(timezone.utc).isoformat(), "claimed": 0, "fetched": 0,
               "failed": 0, "released": 0, "list_captcha_s": None, "stop_reason": "complete", "notes": None}
    try:
        claimed = q.claim(batch)
    except requests.RequestException as exc:
        log.error("claim failed: %s", exc)            # server busy / down: nothing claimed, nothing to undo
        return 0
    items = claimed.get("items", [])
    summary["claimed"] = len(items)
    if not items:
        summary["stop_reason"] = f"nothing-to-do ({claimed.get('paused') or 'queue empty'})"
        log.info("%s; remaining=%s", summary["stop_reason"], claimed.get("remaining"))
        _finish(q, summary)
        return 0
    log.info("claimed %d (remaining %s): %s", len(items), claimed.get("remaining"),
             [r["publication_number"] for r in items])

    open_ids = {r["id"] for r in items}
    stop = threading.Event()

    def beat() -> None:
        while not stop.wait(heartbeat_s):
            try:
                q.heartbeat(sorted(open_ids))
            except requests.RequestException as exc:
                log.warning("heartbeat failed: %s", exc)
    threading.Thread(target=beat, daemon=True).start()

    def release_rest(reason: str) -> None:
        ids = sorted(open_ids)
        if ids:
            try:
                q.release(ids, reason)
                summary["released"] += len(ids)
            except requests.RequestException as exc:
                log.warning("release failed (%s) — the server's stale reclaim will free them", exc)
            open_ids.clear()

    try:
        with sync_playwright() as p:
            browser, page, blocked = launch(p, nav_timeout_s * 1000)
            try:
                for key, rows in group_by_journal(items):
                    if not budget_allows(started, time.time(), budget_min, solve_estimate_s):
                        summary["stop_reason"] = "budget"
                        break
                    jid = journal_page_id(rows[0].get("publication_date") or "", key)
                    if not jid:
                        for r in rows:
                            _report(q, summary, open_ids, report_body(r, worker_id, None, error="journal-missing"))
                        continue
                    page.goto(JOURNAL_PAGE.format(jid=jid), wait_until="domcontentloaded",
                              timeout=nav_timeout_s * 1000)
                    status, list_s = wait_for_list(page, captcha_timeout_s, poll_s)
                    summary["list_captcha_s"] = round(list_s)
                    log.info("journal %s list %s after %.0fs", key, status, list_s)
                    if status != "ok":
                        # List-level failure is about this runner/session, not the rows:
                        # release everything, stop the run, let the server see it.
                        summary["stop_reason"] = f"list-{status}"
                        release_rest(f"list-{status}")
                        break
                    kinds = {r.get("deposit_type_code") or "INS" for r in rows}
                    if len(kinds) == 1:
                        apply_type_filter(page, next(iter(kinds)))
                    pending = {r["publication_number"]: r for r in rows}
                    pages_walked = 0
                    while pending:
                        pages_walked += 1
                        for pub in list(pending):
                            if not budget_allows(started, time.time(), budget_min, solve_estimate_s):
                                summary["stop_reason"] = "budget"
                                pending = {}
                                break
                            tr = page.locator("table tr").filter(has_text=row_pattern(pub))
                            if tr.count() == 0:
                                continue
                            row = pending.pop(pub)
                            pdf, err, dl_s = download_row(page, tr.first, captcha_timeout_s * 1000, blocked)
                            blocked["hit"] = False
                            body = report_body(row, worker_id, pdf, error=err, list_captcha_s=round(list_s),
                                               download_captcha_s=round(dl_s))
                            _report(q, summary, open_ids, body)
                            log.info("%s %s %.0fs%s", pub, "ok" if pdf else "FAILED", dl_s,
                                     f" ({len(pdf)} bytes)" if pdf else f" {err}")
                            if err and err.startswith("download-blocked"):
                                summary["stop_reason"] = "blocked"
                                pending = {}
                                break
                        if summary["stop_reason"] in ("budget", "blocked"):
                            break
                        if pending and not go_next_page(page):
                            break
                    for pub, row in pending.items():
                        _report(q, summary, open_ids,
                                report_body(row, worker_id, None,
                                            error=f"row-missing on journal page (after {pages_walked} pages)"))
                    if summary["stop_reason"] in ("budget", "blocked"):
                        break
            finally:
                browser.close()
    except (PWError, OSError, RuntimeError) as exc:
        summary["stop_reason"] = "crash"
        summary["notes"] = f"worker-crash: {exc}"[:300]
        log.exception("worker crashed")
    finally:
        stop.set()
        release_rest(summary["stop_reason"])
        _finish(q, summary)
    log.info("done: claimed %d fetched %d failed %d released %d stop=%s", summary["claimed"],
             summary["fetched"], summary["failed"], summary["released"], summary["stop_reason"])
    return 0


def _report(q: Queue, summary: dict, open_ids: set, body: dict) -> None:
    try:
        q.report(body)
        open_ids.discard(body["queue_id"])
        summary["fetched" if body["outcome"] == "OK" else "failed"] += 1
    except requests.RequestException as exc:
        # The claim stays open; the server's stale reclaim returns it to PENDING
        # with no strike. The bytes are lost with this runner — by design, nothing
        # is written here.
        log.error("report failed for queue id %s: %s", body["queue_id"], exc)


def _finish(q: Queue, summary: dict) -> None:
    summary["finished_at"] = datetime.now(timezone.utc).isoformat()
    try:
        q.run_finished(dict(summary))
    except requests.RequestException as exc:
        log.warning("run-finished not recorded: %s", exc)


def main(argv: Optional[list[str]] = None) -> int:
    ap = argparse.ArgumentParser(description="RESA deed pull-queue worker")
    ap.add_argument("--probe", action="store_true", help="no server: time one list + one download, discard the PDF")
    args = ap.parse_args(argv)
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    if args.probe:
        return probe(env_int("CAPTCHA_TIMEOUT_S", 720), env_int("NAV_TIMEOUT_S", 60), env_int("POLL_S", 5))
    return run(args)


if __name__ == "__main__":
    sys.exit(main())
