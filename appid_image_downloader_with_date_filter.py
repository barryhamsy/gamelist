#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Steam Library Image Downloader – Async Batch with Date Filter
- Reads D:\gamelist\branches_cache.json
- Downloads covers concurrently to D:\gamelist\{appid}.jpg
- Filters by added_on dates (supports multiple date formats)
- Always overwrites existing images (atomic replace)
- Skips only on HTTP 404 and continues
- Live progress and final summary

Usage:
  # Download all images
  python appid_image_downloader_with_date_filter.py

  # Download only for specific date
  python appid_image_downloader_with_date_filter.py --date 2026-01-01

  # Download for multiple dates
  python appid_image_downloader_with_date_filter.py --date 2026-01-01 --date 2026-01-02

  # Download for date range
  python appid_image_downloader_with_date_filter.py --from-date 2026-01-01 --to-date 2026-01-05

  # Download for today only
  python appid_image_downloader_with_date_filter.py --today

  # Download for yesterday
  python appid_image_downloader_with_date_filter.py --yesterday

  # Download for last N days
  python appid_image_downloader_with_date_filter.py --last-days 7

  # Custom concurrency
  python appid_image_downloader_with_date_filter.py --date 2026-01-01 -j 32
"""

import os
import sys
import json
import time
import asyncio
from pathlib import Path
from typing import List, Dict, Set
from datetime import datetime, date, timedelta

import aiohttp

BASE_URL = "https://steamcdn-a.akamaihd.net/steam/apps/{appid}/library_600x900.jpg"
TIMEOUT_SECS = 60

OUTPUT_DIR = Path(r"D:\gamelist")
BRANCHES_FILE = OUTPUT_DIR / "branches_cache.json"

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/120.0.0.0 Safari/537.36"
    )
}


def human_size(n: int) -> str:
    units = ["B", "KB", "MB", "GB", "TB"]
    s = float(n)
    i = 0
    while s >= 1024 and i < len(units) - 1:
        s /= 1024.0
        i += 1
    return f"{s:.2f} {units[i]}"


def parse_date_string(date_str: str) -> date:
    """Parse date string in format 'YYYY-MM-DD HH:MM:SS' or 'YYYY-MM-DD' to date object."""
    try:
        # Try parsing with time
        dt = datetime.strptime(date_str.split()[0], "%Y-%m-%d")
        return dt.date()
    except (ValueError, IndexError):
        try:
            # Try parsing date only
            dt = datetime.strptime(date_str, "%Y-%m-%d")
            return dt.date()
        except ValueError:
            return None


def load_appids_with_filter(json_path: Path, target_dates: Set[date] = None) -> List[str]:
    """
    Load appids from branches_cache.json, optionally filtered by dates.

    Args:
        json_path: Path to branches_cache.json
        target_dates: Set of date objects to filter by. If None, returns all appids.

    Returns:
        List of unique appids (as strings)
    """
    with open(json_path, "r", encoding="utf-8") as f:
        data = json.load(f)

    if not isinstance(data, list):
        raise ValueError("branches_cache.json should be a JSON array.")

    seen = set()
    appids: List[str] = []
    total_entries = len(data)
    filtered_count = 0

    for item in data:
        if isinstance(item, dict):
            appid = str(item.get("appid", "")).strip()

            # Skip if not a valid appid or already seen
            if not appid.isdigit() or appid in seen:
                continue

            # Filter by date if target_dates is provided
            if target_dates is not None:
                added_on_str = item.get("added_on", "")
                if added_on_str:
                    item_date = parse_date_string(added_on_str)
                    if item_date is None or item_date not in target_dates:
                        filtered_count += 1
                        continue
                else:
                    # Skip items without added_on date when filtering
                    filtered_count += 1
                    continue

            seen.add(appid)
            appids.append(appid)

    if target_dates is not None:
        print(
            f"Filtered: {filtered_count} entries excluded, {len(appids)} matches found from {total_entries} total entries\n")

    return appids


async def fetch_one(session: aiohttp.ClientSession, appid: str, out_dir: Path, sem: asyncio.Semaphore,
                    retries: int = 2) -> Dict:
    """
    Returns: {"appid": str, "status": "ok"|"notfound"|"error", "detail": str|None, "bytes": int}
    Always overwrites target file atomically.
    """
    url = BASE_URL.format(appid=appid)
    final_path = out_dir / f"{appid}.jpg"
    tmp_path = out_dir / f"{appid}.jpg.part"

    attempt = 0
    last_err = None

    while attempt <= retries:
        attempt += 1
        try:
            async with sem:
                async with session.get(url, allow_redirects=True) as resp:
                    if resp.status == 404:
                        try:
                            if tmp_path.exists():
                                tmp_path.unlink(missing_ok=True)
                        except Exception:
                            pass
                        return {"appid": appid, "status": "notfound", "detail": "HTTP 404", "bytes": 0}

                    resp.raise_for_status()

                    total = int(resp.headers.get("Content-Length", 0)) if resp.headers.get("Content-Length") else 0

                    # stream to temp, then atomic replace
                    bytes_written = 0
                    with open(tmp_path, "wb") as f:
                        async for chunk in resp.content.iter_chunked(131072):  # 128 KiB
                            if not chunk:
                                continue
                            f.write(chunk)
                            bytes_written += len(chunk)

                    # (optional) sanity check if server provided Content-Length
                    if total and bytes_written != total:
                        try:
                            tmp_path.unlink(missing_ok=True)
                        except Exception:
                            pass
                        return {"appid": appid, "status": "error", "detail": f"size_mismatch {bytes_written}/{total}",
                                "bytes": bytes_written}

                    # atomic overwrite
                    os.replace(tmp_path, final_path)

                    return {"appid": appid, "status": "ok", "detail": None, "bytes": bytes_written}

        except aiohttp.ClientResponseError as e:
            if e.status == 404:
                try:
                    tmp_path.unlink(missing_ok=True)
                except Exception:
                    pass
                return {"appid": appid, "status": "notfound", "detail": "HTTP 404", "bytes": 0}
            last_err = f"HTTP {e.status}"
        except (aiohttp.ClientError, asyncio.TimeoutError) as e:
            last_err = f"{type(e).__name__}: {e}"
        except Exception as e:
            last_err = f"Error: {e}"

        # clean partial on error before retry
        try:
            tmp_path.unlink(missing_ok=True)
        except Exception:
            pass

        if attempt <= retries:
            await asyncio.sleep(1.5 * attempt)  # simple backoff

    return {"appid": appid, "status": "error", "detail": last_err, "bytes": 0}


async def run_async(concurrency: int = 16, target_dates: Set[date] = None):
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    if not BRANCHES_FILE.exists():
        print(f"Error: branches_cache.json not found at {BRANCHES_FILE}")
        sys.exit(1)

    appids = load_appids_with_filter(BRANCHES_FILE, target_dates)
    total = len(appids)
    if total == 0:
        print("No AppIDs found matching the criteria.")
        return

    print("== Steam Library Image Downloader (Date Filter) ==")
    print(f"Save folder : {OUTPUT_DIR}")
    print(f"Source JSON : {BRANCHES_FILE}")
    if target_dates:
        dates_str = ", ".join(sorted([d.strftime("%Y-%m-%d") for d in target_dates]))
        print(f"Date filter : {dates_str}")
    else:
        print(f"Date filter : ALL (no filter)")
    print(f"AppIDs      : {total}")
    print(f"Concurrency : {concurrency}\n")

    timeout = aiohttp.ClientTimeout(total=TIMEOUT_SECS)
    connector = aiohttp.TCPConnector(limit=0)

    sem = asyncio.Semaphore(concurrency)
    start = time.perf_counter()

    ok = nf = err = 0
    bytes_total = 0
    notfound_list: List[str] = []
    error_list: List[str] = []

    async with aiohttp.ClientSession(headers=HEADERS, timeout=timeout, connector=connector) as session:
        tasks = [asyncio.create_task(fetch_one(session, a, OUTPUT_DIR, sem)) for a in appids]

        done = 0
        for coro in asyncio.as_completed(tasks):
            res = await coro
            done += 1

            status = res["status"]
            if status == "ok":
                ok += 1
                bytes_total += res.get("bytes", 0)
            elif status == "notfound":
                nf += 1
                notfound_list.append(res["appid"])
            else:
                err += 1
                error_list.append(f'{res["appid"]} ({res.get("detail")})')

            # Live one-line progress
            print(f"\rProgress: {done}/{total}  ✓ {ok}  •404 {nf}  ! err {err}", end="", flush=True)

    elapsed = time.perf_counter() - start
    print("\n\n== Summary ==")
    print(f"  ✓ Success     : {ok}")
    print(f"  • Not Found   : {nf}")
    print(f"  ! Errors      : {err}")
    print(f"  Data Download : {human_size(bytes_total)}")
    print(f"  Elapsed       : {elapsed:.1f}s  ({(ok) / elapsed if elapsed > 0 else 0:.2f} files/s)")

    if nf:
        print("\nNot found (404): " + ", ".join(notfound_list[:50]) + (" ..." if nf > 50 else ""))
    if err:
        print("\nErrors: ")
        for line in error_list[:50]:
            print("  - " + line)
        if err > 50:
            print("  ...")


def parse_args():
    import argparse
    p = argparse.ArgumentParser(
        description="Async Steam cover downloader with date filtering",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Download all images (no filter)
  %(prog)s

  # Download only for specific date
  %(prog)s --date 2026-01-01

  # Download for multiple specific dates
  %(prog)s --date 2026-01-01 --date 2026-01-02 --date 2026-01-05

  # Download for a date range
  %(prog)s --from-date 2026-01-01 --to-date 2026-01-05

  # Download for today only
  %(prog)s --today

  # Download for yesterday
  %(prog)s --yesterday

  # Download for last 7 days
  %(prog)s --last-days 7

  # Combine with custom concurrency
  %(prog)s --date 2026-01-01 -j 32
        """
    )

    # Date filtering options
    date_group = p.add_argument_group('Date Filtering Options')
    date_group.add_argument(
        "--date",
        action="append",
        help="Specific date(s) to download (format: YYYY-MM-DD). Can be used multiple times."
    )
    date_group.add_argument(
        "--from-date",
        help="Start date for range filter (format: YYYY-MM-DD)"
    )
    date_group.add_argument(
        "--to-date",
        help="End date for range filter (format: YYYY-MM-DD)"
    )
    date_group.add_argument(
        "--today",
        action="store_true",
        help="Download only games added today"
    )
    date_group.add_argument(
        "--yesterday",
        action="store_true",
        help="Download only games added yesterday"
    )
    date_group.add_argument(
        "--last-days",
        type=int,
        metavar="N",
        help="Download games added in the last N days"
    )

    # Performance options
    perf_group = p.add_argument_group('Performance Options')
    perf_group.add_argument(
        "-j", "--jobs",
        type=int,
        default=16,
        help="Number of concurrent downloads (default: 16)"
    )

    return p.parse_args()


def build_date_set(args) -> Set[date]:
    """Build a set of target dates from command line arguments."""
    target_dates = set()

    # Specific dates
    if args.date:
        for d in args.date:
            try:
                target_dates.add(datetime.strptime(d, "%Y-%m-%d").date())
            except ValueError:
                print(f"Warning: Invalid date format '{d}'. Use YYYY-MM-DD format.")
                sys.exit(1)

    # Date range
    if args.from_date or args.to_date:
        if not (args.from_date and args.to_date):
            print("Error: Both --from-date and --to-date must be specified for range filtering.")
            sys.exit(1)

        try:
            from_dt = datetime.strptime(args.from_date, "%Y-%m-%d").date()
            to_dt = datetime.strptime(args.to_date, "%Y-%m-%d").date()

            if from_dt > to_dt:
                print("Error: --from-date must be before or equal to --to-date")
                sys.exit(1)

            current = from_dt
            while current <= to_dt:
                target_dates.add(current)
                current += timedelta(days=1)
        except ValueError as e:
            print(f"Error: Invalid date format in range. Use YYYY-MM-DD format. {e}")
            sys.exit(1)

    # Today
    if args.today:
        target_dates.add(date.today())

    # Yesterday
    if args.yesterday:
        target_dates.add(date.today() - timedelta(days=1))

    # Last N days
    if args.last_days:
        if args.last_days < 1:
            print("Error: --last-days must be at least 1")
            sys.exit(1)

        today = date.today()
        for i in range(args.last_days):
            target_dates.add(today - timedelta(days=i))

    # Return None if no filters specified (download all)
    return target_dates if target_dates else None


def main():
    try:
        if os.name == "nt":
            sys.stdout.reconfigure(encoding="utf-8")
    except Exception:
        pass

    args = parse_args()

    # Build target dates from arguments
    target_dates = build_date_set(args)

    # Run the async downloader
    asyncio.run(run_async(concurrency=max(1, args.jobs), target_dates=target_dates))


if __name__ == "__main__":
    main()