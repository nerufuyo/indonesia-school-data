"""
Phase 1: Scrape school data from Kemendikdasmen (data.belajar.id API).
Fetches school list with pagination and configurable filters, saves to MongoDB.
"""

import time
import httpx
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type

from config import (
    KEMDIK_LIST_URL,
    KEMDIK_STATS_URL,
    BATCH_SIZE,
    MAX_RETRIES,
)
from db.mongo_client import mongo
from utils.logger import get_logger

logger = get_logger(__name__)

TASK_NAME = "kemendikdasmen_scrape"


@retry(
    stop=stop_after_attempt(MAX_RETRIES),
    wait=wait_exponential(multiplier=2, min=2, max=30),
    retry=retry_if_exception_type((httpx.HTTPStatusError, httpx.ConnectError, httpx.ReadTimeout)),
    before_sleep=lambda rs: logger.warning("Retrying request (attempt %d)...", rs.attempt_number),
)
def _fetch_page(client: httpx.Client, offset: int, limit: int, filters: dict) -> dict:
    """Fetch a single page from the Kemendikdasmen API."""
    params = {
        "limit": limit,
        "offset": offset,
    }

    # Apply optional filters
    if filters.get("kodeWilayah"):
        params["kodeWilayah"] = filters["kodeWilayah"]
    if filters.get("bentukPendidikan"):
        params["bentukPendidikan"] = filters["bentukPendidikan"]
    if filters.get("statusSatuanPendidikan"):
        params["statusSatuanPendidikan"] = filters["statusSatuanPendidikan"]
    if filters.get("jalurPendidikan"):
        params["jalurPendidikan"] = filters["jalurPendidikan"]
    if filters.get("pembina"):
        params["pembina"] = filters["pembina"]
    if filters.get("jenisPendidikan"):
        params["jenisPendidikan"] = filters["jenisPendidikan"]

    resp = client.get(KEMDIK_LIST_URL, params=params, timeout=30)
    resp.raise_for_status()
    return resp.json()


@retry(
    stop=stop_after_attempt(MAX_RETRIES),
    wait=wait_exponential(multiplier=2, min=2, max=30),
    retry=retry_if_exception_type((httpx.HTTPStatusError, httpx.ConnectError, httpx.ReadTimeout)),
)
def _fetch_statistics(client: httpx.Client) -> dict:
    """Fetch overall statistics for progress display."""
    resp = client.get(KEMDIK_STATS_URL, timeout=30)
    resp.raise_for_status()
    return resp.json()


def _build_progress_key(filters: dict) -> str:
    """Build a unique progress key based on filters."""
    parts = [TASK_NAME]
    for key in sorted(filters.keys()):
        if filters[key]:
            parts.append(f"{key}={filters[key]}")
    return "|".join(parts)


def scrape_schools(
    filters: dict | None = None,
    batch_size: int | None = None,
    resume: bool = True,
):
    """
    Main entry point for Phase 1: scrape school list from Kemendikdasmen.

    Args:
        filters: Dict of API filters (kodeWilayah, bentukPendidikan, etc.)
        batch_size: Number of records per API call (default from config)
        resume: Whether to resume from last saved offset
    """
    filters = filters or {}
    batch_size = batch_size or BATCH_SIZE
    progress_key = _build_progress_key(filters)

    logger.info("=" * 60)
    logger.info("PHASE 1: Kemendikdasmen School Scraper")
    logger.info("=" * 60)
    logger.info("Filters: %s", filters if filters else "None (all schools)")
    logger.info("Batch size: %d", batch_size)

    # Determine starting offset (resume support)
    start_offset = 0
    if resume:
        progress = mongo.get_progress(progress_key)
        if progress and progress.get("last_offset") is not None:
            start_offset = progress["last_offset"] + batch_size
            logger.info(
                "Resuming from offset %d (previously saved progress)", start_offset
            )

    with httpx.Client(
        headers={"User-Agent": "IndonesiaSchoolScraper/1.0"},
        follow_redirects=True,
    ) as client:

        # First request to get total count
        logger.info("Fetching first page to determine total records...")
        first_page = _fetch_page(client, start_offset, batch_size, filters)

        total = first_page.get("meta", {}).get("total", 0)
        logger.info("Total schools matching filters: %d", total)

        if total == 0:
            logger.warning("No schools found with the given filters.")
            return

        # Process first page
        schools = first_page.get("data", [])
        if schools:
            count = mongo.upsert_schools_batch(schools)
            mongo.save_progress(progress_key, {
                "last_offset": start_offset,
                "total": total,
                "filters": filters,
            })
            logger.info(
                "[%d/%d] Saved %d schools (offset=%d)",
                min(start_offset + batch_size, total),
                total,
                count,
                start_offset,
            )

        # Paginate through remaining pages
        offset = start_offset + batch_size
        while offset < total:
            try:
                page_data = _fetch_page(client, offset, batch_size, filters)
                schools = page_data.get("data", [])

                if not schools:
                    logger.info("No more data at offset %d. Done.", offset)
                    break

                count = mongo.upsert_schools_batch(schools)
                mongo.save_progress(progress_key, {
                    "last_offset": offset,
                    "total": total,
                    "filters": filters,
                })

                progress_pct = min(offset + batch_size, total) / total * 100
                logger.info(
                    "[%d/%d] (%.1f%%) Saved %d schools (offset=%d)",
                    min(offset + batch_size, total),
                    total,
                    progress_pct,
                    count,
                    offset,
                )

                offset += batch_size

                # Small delay to be polite to the API
                time.sleep(0.5)

            except Exception as e:
                logger.error(
                    "Error at offset %d: %s. Progress saved. You can resume later.",
                    offset,
                    e,
                )
                break

    # Final summary
    db_count = mongo.get_school_count()
    logger.info("=" * 60)
    logger.info("Scraping complete!")
    logger.info("Total schools in database: %d", db_count)
    logger.info("=" * 60)
