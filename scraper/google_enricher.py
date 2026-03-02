"""
Phase 2: Enrich school data with contact info from Google Search + DeepSeek.
Searches Google for each school, scrapes top results, and uses DeepSeek AI
to extract Instagram, WhatsApp, Website, and Contact Number.
"""

import re
import time
import random
import httpx
from bs4 import BeautifulSoup
from googlesearch import search as google_search
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type

from config import BATCH_SIZE, GOOGLE_SEARCH_DELAY, MAX_RETRIES
from db.mongo_client import mongo
from scraper.deepseek_client import extract_contact_info
from utils.logger import get_logger

logger = get_logger(__name__)

TASK_NAME = "google_enrich"

# Regex patterns for quick extraction from raw HTML/text
PHONE_PATTERN = re.compile(
    r"(?:\+62|062|0)[\s\-]?\d{2,4}[\s\-]?\d{3,4}[\s\-]?\d{3,4}"
)
INSTAGRAM_PATTERN = re.compile(
    r"(?:instagram\.com|ig[:\s]*@?|instagram[:\s]*@?)\s*([\w.]+)", re.IGNORECASE
)
WHATSAPP_PATTERN = re.compile(
    r"(?:wa\.me/|whatsapp[:\s]*\+?|wa[:\s]*\+?)([\d\s\-+]+)", re.IGNORECASE
)
WEBSITE_PATTERN = re.compile(
    r"https?://(?:www\.)?(?!(?:google|facebook|instagram|twitter|youtube|wa\.me|maps\.google))"
    r"[a-zA-Z0-9\-]+\.[a-zA-Z]{2,}[^\s\"'<>]*",
    re.IGNORECASE,
)


@retry(
    stop=stop_after_attempt(2),
    wait=wait_exponential(multiplier=1, min=1, max=10),
    retry=retry_if_exception_type((httpx.ConnectError, httpx.ReadTimeout)),
)
def _scrape_page(url: str) -> str:
    """Scrape a web page and return cleaned text content."""
    try:
        with httpx.Client(
            follow_redirects=True,
            timeout=15,
            headers={
                "User-Agent": (
                    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                    "AppleWebKit/537.36 (KHTML, like Gecko) "
                    "Chrome/120.0.0.0 Safari/537.36"
                ),
            },
        ) as client:
            resp = client.get(url)
            resp.raise_for_status()

            soup = BeautifulSoup(resp.text, "html.parser")

            # Remove scripts and styles
            for tag in soup(["script", "style", "nav", "footer", "header"]):
                tag.decompose()

            text = soup.get_text(separator=" ", strip=True)

            # Truncate to avoid sending too much to DeepSeek
            return text[:5000]

    except Exception as e:
        logger.debug("Failed to scrape %s: %s", url, e)
        return ""


def _google_search_school(school_name: str, location: str) -> list[dict]:
    """
    Search Google for a school and return results.
    Returns list of dicts with 'url' and 'snippet'.
    """
    query = f'"{school_name}" {location} kontak instagram whatsapp'

    results = []
    try:
        for url in google_search(query, num_results=5, lang="id"):
            results.append({"url": url, "snippet": ""})
    except Exception as e:
        logger.warning("Google search failed for '%s': %s", school_name, e)

    return results


def _quick_extract_from_text(text: str) -> dict:
    """
    Quick regex-based extraction as a supplement to DeepSeek.
    Returns partial data that can be merged.
    """
    data = {
        "instagram": None,
        "whatsapp": None,
        "website": None,
        "contact_number": None,
    }

    ig_match = INSTAGRAM_PATTERN.search(text)
    if ig_match:
        data["instagram"] = ig_match.group(1).strip()

    wa_match = WHATSAPP_PATTERN.search(text)
    if wa_match:
        data["whatsapp"] = wa_match.group(1).strip()

    phone_match = PHONE_PATTERN.search(text)
    if phone_match:
        data["contact_number"] = phone_match.group(0).strip()

    web_match = WEBSITE_PATTERN.search(text)
    if web_match:
        data["website"] = web_match.group(0).strip()

    return data


def _merge_results(deepseek_data: dict, regex_data: dict) -> dict:
    """Merge DeepSeek and regex results, preferring DeepSeek when available."""
    merged = {}
    for key in ["instagram", "whatsapp", "website", "contact_number"]:
        merged[key] = deepseek_data.get(key) or regex_data.get(key)
    return merged


def enrich_schools(
    filters: dict | None = None,
    batch_size: int | None = None,
    delay: int | None = None,
    resume: bool = True,
):
    """
    Main entry point for Phase 2: enrich school data via Google + DeepSeek.

    Args:
        filters: MongoDB query filters for selecting schools to enrich
        batch_size: Number of schools to process per batch
        delay: Delay between Google searches (seconds)
        resume: Whether to skip already-enriched schools
    """
    batch_size = batch_size or BATCH_SIZE
    delay = delay or GOOGLE_SEARCH_DELAY
    filters = filters or {}

    logger.info("=" * 60)
    logger.info("PHASE 2: Google + DeepSeek Enrichment")
    logger.info("=" * 60)

    total_schools = mongo.get_school_count(filters)
    total_enriched = mongo.get_enriched_count()
    remaining = total_schools - total_enriched

    logger.info("Total schools in DB: %d", total_schools)
    logger.info("Already enriched: %d", total_enriched)
    logger.info("Remaining: %d", remaining)
    logger.info("Batch size: %d, Delay: %ds", batch_size, delay)

    if remaining <= 0:
        logger.info("All schools have been enriched!")
        return

    processed = 0
    errors = 0

    while True:
        # Fetch next batch of unenriched schools
        schools = mongo.get_unenriched_schools(query=filters, limit=batch_size)

        if not schools:
            logger.info("No more unenriched schools. Done!")
            break

        logger.info(
            "Processing batch of %d schools (processed so far: %d)",
            len(schools),
            processed,
        )

        for school in schools:
            npsn = school.get("npsn", "unknown")
            name = school.get("nama", "Unknown School")
            kabupaten = school.get("namaKabupaten", "")
            provinsi = school.get("namaProvinsi", "")
            location = f"{kabupaten}, {provinsi}"

            try:
                logger.info("[%s] Searching: %s (%s)", npsn, name, location)

                # Step 1: Google Search
                search_results = _google_search_school(name, location)

                # Step 2: Scrape top 2 pages
                all_text = ""
                snippets = ""
                for result in search_results[:2]:
                    url = result["url"]
                    page_text = _scrape_page(url)
                    all_text += f"\n--- {url} ---\n{page_text}\n"
                    snippets += f"{url}\n"

                # Step 3: Quick regex extraction
                regex_data = _quick_extract_from_text(all_text)

                # Step 4: DeepSeek extraction
                deepseek_data = extract_contact_info(
                    school_name=name,
                    location=location,
                    search_snippets=snippets,
                    page_content=all_text,
                )

                # Step 5: Merge results
                final_data = _merge_results(deepseek_data, regex_data)
                final_data["school_name"] = name
                final_data["location"] = location

                # Step 6: Save to MongoDB
                mongo.upsert_enrichment(npsn, final_data)
                processed += 1

                has_data = any(
                    final_data.get(k)
                    for k in ["instagram", "whatsapp", "website", "contact_number"]
                )
                if has_data:
                    logger.info(
                        "[%s] Found: IG=%s | WA=%s | Web=%s | Phone=%s",
                        npsn,
                        final_data.get("instagram") or "-",
                        final_data.get("whatsapp") or "-",
                        final_data.get("website") or "-",
                        final_data.get("contact_number") or "-",
                    )
                else:
                    logger.info("[%s] No contact info found.", npsn)

                # Save progress
                mongo.save_progress(TASK_NAME, {
                    "last_npsn": npsn,
                    "processed": processed,
                    "errors": errors,
                })

            except Exception as e:
                errors += 1
                logger.error("[%s] Error enriching %s: %s", npsn, name, e)
                # Save a placeholder so we don't retry endlessly
                mongo.upsert_enrichment(npsn, {
                    "school_name": name,
                    "location": location,
                    "instagram": None,
                    "whatsapp": None,
                    "website": None,
                    "contact_number": None,
                    "error": str(e),
                })

            # Delay between searches to avoid rate limiting
            jitter = random.uniform(0.5, 1.5)
            time.sleep(delay + jitter)

    # Final summary
    total_enriched = mongo.get_enriched_count()
    logger.info("=" * 60)
    logger.info("Enrichment complete!")
    logger.info("Total processed: %d", processed)
    logger.info("Total errors: %d", errors)
    logger.info("Total enriched in DB: %d", total_enriched)
    logger.info("=" * 60)
