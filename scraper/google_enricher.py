"""
Phase 2: Advanced social media & contact enrichment.

Strategy:
1. First check data already obtained from the Kemendikdasmen detail API (phone, email, website).
2. Run multiple targeted Google searches with different query strategies.
3. Use regex to extract from raw HTML before parsing.
4. Send combined data to DeepSeek for structured extraction.
5. Merge all sources intelligently (govt data > DeepSeek > regex).
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

# ── Regex Patterns ────────────────────────────────────────────────────────────

# Indonesian phone numbers: +62xxx, 08xxx, 021-xxx, (021) xxx
PHONE_PATTERN = re.compile(
    r"(?:\+62|062|0)[\s\-.]?\d{2,4}[\s\-.]?\d{3,4}[\s\-.]?\d{2,5}"
)

# Instagram: full URL or @handle or "ig: handle"
INSTAGRAM_PATTERN = re.compile(
    r"(?:instagram\.com/|ig[:\s]+@?|instagram[:\s]+@?|@)([\w][\w.]{1,28}[\w])",
    re.IGNORECASE,
)

# WhatsApp: wa.me link or "wa: number" or "whatsapp: number"
WHATSAPP_PATTERN = re.compile(
    r"(?:wa\.me/|api\.whatsapp\.com/send\?phone=|whatsapp[:\s]*\+?|wa[:\s]*\+?|hubungi\s*(?:wa|whatsapp)[:\s]*\+?)([\d\s\-+]{8,20})",
    re.IGNORECASE,
)

# Facebook: full URL
FACEBOOK_PATTERN = re.compile(
    r"(?:facebook\.com/|fb\.com/)([\w.\-]+)",
    re.IGNORECASE,
)

# Website: any URL that's not a big social platform
WEBSITE_PATTERN = re.compile(
    r"https?://(?:www\.)?(?!(?:google|facebook|instagram|twitter|youtube|wa\.me|maps\.google|tiktok|linkedin|play\.google))"
    r"[a-zA-Z0-9\-]+\.[a-zA-Z]{2,}[^\s\"'<>]*",
    re.IGNORECASE,
)

# Look for email addresses
EMAIL_PATTERN = re.compile(
    r"[a-zA-Z0-9._%+\-]+@[a-zA-Z0-9.\-]+\.[a-zA-Z]{2,}",
    re.IGNORECASE,
)


# ── Web Scraping ──────────────────────────────────────────────────────────────

@retry(
    stop=stop_after_attempt(2),
    wait=wait_exponential(multiplier=1, min=1, max=10),
    retry=retry_if_exception_type((httpx.ConnectError, httpx.ReadTimeout)),
)
def _scrape_page(url: str) -> tuple[str, str]:
    """
    Scrape a page. Returns (raw_html, cleaned_text).
    We keep raw HTML for regex extraction (social links are often in HTML only).
    """
    try:
        with httpx.Client(
            follow_redirects=True,
            timeout=12,
            headers={
                "User-Agent": (
                    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                    "AppleWebKit/537.36 (KHTML, like Gecko) "
                    "Chrome/125.0.0.0 Safari/537.36"
                ),
                "Accept-Language": "id-ID,id;q=0.9,en;q=0.8",
            },
        ) as client:
            resp = client.get(url)
            resp.raise_for_status()

            raw_html = resp.text[:15000]  # Keep more HTML for regex matching

            soup = BeautifulSoup(resp.text, "html.parser")

            # Remove noise
            for tag in soup(["script", "style", "nav", "footer", "header", "noscript"]):
                tag.decompose()

            text = soup.get_text(separator=" ", strip=True)
            return raw_html, text[:6000]

    except Exception as e:
        logger.debug("Failed to scrape %s: %s", url, e)
        return "", ""


# ── Search Strategies ─────────────────────────────────────────────────────────

def _build_search_queries(name: str, location: str, npsn: str) -> list[str]:
    """
    Build multiple targeted search queries for different aspects.
    Returns a list of query strings to try.
    """
    clean_name = name.strip()

    queries = []

    # Strategy 1: General search with school name + location
    queries.append(f'"{clean_name}" {location}')

    # Strategy 2: Instagram specific
    queries.append(f'"{clean_name}" instagram')

    # Strategy 3: WhatsApp / contact specific
    queries.append(f'"{clean_name}" whatsapp OR kontak OR hubungi')

    # Strategy 4: Facebook specific (many Indonesian schools use FB)
    queries.append(f'"{clean_name}" facebook')

    # Strategy 5: Site-specific Instagram search
    queries.append(f'site:instagram.com "{clean_name}"')

    return queries


def _google_search_multi(queries: list[str], max_results_per_query: int = 3) -> list[str]:
    """
    Run multiple Google searches and collect unique URLs.
    Returns deduplicated list of URLs.
    """
    seen_urls = set()
    all_urls = []

    for query in queries:
        try:
            for url in google_search(query, num_results=max_results_per_query, lang="id"):
                if url not in seen_urls:
                    seen_urls.add(url)
                    all_urls.append(url)
        except Exception as e:
            logger.debug("Google search failed for '%s': %s", query[:50], e)

        # Small delay between different searches
        time.sleep(random.uniform(1.5, 3.0))

    return all_urls


# ── Extraction ────────────────────────────────────────────────────────────────

def _extract_from_html_and_text(raw_html: str, text: str) -> dict:
    """
    Extract social media handles and contact info from raw HTML + text.
    HTML is better for finding social links (<a href="instagram.com/...">).
    """
    data = {
        "instagram": None,
        "whatsapp": None,
        "facebook": None,
        "website": None,
        "contact_number": None,
        "email": None,
    }

    combined = raw_html + "\n" + text

    # Instagram - check HTML first (more reliable links)
    ig_matches = INSTAGRAM_PATTERN.findall(combined)
    for handle in ig_matches:
        handle = handle.strip().lower()
        # Filter out common false positives
        if handle not in ("p", "reel", "stories", "explore", "accounts", "about",
                          "help", "privacy", "terms", "press", "api", "developers",
                          "share", "direct", "login", "signup"):
            data["instagram"] = f"@{handle}"
            break

    # WhatsApp
    wa_matches = WHATSAPP_PATTERN.findall(combined)
    for num in wa_matches:
        clean = re.sub(r"[\s\-]", "", num.strip())
        if len(clean) >= 10:
            data["whatsapp"] = clean
            break

    # Facebook
    fb_matches = FACEBOOK_PATTERN.findall(combined)
    for handle in fb_matches:
        handle = handle.strip()
        if handle not in ("share", "sharer", "dialog", "plugins", "login",
                          "l.php", "tr", "watch", "groups", "pages",
                          "marketplace", "help", "privacy", "policies"):
            data["facebook"] = f"https://facebook.com/{handle}"
            break

    # Phone numbers
    phone_matches = PHONE_PATTERN.findall(combined)
    for phone in phone_matches:
        phone = phone.strip()
        if len(re.sub(r"\D", "", phone)) >= 9:
            data["contact_number"] = phone
            break

    # Website
    web_matches = WEBSITE_PATTERN.findall(combined)
    for url in web_matches:
        url = url.strip()
        # Filter out CDN, analytics, etc.
        skip_domains = ("cdn.", "ajax.", "analytics.", "fonts.", "static.",
                        "wp-content", ".js", ".css", ".png", ".jpg", ".gif",
                        "cloudflare", "googleapis", "gstatic")
        if not any(d in url.lower() for d in skip_domains):
            data["website"] = url
            break

    # Email
    email_matches = EMAIL_PATTERN.findall(combined)
    for email in email_matches:
        if not any(d in email.lower() for d in ("noreply", "example.com", "sentry", "webpack")):
            data["email"] = email
            break

    return data


def _extract_from_urls(urls: list[str]) -> dict:
    """
    Analyze the URLs themselves for social media profiles
    (sometimes the Google result URL IS the school's Instagram/Facebook).
    """
    data = {
        "instagram": None,
        "facebook": None,
    }

    for url in urls:
        url_lower = url.lower()

        if "instagram.com/" in url_lower and not data["instagram"]:
            match = re.search(r"instagram\.com/([\w.]+)", url)
            if match:
                handle = match.group(1)
                if handle not in ("p", "reel", "explore", "accounts", "stories"):
                    data["instagram"] = f"@{handle}"

        if ("facebook.com/" in url_lower or "fb.com/" in url_lower) and not data["facebook"]:
            match = re.search(r"(?:facebook|fb)\.com/([\w.\-]+)", url)
            if match:
                handle = match.group(1)
                if handle not in ("share", "sharer", "dialog", "plugins", "login"):
                    data["facebook"] = f"https://facebook.com/{handle}"

    return data


def _merge_all_results(
    govt_data: dict,
    url_data: dict,
    regex_data: dict,
    deepseek_data: dict,
) -> dict:
    """
    Merge results from all sources with priority:
    1. Government detail API (most reliable for phone/email/website)
    2. URL analysis (if Google returned an Instagram/FB profile directly)
    3. DeepSeek AI extraction (smart but may hallucinate)
    4. Regex extraction (fast but may have false positives)
    """
    merged = {}

    for key in ["instagram", "whatsapp", "facebook", "website", "contact_number", "email"]:
        merged[key] = (
            govt_data.get(key)
            or url_data.get(key)
            or deepseek_data.get(key)
            or regex_data.get(key)
        )

    return merged


# ── Main Enrichment Loop ──────────────────────────────────────────────────────

def enrich_schools(
    filters: dict | None = None,
    batch_size: int | None = None,
    delay: int | None = None,
    resume: bool = True,
):
    """
    Phase 2: Enrich school data with social media and contact info.

    Flow per school:
    1. Check existing govt detail data (phone, email from detail API)
    2. Google search with 5 different query strategies
    3. Scrape top results (HTML + text)
    4. Extract via regex from HTML (catches social links in <a> tags)
    5. Extract via DeepSeek AI from page text
    6. Analyze Google result URLs directly (Instagram/FB profile links)
    7. Merge all sources and save to MongoDB
    """
    batch_size = batch_size or BATCH_SIZE
    delay = delay or GOOGLE_SEARCH_DELAY
    filters = filters or {}

    logger.info("=" * 60)
    logger.info("PHASE 2: Advanced Social Media & Contact Enrichment")
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
    found_ig = 0
    found_wa = 0
    found_fb = 0
    found_web = 0
    found_phone = 0

    while True:
        schools = mongo.get_unenriched_schools(query=filters, limit=batch_size)

        if not schools:
            logger.info("No more unenriched schools. Done!")
            break

        logger.info(
            "Processing batch of %d schools (processed so far: %d)",
            len(schools), processed,
        )

        for school in schools:
            npsn = school.get("npsn", "unknown")
            name = school.get("nama", "Unknown School")
            kabupaten = school.get("namaKabupaten", "")
            provinsi = school.get("namaProvinsi", "")
            location = f"{kabupaten}, {provinsi}"

            try:
                logger.info("[%s] Enriching: %s (%s)", npsn, name, location)

                # ── Step 1: Collect government data already in DB ─────────
                govt_data = {
                    "contact_number": school.get("detail_phone"),
                    "email": school.get("detail_email"),
                    "website": school.get("detail_website"),
                    "instagram": None,
                    "whatsapp": None,
                    "facebook": None,
                }

                # If the govt phone looks like a mobile, it might be WA
                phone = school.get("detail_phone") or ""
                if phone and re.match(r"^(?:\+62|62|0)8\d{8,12}$", re.sub(r"[\s\-]", "", phone)):
                    govt_data["whatsapp"] = re.sub(r"[\s\-]", "", phone)

                # ── Step 2: Multi-strategy Google search ──────────────────
                queries = _build_search_queries(name, location, npsn)
                urls = _google_search_multi(queries, max_results_per_query=3)

                logger.debug("[%s] Found %d URLs from Google", npsn, len(urls))

                # ── Step 3: Analyze URLs themselves ───────────────────────
                url_data = _extract_from_urls(urls)

                # ── Step 4: Scrape top pages ──────────────────────────────
                all_html = ""
                all_text = ""
                snippets_for_ai = ""
                pages_scraped = 0

                for url in urls[:4]:
                    if any(d in url.lower() for d in ("instagram.com", "facebook.com", "fb.com")):
                        continue

                    raw_html, text = _scrape_page(url)
                    if raw_html:
                        all_html += f"\n<!-- {url} -->\n{raw_html}\n"
                        all_text += f"\n--- {url} ---\n{text}\n"
                        snippets_for_ai += f"{url}\n"
                        pages_scraped += 1

                    if pages_scraped >= 3:
                        break

                # ── Step 5: Regex extraction from HTML ────────────────────
                regex_data = _extract_from_html_and_text(all_html, all_text)

                # ── Step 6: DeepSeek AI extraction ────────────────────────
                deepseek_data = {}
                if all_text.strip():
                    try:
                        deepseek_data = extract_contact_info(
                            school_name=name,
                            location=location,
                            search_snippets=snippets_for_ai,
                            page_content=all_text,
                        )
                    except Exception as e:
                        logger.debug("[%s] DeepSeek error: %s", npsn, e)

                # ── Step 7: Merge all results ─────────────────────────────
                final_data = _merge_all_results(govt_data, url_data, regex_data, deepseek_data)
                final_data["school_name"] = name
                final_data["location"] = location

                # ── Step 8: Save to MongoDB ───────────────────────────────
                mongo.upsert_enrichment(npsn, final_data)
                processed += 1

                # Track stats
                if final_data.get("instagram"):
                    found_ig += 1
                if final_data.get("whatsapp"):
                    found_wa += 1
                if final_data.get("facebook"):
                    found_fb += 1
                if final_data.get("website"):
                    found_web += 1
                if final_data.get("contact_number"):
                    found_phone += 1

                has_data = any(final_data.get(k) for k in [
                    "instagram", "whatsapp", "facebook", "website", "contact_number", "email",
                ])
                if has_data:
                    logger.info(
                        "[%s] IG=%s | WA=%s | FB=%s | Web=%s | Phone=%s | Email=%s",
                        npsn,
                        final_data.get("instagram") or "-",
                        final_data.get("whatsapp") or "-",
                        final_data.get("facebook") or "-",
                        final_data.get("website") or "-",
                        final_data.get("contact_number") or "-",
                        final_data.get("email") or "-",
                    )
                else:
                    logger.info("[%s] No contact info found.", npsn)

                # Save progress
                mongo.save_progress(TASK_NAME, {
                    "last_npsn": npsn,
                    "processed": processed,
                    "errors": errors,
                    "found_ig": found_ig,
                    "found_wa": found_wa,
                    "found_fb": found_fb,
                    "found_web": found_web,
                    "found_phone": found_phone,
                })

            except Exception as e:
                errors += 1
                logger.error("[%s] Error enriching %s: %s", npsn, name, e)
                mongo.upsert_enrichment(npsn, {
                    "school_name": name,
                    "location": location,
                    "instagram": None,
                    "whatsapp": None,
                    "facebook": None,
                    "website": None,
                    "contact_number": None,
                    "email": None,
                    "error": str(e),
                })

            # Delay between schools
            jitter = random.uniform(0.5, 1.5)
            time.sleep(delay + jitter)

    # Final summary
    total_enriched = mongo.get_enriched_count()
    logger.info("=" * 60)
    logger.info("Enrichment complete!")
    logger.info("Processed: %d | Errors: %d", processed, errors)
    logger.info(
        "IG: %d | WA: %d | FB: %d | Web: %d | Phone: %d",
        found_ig, found_wa, found_fb, found_web, found_phone,
    )
    logger.info("Total enriched in DB: %d", total_enriched)
    logger.info("=" * 60)
