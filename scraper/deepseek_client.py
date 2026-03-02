"""
DeepSeek API client for extracting structured contact information
from raw text scraped from Google search results.

Uses the OpenAI-compatible API endpoint.
"""

import json
from openai import OpenAI
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type

from config import DEEPSEEK_API_KEY, DEEPSEEK_BASE_URL, DEEPSEEK_MODEL, MAX_RETRIES
from utils.logger import get_logger

logger = get_logger(__name__)

# Initialize the DeepSeek client (OpenAI-compatible)
_client: OpenAI | None = None


def _get_client() -> OpenAI:
    """Lazy initialization of the DeepSeek OpenAI client."""
    global _client
    if _client is None:
        _client = OpenAI(
            api_key=DEEPSEEK_API_KEY,
            base_url=f"{DEEPSEEK_BASE_URL}/v1",
        )
    return _client


SYSTEM_PROMPT = """You are a data extraction assistant. Your task is to extract contact and social media information for an Indonesian school from the provided text content.

Extract the following fields:
- instagram: Instagram handle or URL (e.g., @schoolname or https://instagram.com/schoolname)
- whatsapp: WhatsApp number or wa.me link (Indonesian format, e.g., +62812xxxx or https://wa.me/62812xxxx)
- facebook: Facebook page name or URL (e.g., https://facebook.com/schoolname)
- website: Official school website URL (NOT social media links, NOT google maps)
- contact_number: Phone/telephone number (landline or mobile, Indonesian format)
- email: Email address of the school

Rules:
1. Only extract information that is CLEARLY related to the school being queried.
2. If a field is not found, set it to null.
3. Return ONLY valid JSON, no markdown, no explanation.
4. For phone numbers, prefer the format with country code (+62...) or local (0...).
5. Instagram should be the handle (e.g., @school_name) or full URL.
6. WhatsApp should be the number or wa.me link.
7. Website should be a full URL (https://...).
8. Look carefully in the text for social media links, they are often in footer or sidebar sections.
9. Indonesian schools often list WhatsApp with "WA", "Hubungi", or "Kontak" markers.

Response format (JSON only):
{
    "instagram": "value or null",
    "whatsapp": "value or null",
    "facebook": "value or null",
    "website": "value or null",
    "contact_number": "value or null",
    "email": "value or null"
}"""


@retry(
    stop=stop_after_attempt(MAX_RETRIES),
    wait=wait_exponential(multiplier=2, min=3, max=30),
    retry=retry_if_exception_type((Exception,)),
    before_sleep=lambda rs: logger.warning(
        "DeepSeek retry (attempt %d)...", rs.attempt_number
    ),
)
def extract_contact_info(
    school_name: str,
    location: str,
    search_snippets: str,
    page_content: str = "",
) -> dict:
    """
    Use DeepSeek to extract structured contact info from raw text.

    Args:
        school_name: Name of the school
        location: Location string (kabupaten, province)
        search_snippets: Google search result snippets
        page_content: Scraped page text (truncated)

    Returns:
        Dict with keys: instagram, whatsapp, website, contact_number
    """
    # Build the user prompt with collected data
    user_prompt = f"""School: {school_name}
Location: {location}

--- Google Search Snippets ---
{search_snippets[:3000]}

--- Scraped Page Content ---
{page_content[:4000]}

Extract the contact and social media information for this school. Return JSON only."""

    client = _get_client()

    response = client.chat.completions.create(
        model=DEEPSEEK_MODEL,
        messages=[
            {"role": "system", "content": SYSTEM_PROMPT},
            {"role": "user", "content": user_prompt},
        ],
        temperature=0.1,
        max_tokens=300,
    )

    raw_text = response.choices[0].message.content.strip()

    # Parse the JSON response
    result = _parse_response(raw_text)

    logger.debug("DeepSeek extraction for %s: %s", school_name, result)
    return result


def _parse_response(raw_text: str) -> dict:
    """
    Parse DeepSeek's response into a clean dict.
    Handles markdown code blocks and malformed JSON gracefully.
    """
    default = {
        "instagram": None,
        "whatsapp": None,
        "facebook": None,
        "website": None,
        "contact_number": None,
        "email": None,
    }

    # Strip markdown code fences if present
    text = raw_text
    if "```json" in text:
        text = text.split("```json")[1].split("```")[0]
    elif "```" in text:
        text = text.split("```")[1].split("```")[0]

    text = text.strip()

    try:
        parsed = json.loads(text)
        # Ensure all expected keys exist
        for key in default:
            if key not in parsed or parsed[key] in ("", "null", "N/A", "-"):
                parsed[key] = None
        return {k: parsed.get(k) for k in default}
    except json.JSONDecodeError:
        logger.warning("Failed to parse DeepSeek response: %s", raw_text[:200])
        return default
