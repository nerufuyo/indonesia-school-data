"""
Retry utilities for the scraper.
Provides reusable decorators using tenacity for API resilience.
"""

from tenacity import (
    retry,
    stop_after_attempt,
    wait_exponential,
    retry_if_exception_type,
    before_sleep_log,
)
import logging
import httpx

from config import MAX_RETRIES

logger = logging.getLogger(__name__)


def retry_on_http_error(func):
    """Decorator: retry on HTTP client errors with exponential backoff."""
    return retry(
        stop=stop_after_attempt(MAX_RETRIES),
        wait=wait_exponential(multiplier=2, min=2, max=60),
        retry=retry_if_exception_type(
            (httpx.HTTPStatusError, httpx.ConnectError, httpx.ReadTimeout)
        ),
        before_sleep=before_sleep_log(logger, logging.WARNING),
    )(func)


def retry_on_any_error(func):
    """Decorator: retry on any exception with exponential backoff."""
    return retry(
        stop=stop_after_attempt(MAX_RETRIES),
        wait=wait_exponential(multiplier=2, min=2, max=60),
        retry=retry_if_exception_type(Exception),
        before_sleep=before_sleep_log(logger, logging.WARNING),
    )(func)
