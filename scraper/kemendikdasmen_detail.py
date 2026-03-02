"""
Phase 1.5: Fetch full school details from Kemendikdasmen detail API.
Enriches existing school records with phone, email, website, principal name,
student count, teacher count, accreditation, and more.

API: https://sekolah.data.kemendikdasmen.go.id/v1/sekolah-service/sekolah/full-detail/{sekolah_id}
"""

import time
import httpx
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type

from config import BATCH_SIZE
from db.mongo_client import mongo
from utils.logger import get_logger

logger = get_logger(__name__)

DETAIL_API_URL = (
    "https://sekolah.data.kemendikdasmen.go.id/v1/sekolah-service"
    "/sekolah/full-detail"
)

TASK_NAME = "kemendikdasmen_detail"


@retry(
    stop=stop_after_attempt(3),
    wait=wait_exponential(multiplier=2, min=2, max=20),
    retry=retry_if_exception_type((httpx.HTTPStatusError, httpx.ConnectError, httpx.ReadTimeout)),
    before_sleep=lambda rs: None,  # silent retry
)
def _fetch_detail(client: httpx.Client, sekolah_id: str) -> dict | None:
    """Fetch full detail for a single school by sekolah_id (UUID)."""
    # API requires uppercase UUID
    url = f"{DETAIL_API_URL}/{sekolah_id.upper()}"
    resp = client.get(url, timeout=20)
    resp.raise_for_status()
    data = resp.json()

    if data.get("status_code") != 200:
        return None

    return data.get("data", {})


def _extract_detail_fields(raw: dict) -> dict:
    """
    Extract useful fields from the full-detail API response
    and flatten them into a single dict for MongoDB update.
    """
    result = {}

    # --- School basic info ---
    sekolah_list = raw.get("sekolah", [])
    if sekolah_list:
        s = sekolah_list[0]
        result["detail_phone"] = _clean_value(s.get("nomor_telepon"))
        result["detail_email"] = _clean_value(s.get("email"))
        result["detail_website"] = _clean_website(s.get("website"))
        result["detail_address"] = _clean_value(s.get("alamat_jalan"))
        result["detail_kode_pos"] = _clean_value(s.get("kode_pos"))
        result["detail_akreditasi"] = _clean_value(s.get("akreditasi"))
        result["detail_lintang"] = s.get("lintang")
        result["detail_bujur"] = s.get("bujur")
        result["detail_status"] = _clean_value(s.get("status_sekolah"))
        result["detail_yayasan"] = _clean_value(s.get("yayasan"))
        result["detail_akses_internet"] = _clean_value(s.get("akses_internet"))
        result["detail_daya_listrik"] = s.get("daya_listrik")

    # --- Principal ---
    kepala = raw.get("kepala_sekolah", [])
    if kepala:
        result["detail_kepala_sekolah"] = kepala[0].get("nama")
    else:
        result["detail_kepala_sekolah"] = None

    # --- Student count ---
    rasio = raw.get("rasio_siswa", [])
    if rasio:
        r = rasio[0]
        result["detail_jumlah_siswa"] = r.get("jml_pd")
        result["detail_jumlah_rombel"] = r.get("jml_rombel")
        result["detail_siswa_laki"] = r.get("jml_pd_l")
        result["detail_siswa_perempuan"] = r.get("jml_pd_p")
    else:
        result["detail_jumlah_siswa"] = None
        result["detail_jumlah_rombel"] = None
        result["detail_siswa_laki"] = None
        result["detail_siswa_perempuan"] = None

    # --- Teacher count ---
    ptk = raw.get("ptk", [])
    if ptk:
        p = ptk[0]
        result["detail_guru_laki"] = p.get("ptk_guru_l")
        result["detail_guru_perempuan"] = p.get("ptk_guru_p")
    else:
        result["detail_guru_laki"] = None
        result["detail_guru_perempuan"] = None

    # --- Curriculum ---
    kurikulum = raw.get("kurikulum", [])
    if kurikulum:
        result["detail_kurikulum"] = kurikulum[0].get("kurikulum")
    else:
        result["detail_kurikulum"] = None

    # --- Room / facilities ---
    ruang = raw.get("ruang", [])
    if ruang:
        r = ruang[0]
        result["detail_ruang_kelas_baik"] = r.get("ruang_kelas_baik")
        result["detail_perpustakaan"] = r.get("ruang_perpustakaan_baik", 0)
        result["detail_lab_komputer"] = r.get("laboratorium_komputer_baik", 0)
    else:
        result["detail_ruang_kelas_baik"] = None
        result["detail_perpustakaan"] = None
        result["detail_lab_komputer"] = None

    # Mark as fetched
    result["detail_fetched"] = True

    return result


def _clean_value(val) -> str | None:
    """Return None for empty / useless values."""
    if val is None:
        return None
    s = str(val).strip()
    if s in ("", "-", "null", "None", "N/A"):
        return None
    return s


def _clean_website(val) -> str | None:
    """Return None for placeholder website values."""
    if val is None:
        return None
    s = str(val).strip()
    # Many schools have "http://" with no actual domain
    if s in ("", "-", "null", "None", "http://", "https://", "http:// ", "https:// "):
        return None
    if len(s) < 10:
        return None
    return s


def fetch_school_details(
    filters: dict | None = None,
    batch_size: int | None = None,
    delay: float = 0.3,
    resume: bool = True,
):
    """
    Fetch full detail for each school from the Kemendikdasmen detail API.
    Updates existing school records in MongoDB with rich data.

    Args:
        filters: MongoDB query to select which schools to fetch details for
        batch_size: Number of schools per batch
        delay: Delay between API calls (seconds)
        resume: Skip schools that already have detail_fetched=True
    """
    filters = filters or {}
    batch_size = batch_size or min(BATCH_SIZE, 200)

    logger.info("=" * 60)
    logger.info("PHASE 1.5: Kemendikdasmen Detail Fetcher")
    logger.info("=" * 60)

    # Build query: only fetch for schools without detail data
    query = {**filters}
    if resume:
        query["detail_fetched"] = {"$ne": True}

    total = mongo.get_school_count(query)
    logger.info("Schools needing detail fetch: %d", total)

    if total == 0:
        logger.info("All schools already have full details!")
        return

    processed = 0
    errors = 0
    found_phone = 0
    found_email = 0
    found_website = 0

    with httpx.Client(
        headers={"User-Agent": "IndonesiaSchoolScraper/1.0"},
        follow_redirects=True,
    ) as client:

        while True:
            # Fetch batch of schools without details
            schools = mongo.get_schools(query=query, limit=batch_size)

            if not schools:
                break

            logger.info(
                "Processing batch of %d schools (done: %d/%d)",
                len(schools), processed, total,
            )

            for school in schools:
                npsn = school.get("npsn", "?")
                name = school.get("nama", "?")
                sekolah_id = school.get("satuanPendidikanId")

                if not sekolah_id:
                    logger.warning("[%s] No satuanPendidikanId — skipping", npsn)
                    # Mark as fetched so we don't retry
                    mongo.update_school(npsn, {"detail_fetched": True, "detail_error": "no_uuid"})
                    processed += 1
                    continue

                try:
                    raw = _fetch_detail(client, sekolah_id)

                    if raw is None:
                        mongo.update_school(npsn, {"detail_fetched": True, "detail_error": "api_empty"})
                        processed += 1
                        continue

                    detail = _extract_detail_fields(raw)
                    mongo.update_school(npsn, detail)
                    processed += 1

                    # Track stats
                    if detail.get("detail_phone"):
                        found_phone += 1
                    if detail.get("detail_email"):
                        found_email += 1
                    if detail.get("detail_website"):
                        found_website += 1

                    if processed % 100 == 0:
                        pct = processed / total * 100
                        logger.info(
                            "[%d/%d] (%.1f%%) phones=%d emails=%d websites=%d",
                            processed, total, pct, found_phone, found_email, found_website,
                        )

                    # Save progress periodically
                    if processed % 500 == 0:
                        mongo.save_progress(TASK_NAME, {
                            "processed": processed,
                            "errors": errors,
                            "found_phone": found_phone,
                            "found_email": found_email,
                            "found_website": found_website,
                        })

                except Exception as e:
                    errors += 1
                    logger.debug("[%s] Error fetching detail: %s", npsn, e)
                    # Mark as fetched to avoid infinite retry
                    mongo.update_school(npsn, {
                        "detail_fetched": True,
                        "detail_error": str(e)[:200],
                    })
                    processed += 1

                time.sleep(delay)

    # Save final progress
    mongo.save_progress(TASK_NAME, {
        "processed": processed,
        "errors": errors,
        "found_phone": found_phone,
        "found_email": found_email,
        "found_website": found_website,
    })

    logger.info("=" * 60)
    logger.info("Detail fetch complete!")
    logger.info("Processed: %d | Errors: %d", processed, errors)
    logger.info("Found phones: %d | emails: %d | websites: %d", found_phone, found_email, found_website)
    logger.info("=" * 60)
