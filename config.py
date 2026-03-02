"""
Central configuration for the Indonesia School Data Scraper.
Loads settings from .env and provides constants, filter options, and defaults.
"""

import os
from dotenv import load_dotenv

load_dotenv()

# ─── MongoDB ───────────────────────────────────────────────────────────────────
MONGO_URI = os.getenv("MONGO_URI", "mongodb://localhost:27017")
MONGO_DB_NAME = os.getenv("MONGO_DB_NAME", "indonesia_school_data")

# Collection names
COL_SCHOOLS = "schools"
COL_ENRICHED = "schools_enriched"
COL_PROGRESS = "scrape_progress"

# ─── DeepSeek ──────────────────────────────────────────────────────────────────
DEEPSEEK_API_KEY = os.getenv("DEEPSEEK_API_KEY", "")
DEEPSEEK_BASE_URL = os.getenv("DEEPSEEK_BASE_URL", "https://api.deepseek.com")
DEEPSEEK_MODEL = "deepseek-chat"

# ─── Kemendikdasmen API ───────────────────────────────────────────────────────
KEMDIK_BASE_URL = (
    "https://api.data.belajar.id/data-portal-backend/v2"
    "/master-data/satuan-pendidikan"
)
KEMDIK_LIST_URL = f"{KEMDIK_BASE_URL}/daftar-data-induk/360"
KEMDIK_COUNT_URL = f"{KEMDIK_BASE_URL}/jumlah-data-induk/360"
KEMDIK_STATS_URL = (
    "https://api.data.belajar.id/data-portal-backend/v1"
    "/master-data/satuan-pendidikan/statistics/360"
)
KEMDIK_FILTERS_URL = f"{KEMDIK_BASE_URL}/../filters/satuan-pendidikan/360"

# ─── Cloudflare R2 Storage ─────────────────────────────────────────────────────
R2_ACCOUNT_ID = os.getenv("R2_ACCOUNT_ID", "")
R2_ACCESS_KEY_ID = os.getenv("R2_ACCESS_KEY_ID", "")
R2_SECRET_ACCESS_KEY = os.getenv("R2_SECRET_ACCESS_KEY", "")
R2_BUCKET_NAME = os.getenv("R2_BUCKET_NAME", "geniusai")
R2_ENDPOINT_URL = os.getenv(
    "R2_ENDPOINT_URL",
    "https://cd5c3852e5530592591826055a754455.r2.cloudflarestorage.com",
)
R2_EXPORT_PREFIX = "school-data/exports"  # folder prefix inside the bucket

# ─── Scraping Settings ────────────────────────────────────────────────────────
BATCH_SIZE = int(os.getenv("BATCH_SIZE", "50"))
GOOGLE_SEARCH_DELAY = int(os.getenv("GOOGLE_SEARCH_DELAY", "4"))
MAX_RETRIES = int(os.getenv("MAX_RETRIES", "3"))

# ─── Export ────────────────────────────────────────────────────────────────────
EXPORT_DIR = os.path.join(os.path.dirname(__file__), "exports")
MAX_ROWS_PER_FILE = 100_000  # split into multiple files if exceeded

# ─── Province / Region Codes ──────────────────────────────────────────────────
PROVINCES = {
    "010000": "D.K.I. JAKARTA",
    "020000": "JAWA BARAT",
    "030000": "JAWA TENGAH",
    "040000": "D.I. YOGYAKARTA",
    "050000": "JAWA TIMUR",
    "060000": "ACEH",
    "070000": "SUMATERA UTARA",
    "080000": "SUMATERA BARAT",
    "090000": "RIAU",
    "100000": "JAMBI",
    "110000": "SUMATERA SELATAN",
    "120000": "LAMPUNG",
    "130000": "KALIMANTAN BARAT",
    "140000": "KALIMANTAN TENGAH",
    "150000": "KALIMANTAN SELATAN",
    "160000": "KALIMANTAN TIMUR",
    "170000": "SULAWESI UTARA",
    "180000": "SULAWESI TENGAH",
    "190000": "SULAWESI SELATAN",
    "200000": "SULAWESI TENGGARA",
    "210000": "MALUKU",
    "220000": "BALI",
    "230000": "NUSA TENGGARA BARAT",
    "240000": "NUSA TENGGARA TIMUR",
    "250000": "PAPUA",
    "260000": "BENGKULU",
    "270000": "MALUKU UTARA",
    "280000": "BANTEN",
    "290000": "KEPULAUAN BANGKA BELITUNG",
    "300000": "GORONTALO",
    "310000": "KEPULAUAN RIAU",
    "320000": "PAPUA BARAT",
    "330000": "SULAWESI BARAT",
    "340000": "KALIMANTAN UTARA",
    "360000": "PAPUA TENGAH",
    "370000": "PAPUA SELATAN",
    "380000": "PAPUA PEGUNUNGAN",
    "390000": "PAPUA BARAT DAYA",
}

# ─── Education Type Filters ──────────────────────────────────────────────────
EDUCATION_TYPES = {
    # PAUD
    "paud": "Semua PAUD",
    "tk": "TK",
    "kb": "KB",
    "tpa": "TPA",
    "sps": "SPS",
    "ra": "RA",
    # DIKDAS
    "dikdas": "Semua DIKDAS",
    "sd": "SD",
    "smp": "SMP",
    "mi": "MI",
    "mts": "MTs",
    # DIKMEN
    "dikmen": "Semua DIKMEN",
    "sma": "SMA",
    "smk": "SMK",
    "ma": "MA",
    "mak": "MAK",
    "slb": "SLB",
    # DIKMAS
    "dikmas": "Semua DIKMAS",
    "kursus": "Kursus",
    "pkbm": "PKBM",
    "pondok-pesantren": "Pondok Pesantren",
}

STATUS_OPTIONS = {
    "1": "Negeri",
    "2": "Swasta",
}

JALUR_OPTIONS = {
    "formal": "Formal",
    "non-formal": "Non Formal",
}

PEMBINA_OPTIONS = {
    "kemendikdasmen": "KEMENTERIAN PENDIDIKAN DASAR DAN MENENGAH",
    "kemenag": "KEMENTERIAN AGAMA",
    "kemendiktisaintek": "KEMENTERIAN PENDIDIKAN TINGGI SAINS DAN TEKNOLOGI",
}
