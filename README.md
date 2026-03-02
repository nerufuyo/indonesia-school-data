# Indonesia School Data Scraper 🇮🇩

Scrapes **552K+ Indonesian school data** from [Kemendikdasmen](https://data.belajar.id), fetches detailed info (phone, email, principal, accreditation) from the government detail API, and enriches with social media (Instagram, WhatsApp, Facebook) using multi-strategy Google Search + DeepSeek AI.

## Features

- **Phase 1** — Paginated scraping from Kemendikdasmen list API with configurable filters
- **Phase 1.5** — Fetch full school details (phone, email, principal, student count, etc.) from Kemendikdasmen detail API
- **Phase 2** — 5-strategy Google search + regex + DeepSeek AI extraction for social media & contacts
- **Resume support** — Automatically resumes from where it stopped
- **Batch processing** — Configurable batch sizes for large datasets
- **MongoDB storage** — Persistent storage with proper indexing and upsert
- **Excel export** — Formatted `.xlsx` with 34 columns, chunking for large datasets
- **Cloudflare R2** — Auto-upload exports to cloud storage
- **Deduplication** — NPSN-based dedup in batches, deterministic filenames, R2 overwrite

## Architecture

```
main.py (CLI)
├── scraper/
│   ├── kemendikdasmen.py        # Phase 1: School list from API
│   ├── kemendikdasmen_detail.py # Phase 1.5: Full detail per school
│   ├── google_enricher.py       # Phase 2: Multi-strategy Google + scraping
│   └── deepseek_client.py       # AI-powered structured extraction
├── db/
│   └── mongo_client.py          # MongoDB CRUD + progress tracking
├── export/
│   └── spreadsheet.py           # Excel export (34 columns)
├── storage/
│   └── r2_client.py             # Cloudflare R2 upload
├── utils/
│   ├── logger.py                # Console + file logging
│   └── retry.py                 # Retry decorators
└── config.py                    # Central configuration
```

## Setup

### 1. Prerequisites

- Python 3.11+
- MongoDB (local or remote)
- DeepSeek API key
- Cloudflare R2 credentials (optional, for cloud export)

### 2. Install Dependencies

```bash
cd indonesia-school-data
python -m venv venv

# Windows
venv\Scripts\activate

# macOS/Linux
source venv/bin/activate

pip install -r requirements.txt
```

### 3. Configure Environment

Create a `.env` file (or edit the existing one):

```env
MONGO_URI=mongodb://your-mongo-connection-string
MONGO_DB_NAME=indonesia_school_data
DEEPSEEK_API_KEY=your-deepseek-api-key
BATCH_SIZE=50
GOOGLE_SEARCH_DELAY=4
MAX_RETRIES=3

# Cloudflare R2 (optional)
R2_ACCOUNT_ID=your-account-id
R2_ACCESS_KEY_ID=your-access-key
R2_SECRET_ACCESS_KEY=your-secret-key
R2_BUCKET_NAME=your-bucket
R2_ENDPOINT_URL=https://your-account-id.r2.cloudflarestorage.com
```

## Usage

### Full Pipeline

```bash
# 1. Scrape school list from government API
python main.py scrape-schools --province 010000

# 2. Fetch detailed info (phone, email, principal, accreditation, etc.)
python main.py fetch-details --province 010000

# 3. Enrich with social media via Google + DeepSeek
python main.py enrich --province 010000 --batch-size 50 --delay 3

# 4. Export to Excel + upload to R2
python main.py export --province 010000
```

### Scrape Schools (Phase 1)

```bash
# Scrape ALL schools (552K+ records)
python main.py scrape-schools

# Scrape by province (DKI Jakarta)
python main.py scrape-schools --province 010000

# Scrape SD schools only
python main.py scrape-schools --edu-type sd

# Scrape with custom batch size
python main.py scrape-schools --province 010000 --batch-size 100

# Start fresh (ignore saved progress)
python main.py scrape-schools --no-resume
```

### Fetch School Details (Phase 1.5)

Calls the Kemendikdasmen detail API for each school to get phone, email, website, principal name, student/teacher counts, accreditation, curriculum, facilities, and coordinates.

```bash
# Fetch details for all schools in DB
python main.py fetch-details

# Fetch for a specific province
python main.py fetch-details --province 010000

# Custom delay between API calls (default 0.3s)
python main.py fetch-details --delay 0.5 --batch-size 200

# Re-fetch all (even already fetched)
python main.py fetch-details --no-resume
```

### Enrich with Social Media (Phase 2)

Runs 5 Google search strategies per school, scrapes top results, extracts from raw HTML via regex, and uses DeepSeek AI for structured extraction. Merges all sources with priority: government data > URL analysis > DeepSeek > regex.

```bash
# Enrich all scraped schools
python main.py enrich

# Enrich specific province
python main.py enrich --province 010000

# Custom delay between searches
python main.py enrich --delay 5 --batch-size 10
```

### Export to Excel

```bash
# Export all data (auto-uploads to R2)
python main.py export

# Export specific province
python main.py export --province 010000

# Export without R2 upload
python main.py export --province 010000 --no-upload

# Export to specific file
python main.py export --output my_data.xlsx
```

### Other Commands

```bash
# Check progress
python main.py status

# List province codes
python main.py list-provinces

# List education type codes
python main.py list-edu-types

# Reset progress
python main.py reset --task scrape     # Phase 1
python main.py reset --task details    # Phase 1.5
python main.py reset --task enrich     # Phase 2
python main.py reset --task all        # Everything
```

## Province Codes

| Code   | Province                    |
|--------|-----------------------------|
| 010000 | D.K.I. JAKARTA              |
| 020000 | JAWA BARAT                  |
| 030000 | JAWA TENGAH                 |
| 040000 | D.I. YOGYAKARTA             |
| 050000 | JAWA TIMUR                  |
| 060000 | ACEH                        |
| 220000 | BALI                        |
| ...    | Run `list-provinces` for all |

## MongoDB Collections

| Collection         | Description                                          |
|--------------------|------------------------------------------------------|
| `schools`          | School data from list API + detail fields             |
| `schools_enriched` | Social media & contact info from Google + DeepSeek    |
| `scrape_progress`  | Resume checkpoints for all phases                    |

## Export Columns (34)

| Category          | Fields                                                                  |
|-------------------|-------------------------------------------------------------------------|
| **Basic Info**    | NPSN, Nama, Bentuk Pendidikan, Status, Jenjang, Akreditasi, Pembina    |
| **Location**      | Provinsi, Kabupaten, Kecamatan, Desa, Alamat, Kode Pos, Lat/Lng       |
| **Government**    | Kepala Sekolah, Telepon, Email, Website (from detail API)              |
| **Social Media**  | Instagram, WhatsApp, Facebook, Website, Kontak, Email (from Google/AI) |
| **Statistics**    | Jumlah Siswa (L/P), Rombel, Guru (L/P), Kurikulum, Yayasan            |

## Data Flow

```
Kemendikdasmen List API ──→ MongoDB (schools)
         │
         ▼
Kemendikdasmen Detail API ──→ MongoDB (schools) + phone/email/principal
         │
         ▼
Google Search (5 strategies) ──→ Scrape HTML ──→ Regex + DeepSeek AI
         │
         ▼
Merge all sources ──→ MongoDB (schools_enriched)
         │
         ▼
MongoDB LEFT JOIN ──→ Excel (.xlsx) ──→ Cloudflare R2
```

## Enrichment Strategy

Phase 2 uses 5 Google search queries per school for maximum coverage:

1. `"School Name" location` — general info
2. `"School Name" instagram` — Instagram accounts
3. `"School Name" whatsapp OR kontak OR hubungi` — WhatsApp/contact
4. `"School Name" facebook` — Facebook pages
5. `site:instagram.com "School Name"` — direct IG profile search

Data is extracted from:
- **Raw HTML** (catches social links in `<a>` tags invisible in plain text)
- **DeepSeek AI** (structured extraction from page content)
- **URL analysis** (if Google returns an instagram.com URL directly)
- **Government detail API** (phone, email, mobile→WhatsApp detection)

## Rate Limiting

- **Kemendikdasmen List API**: 0.5s delay between pages
- **Kemendikdasmen Detail API**: 0.3s delay between calls (configurable)
- **Google Search**: 4–5.5s delay with random jitter (configurable)
- **DeepSeek API**: Exponential backoff on rate limits

## License

MIT
