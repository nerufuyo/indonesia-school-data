# Indonesia School Data Scraper 🇮🇩

Scrapes **552K+ Indonesian school data** from [Kemendikdasmen](https://data.belajar.id) and enriches it with contact information (Instagram, WhatsApp, Website, Phone) using Google Search + DeepSeek AI.

## Features

- **Phase 1**: Paginated scraping from Kemendikdasmen API with configurable filters
- **Phase 2**: Google search + DeepSeek AI extraction for social media & contacts
- **Resume support**: Automatically resumes from where it stopped
- **Batch processing**: Configurable batch sizes for large datasets
- **MongoDB storage**: Persistent storage with proper indexing
- **Excel export**: Formatted `.xlsx` export with chunking for large datasets
- **Configurable filters**: Province, education type, status, and more

## Architecture

```
main.py (CLI)
├── scraper/
│   ├── kemendikdasmen.py   # Phase 1: School list from API
│   ├── google_enricher.py  # Phase 2: Google + page scraping
│   └── deepseek_client.py  # AI-powered data extraction
├── db/
│   └── mongo_client.py     # MongoDB CRUD + progress tracking
├── export/
│   └── spreadsheet.py      # Excel export with formatting
├── utils/
│   ├── logger.py           # Console + file logging
│   └── retry.py            # Retry decorators
└── config.py               # Central configuration
```

## Setup

### 1. Prerequisites

- Python 3.11+
- MongoDB (local or remote)
- DeepSeek API key

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
```

## Usage

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

### Enrich with Contact Info (Phase 2)

```bash
# Enrich all scraped schools
python main.py enrich

# Enrich specific province
python main.py enrich --province 010000

# Custom delay between searches (be nice to Google)
python main.py enrich --delay 5 --batch-size 10
```

### Export to Excel

```bash
# Export all data
python main.py export

# Export specific province
python main.py export --province 010000

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

# Reset progress (to re-scrape)
python main.py reset --task scrape
python main.py reset --task enrich
python main.py reset --task all
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

| Collection        | Description                         |
|-------------------|-------------------------------------|
| `schools`         | Raw school data from Kemendikdasmen |
| `schools_enriched`| Contact info from Google + DeepSeek |
| `scrape_progress` | Resume checkpoints                  |

## Data Flow

```
Kemendikdasmen API ──→ MongoDB (schools)
         │
         ▼
   Google Search ──→ DeepSeek AI ──→ MongoDB (schools_enriched)
         │
         ▼
   MongoDB JOIN ──→ Excel (.xlsx)
```

## Rate Limiting

- **Kemendikdasmen API**: 0.5s delay between pages (polite scraping)
- **Google Search**: 4-5.5s delay with random jitter (configurable)
- **DeepSeek API**: Exponential backoff on rate limits

## License

MIT
