"""
Indonesia School Data Scraper — CLI Entry Point.

Usage:
    python main.py scrape-schools [--province CODE] [--edu-type TYPE] [--status 1|2] [--batch-size N]
    python main.py enrich [--province CODE] [--batch-size N] [--delay SECONDS]
    python main.py export [--province CODE] [--output PATH]
    python main.py status
    python main.py list-provinces
    python main.py list-edu-types
"""

import argparse
import os
import sys

from config import PROVINCES, EDUCATION_TYPES, STATUS_OPTIONS, BATCH_SIZE, GOOGLE_SEARCH_DELAY
from db.mongo_client import mongo
from utils.logger import get_logger

logger = get_logger("main")


def cmd_scrape_schools(args):
    """Phase 1: Scrape school list from Kemendikdasmen."""
    from scraper.kemendikdasmen import scrape_schools

    filters = {}
    if args.province:
        filters["kodeWilayah"] = args.province
    if args.edu_type:
        filters["bentukPendidikan"] = args.edu_type
    if args.status:
        filters["statusSatuanPendidikan"] = args.status
    if args.jalur:
        filters["jalurPendidikan"] = args.jalur
    if args.pembina:
        filters["pembina"] = args.pembina

    scrape_schools(
        filters=filters,
        batch_size=args.batch_size,
        resume=not args.no_resume,
    )


def cmd_fetch_details(args):
    """Phase 1.5: Fetch full details from Kemendikdasmen detail API."""
    from scraper.kemendikdasmen_detail import fetch_school_details

    filters = {}
    if args.province:
        filters["namaProvinsi"] = {"$regex": PROVINCES.get(args.province, ""), "$options": "i"}

    fetch_school_details(
        filters=filters,
        batch_size=args.batch_size,
        delay=args.delay,
        resume=not args.no_resume,
    )


def cmd_enrich(args):
    """Phase 2: Enrich schools with Google + DeepSeek data."""
    from scraper.google_enricher import enrich_schools

    filters = {}
    if args.province:
        filters["namaProvinsi"] = {"$regex": PROVINCES.get(args.province, ""), "$options": "i"}

    enrich_schools(
        filters=filters,
        batch_size=args.batch_size,
        delay=args.delay,
        resume=not args.no_resume,
    )


def cmd_export(args):
    """Export school data to Excel and upload to R2."""
    from export.spreadsheet import export_to_excel
    from storage.r2_client import r2_storage

    filters = {}
    province_name = ""
    if args.province:
        province_name = PROVINCES.get(args.province, "")
        filters["namaProvinsi"] = {"$regex": province_name, "$options": "i"}

    result = export_to_excel(
        output_path=args.output,
        filters=filters,
        province_name=province_name,
    )

    if result:
        # Collect all file paths
        files = result if isinstance(result, list) else [result]
        for f in files:
            logger.info("Exported: %s", f)

        # Upload to R2 unless --no-upload
        if not args.no_upload:
            from config import R2_EXPORT_PREFIX

            logger.info("Uploading to Cloudflare R2 (bucket: geniusai)...")
            for f in files:
                filename = os.path.basename(f)
                r2_key = f"{R2_EXPORT_PREFIX}/{filename}"

                # Delete old version in R2 first to avoid duplicates
                r2_storage.delete_file(r2_key)

                url = r2_storage.upload_file(f, r2_key=r2_key)
                if url:
                    logger.info("  ☁ Uploaded: %s", url)


def cmd_status(args):
    """Show current scraping status and progress."""
    summary = mongo.get_status_summary()

    print("\n" + "=" * 55)
    print("  INDONESIA SCHOOL DATA SCRAPER — STATUS")
    print("=" * 55)
    print(f"  Schools in DB:     {summary['total_schools']:>10,}")
    print(f"  With details:      {summary['total_with_details']:>10,}")
    print(f"  Enriched:          {summary['total_enriched']:>10,}")
    remaining = summary["total_schools"] - summary["total_enriched"]
    print(f"  Remaining:         {remaining:>10,}")

    scrape_prog = summary.get("scrape_progress")
    if scrape_prog:
        print(f"\n  Phase 1 Progress:")
        print(f"    Last offset:     {scrape_prog.get('last_offset', 'N/A')}")
        total = scrape_prog.get("total", 0)
        if total:
            pct = (scrape_prog.get("last_offset", 0) / total) * 100
            print(f"    Total:           {total:,}")
            print(f"    Progress:        {pct:.1f}%")

    enrich_prog = summary.get("enrich_progress")
    if enrich_prog:
        print(f"\n  Phase 2 Progress:")
        print(f"    Processed:       {enrich_prog.get('processed', 0):,}")
        print(f"    Errors:          {enrich_prog.get('errors', 0):,}")
        print(f"    Last NPSN:       {enrich_prog.get('last_npsn', 'N/A')}")

    print("=" * 55 + "\n")


def cmd_list_provinces(args):
    """List available province codes."""
    print("\n  Province Codes:")
    print("  " + "-" * 40)
    for code, name in sorted(PROVINCES.items(), key=lambda x: x[1]):
        print(f"  {code}  {name}")
    print()


def cmd_list_edu_types(args):
    """List available education type filters."""
    print("\n  Education Type Filters:")
    print("  " + "-" * 40)
    for code, name in EDUCATION_TYPES.items():
        print(f"  {code:<25} {name}")
    print()


def cmd_reset(args):
    """Reset progress tracking (allows re-scraping)."""
    if args.task == "scrape":
        mongo.clear_progress("kemendikdasmen_scrape")
        logger.info("Phase 1 progress reset.")
    elif args.task == "details":
        mongo.clear_progress("kemendikdasmen_detail")
        # Also clear the detail_fetched flag so they get re-fetched
        mongo.db["schools"].update_many({}, {"$unset": {"detail_fetched": ""}})
        logger.info("Phase 1.5 progress reset (detail_fetched flags cleared).")
    elif args.task == "enrich":
        mongo.clear_progress("google_enrich")
        logger.info("Phase 2 progress reset.")
    elif args.task == "all":
        mongo.clear_progress("kemendikdasmen_scrape")
        mongo.clear_progress("kemendikdasmen_detail")
        mongo.clear_progress("google_enrich")
        mongo.db["schools"].update_many({}, {"$unset": {"detail_fetched": ""}})
        logger.info("All progress reset.")


def main():
    parser = argparse.ArgumentParser(
        description="Indonesia School Data Scraper — Kemendikdasmen + Google + DeepSeek",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python main.py scrape-schools                           # Scrape all schools
  python main.py scrape-schools --province 010000         # Scrape DKI Jakarta only
  python main.py fetch-details --province 010000          # Fetch phone/email/principal
  python main.py enrich --batch-size 10 --delay 5         # Enrich with social media
  python main.py export --province 010000 --output out.xlsx
  python main.py status                                   # Check progress
  python main.py list-provinces                           # Show province codes
  python main.py reset --task scrape                      # Reset scrape progress
        """,
    )
    subparsers = parser.add_subparsers(dest="command", help="Command to run")

    # ── scrape-schools ────────────────────────────────────────────────────
    p_scrape = subparsers.add_parser("scrape-schools", help="Phase 1: Scrape from Kemendikdasmen")
    p_scrape.add_argument("--province", type=str, help="Province code (e.g., 010000)")
    p_scrape.add_argument("--edu-type", type=str, help="Education type (e.g., sd, smp, sma, smk)")
    p_scrape.add_argument("--status", type=str, choices=["1", "2"], help="1=Negeri, 2=Swasta")
    p_scrape.add_argument("--jalur", type=str, help="Jalur pendidikan (formal/non-formal)")
    p_scrape.add_argument("--pembina", type=str, help="Pembina (kemendikdasmen/kemenag)")
    p_scrape.add_argument("--batch-size", type=int, default=BATCH_SIZE, help=f"Records per page (default: {BATCH_SIZE})")
    p_scrape.add_argument("--no-resume", action="store_true", help="Start from beginning, ignore saved progress")
    p_scrape.set_defaults(func=cmd_scrape_schools)

    # ── fetch-details ─────────────────────────────────────────────────────
    p_detail = subparsers.add_parser("fetch-details", help="Phase 1.5: Fetch full school details (phone, email, principal, etc.)")
    p_detail.add_argument("--province", type=str, help="Filter by province code")
    p_detail.add_argument("--batch-size", type=int, default=200, help="Schools per batch (default: 200)")
    p_detail.add_argument("--delay", type=float, default=0.3, help="Seconds between API calls (default: 0.3)")
    p_detail.add_argument("--no-resume", action="store_true", help="Re-fetch all details")
    p_detail.set_defaults(func=cmd_fetch_details)

    # ── enrich ────────────────────────────────────────────────────────────
    p_enrich = subparsers.add_parser("enrich", help="Phase 2: Enrich via Google + DeepSeek")
    p_enrich.add_argument("--province", type=str, help="Filter by province code")
    p_enrich.add_argument("--batch-size", type=int, default=BATCH_SIZE, help=f"Schools per batch (default: {BATCH_SIZE})")
    p_enrich.add_argument("--delay", type=int, default=GOOGLE_SEARCH_DELAY, help=f"Seconds between Google searches (default: {GOOGLE_SEARCH_DELAY})")
    p_enrich.add_argument("--no-resume", action="store_true", help="Re-enrich all, including already enriched")
    p_enrich.set_defaults(func=cmd_enrich)

    # ── export ────────────────────────────────────────────────────────────
    p_export = subparsers.add_parser("export", help="Export to Excel (.xlsx) and upload to R2")
    p_export.add_argument("--province", type=str, help="Filter by province code")
    p_export.add_argument("--output", type=str, help="Output file path (default: auto-generated)")
    p_export.add_argument("--no-upload", action="store_true", help="Skip uploading to Cloudflare R2")
    p_export.set_defaults(func=cmd_export)

    # ── status ────────────────────────────────────────────────────────────
    p_status = subparsers.add_parser("status", help="Show scraping progress")
    p_status.set_defaults(func=cmd_status)

    # ── list-provinces ────────────────────────────────────────────────────
    p_list_prov = subparsers.add_parser("list-provinces", help="List province codes")
    p_list_prov.set_defaults(func=cmd_list_provinces)

    # ── list-edu-types ────────────────────────────────────────────────────
    p_list_edu = subparsers.add_parser("list-edu-types", help="List education type codes")
    p_list_edu.set_defaults(func=cmd_list_edu_types)

    # ── reset ─────────────────────────────────────────────────────────────
    p_reset = subparsers.add_parser("reset", help="Reset progress tracking")
    p_reset.add_argument("--task", type=str, choices=["scrape", "details", "enrich", "all"], default="all", help="Which task to reset")
    p_reset.set_defaults(func=cmd_reset)

    args = parser.parse_args()

    if not args.command:
        parser.print_help()
        sys.exit(1)

    # Connect to MongoDB
    try:
        mongo.connect()
    except Exception as e:
        logger.error("Cannot start: %s", e)
        sys.exit(1)

    try:
        args.func(args)
    except KeyboardInterrupt:
        logger.warning("\nInterrupted by user. Progress has been saved.")
    except Exception as e:
        logger.error("Unexpected error: %s", e, exc_info=True)
    finally:
        mongo.close()


if __name__ == "__main__":
    main()
