"""
MongoDB client for the Indonesia School Data Scraper.
Handles connection, CRUD operations, and progress tracking.
"""

from pymongo import MongoClient, UpdateOne
from pymongo.errors import ConnectionFailure, BulkWriteError
from config import (
    MONGO_URI,
    MONGO_DB_NAME,
    COL_SCHOOLS,
    COL_ENRICHED,
    COL_PROGRESS,
)
from utils.logger import get_logger

logger = get_logger(__name__)


class MongoDB:
    """Singleton-style MongoDB wrapper with CRUD and progress tracking."""

    def __init__(self):
        self.client: MongoClient | None = None
        self.db = None

    # ── Connection ────────────────────────────────────────────────────────

    def connect(self):
        """Establish MongoDB connection and create indexes."""
        try:
            self.client = MongoClient(MONGO_URI, serverSelectionTimeoutMS=10_000)
            # Force a connection test
            self.client.admin.command("ping")
            self.db = self.client[MONGO_DB_NAME]
            self._ensure_indexes()
            logger.info("Connected to MongoDB: %s", MONGO_DB_NAME)
        except ConnectionFailure as e:
            logger.error("Failed to connect to MongoDB: %s", e)
            raise

    def close(self):
        """Close the MongoDB connection."""
        if self.client:
            self.client.close()
            logger.info("MongoDB connection closed.")

    def _ensure_indexes(self):
        """Create indexes for performance."""
        self.db[COL_SCHOOLS].create_index("npsn", unique=True)
        self.db[COL_SCHOOLS].create_index("namaProvinsi")
        self.db[COL_SCHOOLS].create_index("bentukPendidikan")
        self.db[COL_ENRICHED].create_index("npsn", unique=True)
        self.db[COL_PROGRESS].create_index("task_name", unique=True)

    # ── Schools (Phase 1) ─────────────────────────────────────────────────

    def upsert_schools_batch(self, schools: list[dict]) -> int:
        """
        Upsert a batch of school records by NPSN.
        Returns the number of upserted/modified documents.
        """
        if not schools:
            return 0

        operations = []
        for school in schools:
            operations.append(
                UpdateOne(
                    {"npsn": school["npsn"]},
                    {"$set": school},
                    upsert=True,
                )
            )

        try:
            result = self.db[COL_SCHOOLS].bulk_write(operations, ordered=False)
            count = result.upserted_count + result.modified_count
            return count
        except BulkWriteError as e:
            logger.warning("Bulk write had errors: %s", e.details.get("writeErrors", []))
            return e.details.get("nInserted", 0)

    def get_school_count(self, query: dict | None = None) -> int:
        """Get the count of schools matching a query."""
        return self.db[COL_SCHOOLS].count_documents(query or {})

    def get_schools(
        self,
        query: dict | None = None,
        skip: int = 0,
        limit: int = 0,
    ) -> list[dict]:
        """Retrieve schools with optional filtering and pagination."""
        cursor = self.db[COL_SCHOOLS].find(query or {})
        if skip:
            cursor = cursor.skip(skip)
        if limit:
            cursor = cursor.limit(limit)
        return list(cursor)

    # ── Enrichment (Phase 2) ──────────────────────────────────────────────

    def upsert_enrichment(self, npsn: str, data: dict) -> bool:
        """Upsert a single enrichment record by NPSN."""
        try:
            data["npsn"] = npsn
            self.db[COL_ENRICHED].update_one(
                {"npsn": npsn},
                {"$set": data},
                upsert=True,
            )
            return True
        except Exception as e:
            logger.error("Failed to upsert enrichment for NPSN %s: %s", npsn, e)
            return False

    def get_unenriched_schools(self, query: dict | None = None, limit: int = 50) -> list[dict]:
        """
        Get schools that have NOT been enriched yet.
        Performs a left-anti-join: schools NOT IN schools_enriched.
        """
        pipeline = [
            {"$lookup": {
                "from": COL_ENRICHED,
                "localField": "npsn",
                "foreignField": "npsn",
                "as": "enrichment",
            }},
            {"$match": {"enrichment": {"$size": 0}}},
            {"$project": {"enrichment": 0}},
        ]

        if query:
            pipeline.insert(0, {"$match": query})

        pipeline.append({"$limit": limit})
        return list(self.db[COL_SCHOOLS].aggregate(pipeline))

    def get_enriched_count(self, query: dict | None = None) -> int:
        """Get the count of enriched records."""
        return self.db[COL_ENRICHED].count_documents(query or {})

    # ── Joined data for export ────────────────────────────────────────────

    def get_schools_with_enrichment(
        self,
        query: dict | None = None,
        skip: int = 0,
        limit: int = 0,
    ) -> list[dict]:
        """
        Get schools LEFT-JOINED with their enrichment data.
        Returns merged documents.
        """
        pipeline = []

        if query:
            pipeline.append({"$match": query})

        pipeline.extend([
            {"$lookup": {
                "from": COL_ENRICHED,
                "localField": "npsn",
                "foreignField": "npsn",
                "as": "enrichment",
            }},
            {"$unwind": {
                "path": "$enrichment",
                "preserveNullAndEmptyArrays": True,
            }},
            {"$addFields": {
                "instagram": "$enrichment.instagram",
                "whatsapp": "$enrichment.whatsapp",
                "website": "$enrichment.website",
                "contact_number": "$enrichment.contact_number",
            }},
            {"$project": {"enrichment": 0, "_id": 0}},
        ])

        if skip:
            pipeline.append({"$skip": skip})
        if limit:
            pipeline.append({"$limit": limit})

        return list(self.db[COL_SCHOOLS].aggregate(pipeline, allowDiskUse=True))

    # ── Progress Tracking ─────────────────────────────────────────────────

    def save_progress(self, task_name: str, data: dict):
        """Save or update progress for a named task."""
        self.db[COL_PROGRESS].update_one(
            {"task_name": task_name},
            {"$set": {**data, "task_name": task_name}},
            upsert=True,
        )

    def get_progress(self, task_name: str) -> dict | None:
        """Retrieve progress for a named task."""
        return self.db[COL_PROGRESS].find_one(
            {"task_name": task_name},
            {"_id": 0},
        )

    def clear_progress(self, task_name: str):
        """Remove progress tracking for a task (reset)."""
        self.db[COL_PROGRESS].delete_one({"task_name": task_name})

    # ── Stats ─────────────────────────────────────────────────────────────

    def get_status_summary(self) -> dict:
        """Return a summary of the current database state."""
        return {
            "total_schools": self.get_school_count(),
            "total_enriched": self.get_enriched_count(),
            "scrape_progress": self.get_progress("kemendikdasmen_scrape"),
            "enrich_progress": self.get_progress("google_enrich"),
        }


# Module-level singleton
mongo = MongoDB()
