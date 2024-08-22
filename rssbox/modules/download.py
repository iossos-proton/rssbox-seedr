from pymongo.collection import Collection
from feedparser import FeedParserDict
from ..enum import DownloadStatus
from bson.objectid import ObjectId
from pymongo.errors import DuplicateKeyError
import logging

logger = logging.getLogger(__name__)

class Download:
    url: str
    name: str
    id: str
    status: DownloadStatus
    download_name: str | None
    locked_by: str | None

    def __init__(self, client: Collection, dict: dict):
        self.client = client
        self.url = dict["url"]
        self.name = dict["name"]
        self.id = dict["_id"]
        self.status = DownloadStatus(dict["status"])
        self.download_name = dict.get("download_name")
        self.locked_by = dict.get("locked_by")

    @property
    def dict(self):
        return {
            "url": self.url,
            "name": self.name,
            "status": self.status.value,
            "download_name": self.download_name,
            "locked_by": self.locked_by
        }

    def create(self):
        try:
            self.client.insert_one(self.dict)
        except DuplicateKeyError:
            logger.warning(f"Duplicate key error for download {self.id}")
            result = self.client.find_one({"url": self.url})
            self.id = result["_id"]
            self.url = result["url"]
            self.name = result["name"]
            self.status = DownloadStatus(result["status"])
            self.download_name = result.get("download_name")

    def save(self):
        self.client.update_one({"_id": self.id}, {"$set": self.dict}, upsert=True)

    def mark_as_processing(self, name: str):
        self.status = DownloadStatus.PROCESSING
        self.download_name = name
        self.locked_by = None
        self.save()

    def mark_as_pending(self):
        self.status = DownloadStatus.PENDING
        self.download_name = None
        self.locked_by = None
        self.save()
    
    def unlock(self):
        self.locked_by = None
        self.save()

    def delete(self):
        self.client.delete_one({"_id": self.id})

    @staticmethod
    def from_entry(client: Collection, entry: FeedParserDict):
        dict = {
            "url": entry.link,
            "name": entry.title,
            "status": DownloadStatus.PENDING.value,
            "_id": ObjectId(),
            "download_name": None
        }
        return Download(client=client, dict=dict)
