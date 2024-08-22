from datetime import datetime, timedelta
import os
from typing import List
from pymongo import ReturnDocument
import pytz
import requests
from pymongo.collection import Collection
from rssbox.modules.download import Download
from rssbox import deta, files
from rssbox.config import Config
from rssbox.enum import DownloadStatus, SeedrStatus
from rssbox.modules.heartbeat import Heartbeat
from rssbox.utils import delete_file, md5hash
from .modules.seedr import Seedr, SeedrFile, SeedrFolder, SeedrList
import logging
from apscheduler.schedulers.background import BackgroundScheduler
import humanize
import nanoid

logger = logging.getLogger(__name__)


class SeedrClient(Heartbeat):
    id: str
    accounts: Collection
    downloads: Collection
    workers: Collection
    scheduler: BackgroundScheduler

    def __init__(
        self,
        accounts: Collection,
        downloads: Collection,
        workers: Collection,
        scheduler: BackgroundScheduler,
    ):
        logger.info("Initializing SeedrClient")
        self.id = nanoid.generate(alphabet="1234567890abcdef")
        self.accounts = accounts
        self.downloads = downloads
        self.workers = workers
        self.scheduler = scheduler
        super().__init__(self.id, self.workers, self.scheduler)
        self.clean_stale_seedrs_and_workers()

    def start(self):
        self.begin_download()
        self.check_downloads()
        super().stop_heartbeat()

    def clean_stale_seedrs_and_workers(self):
        logger.info("Unlocking idle Seedrs and removing stale workers")

        timeout_period = timedelta(seconds=40)
        current_time = datetime.now(tz=pytz.utc)
        timeout_threshold = current_time - timeout_period

        # Find and delete stale workers, then capture their IDs
        stale_workers = self.workers.find(
            {"last_heartbeat": {"$lt": timeout_threshold}}, {"_id": 1}
        )

        stale_worker_ids = [worker["_id"] for worker in stale_workers]

        if stale_worker_ids:
            result = self.workers.delete_many({"_id": {"$in": stale_worker_ids}})
            logger.info(f"Removed {result.deleted_count} stale workers.")
        else:
            logger.info("No stale workers to remove.")

        # Find accounts that are in PROCESSING, UPLOADING, or DOWNLOAD_CHECKING status and are orphaned or idle
        pipeline = [
            {
                "$match": {
                    "status": {
                        "$in": [
                            SeedrStatus.PROCESSING.value,
                            SeedrStatus.UPLOADING.value,
                            SeedrStatus.DOWNLOAD_CHECKING.value,
                        ]
                    }
                }
            },
            {
                "$lookup": {
                    "from": "workers",
                    "localField": "locked_by",
                    "foreignField": "_id",
                    "as": "worker",
                }
            },
            {"$unwind": {"path": "$worker", "preserveNullAndEmptyArrays": True}},
            {
                "$match": {
                    "$or": [
                        {
                            "worker": {"$exists": False}
                        },  # Worker doesn't exist (orphaned)
                        {
                            "worker.last_heartbeat": {"$lt": timeout_threshold}
                        },  # Worker is stale
                        {
                            "locked_by": {"$in": stale_worker_ids}
                        },  # Locked by a stale worker
                    ]
                }
            },
            {"$project": {"_id": 1, "status": 1}},
        ]

        orphaned_or_idle_accounts = list(self.accounts.aggregate(pipeline))

        if orphaned_or_idle_accounts:
            for account in orphaned_or_idle_accounts:
                new_status = (
                    SeedrStatus.DOWNLOADING.value
                    if account["status"] == SeedrStatus.DOWNLOAD_CHECKING.value
                    else (
                        SeedrStatus.DOWNLOADING.value
                        if account["status"] == SeedrStatus.UPLOADING.value
                        else SeedrStatus.IDLE.value
                    )
                )

                # Update each account individually based on the condition
                self.accounts.update_one(
                    {"_id": account["_id"]},
                    {
                        "$set": {
                            "status": new_status,
                            "locked_by": None,
                        }
                    },
                )

            logger.info(
                f"Updated {len(orphaned_or_idle_accounts)} orphaned or idle Seedr accounts."
            )
        else:
            logger.info("No orphaned or idle Seedr accounts to update.")

    def get_seedr(self, account: dict) -> Seedr:
        return Seedr(client=self.accounts, account=account)

    def get_free_seedr(self) -> Seedr:
        result = self.accounts.find_one_and_update(
            {
                "$or": [
                    {"status": SeedrStatus.IDLE.value},
                    {"status": {"$exists": False}},
                    {"status": ""},
                ]
            },
            {"$set": {"status": SeedrStatus.PROCESSING.value, "locked_by": self.id}},
        )
        if not result:
            return None

        return self.get_seedr(result)

    def get_downloads_to_check(self) -> List[Seedr]:
        raw_accounts = self.accounts.find(
            {
                "status": SeedrStatus.DOWNLOADING.value,
                "$or": [
                    {"locked_by": {"$exists": False}},  # Not locked by any instance
                    {"locked_by": None},  # Explicitly not locked
                    {"locked_by": ""},  # Explicitly not locked
                ],
            }
        ).limit(5)

        locked_accounts = []

        for account in raw_accounts:
            locked_account = self.accounts.find_one_and_update(
                {
                    "_id": account["_id"],
                    "$or": [
                        {"locked_by": {"$exists": False}},  # Not locked by any instance
                        {"locked_by": None},  # Explicitly not locked
                        {"locked_by": ""},  # Explicitly not locked
                    ],
                },  # Ensure it's still unlocked
                {
                    "$set": {
                        "status": SeedrStatus.DOWNLOAD_CHECKING.value,
                        "locked_by": self.id,
                    }
                },
                return_document=ReturnDocument.AFTER,
            )
            if locked_account:
                locked_accounts.append(locked_account)

        return [self.get_seedr(account) for account in locked_accounts]

    def check_downloads(self):
        seedrs = self.get_downloads_to_check()

        for seedr in seedrs:
            download = seedr.download
            if not download:
                logger.info(
                    f"Seedr downloaded but no download found for {seedr.download_id} ({seedr.key})"
                )
                seedr.mark_as_idle()
                continue

            result = seedr.list()

            downloaded_file = self.find_seedr(download, result)
            if downloaded_file:
                logger.info(f"Downloaded {download.name} by {seedr.id}")
                self.upload(seedr, downloaded_file)
            else:
                if seedr.download_timeout():
                    logger.info(
                        f"Download timed out for {seedr.download_name} by {seedr.key}"
                    )
                else:
                    is_downloading = False
                    for torrent in result.torrents:
                        if torrent.name == download.name:
                            logger.info(
                                f"Download in progress for {download.name} by {seedr.id} ({torrent.progress}%)"
                            )
                            seedr.update_status(SeedrStatus.DOWNLOADING)
                            is_downloading = True
                            break

                    if not is_downloading:
                        logger.warning(
                            f"Download not found for {download.name} by {seedr.id}"
                        )
                        seedr.reset()

    def find_seedr(
        self, download: Download, seedr_list: SeedrList
    ) -> SeedrFile | SeedrFolder | None:
        for folder in seedr_list.folders:
            if folder.name == download.name:
                return folder

        for file in seedr_list.files:
            if file.name == download.name:
                return file

        return None

    def begin_download(self):
        for raw_download in self.downloads.find(
            {"status": DownloadStatus.PENDING.value}
        ):
            download = Download(self.downloads, raw_download)
            seedr = self.get_free_seedr()
            if not seedr:
                logger.info("No seedrs available")
                break

            response = seedr.add_download(download=download)
            if response.success:
                logger.info(f"Torrent {download.name} added to {seedr.id}")
            else:
                logger.error(
                    f"Failed to add {download.name} to {seedr.id}: {response.error}"
                )

    def upload(self, seedr: Seedr, seedr_object: SeedrFile | SeedrFolder):
        seedr.mark_as_uploading(self.id)

        if isinstance(seedr_object, SeedrFile):
            self.process_file(seedr_object)
        elif isinstance(seedr_object, SeedrFolder):
            self.process_folder(seedr_object)

        seedr.mark_as_completed()

    def process_folder(self, folder: SeedrFolder):
        folder_list = folder.list()
        for file in folder_list.files:
            self.process_file(file)
        for f in folder_list.folders:
            self.process_folder(f)

    def process_file(self, file: SeedrFile):
        if self.check_extension(file.name):
            self.download_file(file)
            self.upload_file(file)

    def check_extension(self, name: str):
        _, ext = os.path.splitext(name)
        if ext[1:].lower() in Config.FILTER_EXTENSIONS:
            return True
        return False

    def download_file(self, file: SeedrFile) -> str:
        filepath = self.get_filepath(file)
        if os.path.exists(filepath):
            return filepath

        os.makedirs(os.path.dirname(filepath), exist_ok=True)

        url = file.get_download_link()
        response = requests.get(url, stream=True)

        if response.status_code == 200:
            logger.info(
                f"Downloading {file.name} to {filepath} ({humanize.naturalsize(file.size)})"
            )
            with open(filepath, "wb") as f:
                for chunk in response.iter_content(chunk_size=1024):
                    if chunk:
                        f.write(chunk)

            logger.info(
                f"Downloaded {file.name} to {filepath} ({humanize.naturalsize(file.size)})"
            )
            return filepath

        return None

    def upload_file(self, file: SeedrFile):
        filepath = self.get_filepath(file)

        drive_name = md5hash(file.name)
        drive = deta.Drive(drive_name)
        logger.info(
            f"Uploading {file.name} ({humanize.naturalsize(file.size)}) to {drive_name}"
        )
        result = drive.put(name=file.name, path=filepath)

        files.insert(
            {
                "name": file.name,
                "size": file.size,
                "hash": drive_name,
                "created_at": datetime.now(tz=pytz.utc).isoformat(),
                "downloads_count": 0,
            }
        )
        logger.info(
            f"Uploaded {file.name} ({humanize.naturalsize(file.size)}) to {drive_name}"
        )

        delete_file(os.path.dirname(filepath))
        return result

    def get_filepath(self, file: SeedrFile) -> str:
        return os.path.join(Config.DOWNLOAD_PATH, str(file.id), file.name)
