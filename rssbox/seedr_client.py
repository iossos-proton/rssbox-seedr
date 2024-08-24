import logging
from datetime import datetime, timedelta, timezone
from ssl import SSLEOFError
from time import sleep

import nanoid
from apscheduler.schedulers.background import BackgroundScheduler
from deta import Deta, _Base
from pymongo import ReturnDocument
from pymongo.collection import Collection
from requests.exceptions import ConnectionError

from rssbox.enum import DownloadStatus, SeedrStatus
from rssbox.handlers.file_handler import FileHandler
from rssbox.handlers.worker_handler import WorkerHandler
from rssbox.modules.download import Download
from rssbox.modules.heartbeat import Heartbeat
from rssbox.modules.seedr import Seedr, SeedrFile, SeedrFolder, SeedrList

logger = logging.getLogger(__name__)


class SeedrClient:
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
        deta: Deta,
        files: _Base,
    ):
        self.id = nanoid.generate(alphabet="1234567890abcdef")
        self.accounts = accounts
        self.downloads = downloads
        self.workers = workers
        self.scheduler = scheduler
        self.deta = deta
        self.files = files

        logger.info(f"Initializing SeedrClient with ID: {self.id}")

        self.heartbeat = Heartbeat(self.id, self.workers, self.scheduler)
        self.worker_handler = WorkerHandler(
            self.workers, self.accounts, self.downloads, self.scheduler
        )
        self.file_handler = FileHandler(self.deta, self.files)

        self.worker_handler.clean_stale_seedrs_and_workers()

    def start(self):
        with self.heartbeat:
            self.begin_download()  # First download
            self.scheduler.add_job(self.begin_download, "interval", seconds=30, id="begin_download")
            self.check_downloads()
            self.begin_download()

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
            sort=[("priority", -1)],
            return_document=ReturnDocument.AFTER,
        )
        if not result:
            return None

        return self.get_seedr(result)

    def get_pending_download(self) -> Download | None:
        raw_download = self.downloads.find_one_and_update(
            {
                "status": DownloadStatus.PENDING.value,
                "$or": [
                    {"locked_by": {"$exists": False}},  # Not locked by any instance
                    {"locked_by": None},  # Explicitly not locked
                    {"locked_by": ""},  # Explicitly not locked
                ],
            },
            {"$set": {"locked_by": self.id}},
            return_document=ReturnDocument.AFTER,
        )

        if not raw_download:
            return None

        return Download(self.downloads, raw_download)

    def get_download_to_check(self) -> Seedr | None:
        locked_account = self.accounts.find_one_and_update(
            {
                "status": SeedrStatus.DOWNLOADING.value,
                "$or": [
                    {"locked_by": {"$exists": False}},  # Not locked by any instance
                    {"locked_by": None},  # Explicitly not locked
                    {"locked_by": ""},  # Explicitly not locked
                ],
            },  # Ensure it's still unlocked
            {
                "$set": {
                    "status": SeedrStatus.LOCKED.value,
                    "locked_by": self.id,
                    "last_checked_at": datetime.now(tz=timezone.utc),
                }
            },
            sort=[("last_checked_at", 1)],
            return_document=ReturnDocument.AFTER,
        )

        if locked_account:
            return self.get_seedr(locked_account)
        else:
            return None

    def check_downloads(self):
        limit = 3
        timeout_in_seconds = 8 * 60  # 8 minutes
        now = datetime.now(tz=timezone.utc)

        while True:
            if limit <= 0 or datetime.now(tz=timezone.utc) - now > timedelta(
                seconds=timeout_in_seconds
            ):
                break

            seedr = self.get_download_to_check()
            if not seedr:
                break

            download = seedr.download
            if not download:
                logger.info(
                    f"Seedr downloaded but no download found for {seedr.download_id} ({seedr.id})"
                )
                seedr.mark_as_idle()
                continue
            if not download.download_name:
                logger.info(
                    f"Seedr downloaded but no download name found for {seedr.download_id} ({seedr.id})"
                )
                seedr.reset()
                continue

            result = seedr.list()
            downloaded_file = self.find_seedr(download, result)

            if downloaded_file:
                logger.info(f"Downloaded {download.name} by {seedr.id}")
                try:
                    seedr.mark_as_uploading(self.id)
                    files_uploaded = self.file_handler.upload(downloaded_file)
                    if files_uploaded:
                        seedr.mark_as_completed()
                        limit -= 1
                    else:
                        logger.info(
                            f"No files uploaded for {download.name} by {seedr.id}"
                        )
                        seedr.update_status(SeedrStatus.DOWNLOADING)
                        sleep(5)
                except ConnectionError as error:
                    logger.error(
                        f"Failed to upload {download.name} to {seedr.id}: {error}"
                    )
                    seedr.mark_as_failed()
                except SSLEOFError as error:
                    logger.error(
                        f"Failed to upload {download.name} to {seedr.id}: {error}"
                    )
                    seedr.mark_as_failed(soft=True)
            else:
                if seedr.download_timeout():
                    logger.info(f"Download timed out for {download.name} by {seedr.id}")
                else:
                    is_downloading = False
                    for torrent in result.torrents:
                        if torrent.name == download.download_name:
                            logger.info(
                                f"Download in progress for {download.name} by {seedr.id} ({torrent.progress:.2f}%) ({seedr.time_taken})"
                            )
                            seedr.update_status(SeedrStatus.DOWNLOADING)
                            is_downloading = True
                            sleep(5)
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
            if folder.name == download.download_name:
                return folder

        for file in seedr_list.files:
            if file.name == download.download_name:
                return file

        return None

    def begin_download(self):
        while True:
            download = self.get_pending_download()
            if not download:
                break

            seedr = self.get_free_seedr()
            if not seedr:
                download.unlock()
                logger.info("No seedrs available")
                break

            response = seedr.add_download(download=download)
            if response.success:
                logger.info(f"Torrent {download.name} added to {seedr.id}")
            else:
                download.unlock()
                logger.error(
                    f"Failed to add {download.name} to {seedr.id}: {response.error}"
                )
