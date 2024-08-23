from datetime import datetime, timedelta
from seedrcc import Seedr as Seedrcc, Login
from typing import List
import pytz
from pymongo.collection import Collection
import logging
from rssbox import downloads, mongo_client
from rssbox.modules.download import Download
from rssbox.enum import SeedrStatus

logger = logging.getLogger(__name__)

class Seedr(Seedrcc):
    client: Collection
    id: str
    token: str
    status: SeedrStatus
    added_at: datetime | None
    download_id: str | None
    locked_by: str | None

    def __init__(self, client: Collection, account: dict):
        self.client = client
        self.id = account["_id"]
        self.token = account.get("token", None)
        self.status = SeedrStatus(account.get("status", SeedrStatus.IDLE.value))
        self.added_at = account.get("added_at", None)
        self.download_id = account.get("download_id", None)
        self.locked_by = account.get("locked_by", None)
        self.priority = account.get("priority", 0)
        self.__download = None

        if not self.token:
            self.token = self.get_token(account["password"])
        super().__init__(token=self.token, callbackFunc=self.callback_factory(self.id))

    def get_token(self, password: str):
        logging.info(f"Authenticating account {self.id}")
        seedr = Login(self.id, password)
        seedr.authorize()

        logging.debug(f"Updating token for account {self.id}")
        self.token = seedr.token
        self.save()
        return seedr.token
    
    def callback_factory(self, id):
        def callbackFunc(token):
            logger.info(f"Updating token for account {id}")
            self.token = token
            self.save()
        return callbackFunc

    def get_download_link(self, file: dict | str):
        if isinstance(file, dict):
            file = file["folder_file_id"]

        response = self.fetchFile(file)
        return response["url"]

    def list(self, folder_id: str = None):
        if folder_id:
            response = self.listContents(folder_id)
        else:
            response = self.listContents()

        return SeedrList(self, response)

    def purge(self):
        seedr_list = self.list()
        for folder in seedr_list.folders:
            self.deleteFolder(folder.id)
        for file in seedr_list.files:
            self.deleteFile(file.id)

    def add_download(self, download: Download):
        self.purge()

        response = self.addTorrent(torrentFile=download.url)
        response = SeedrAddDownloadResponse(response)

        if response.success:
            self.mark_as_downloading(download, response)
        else:
            self.mark_as_idle()
        return response
    
    def save(self):
        self.client.update_one(
            {"_id": self.id},
            {
                "$set": {
                    "token": self.token,
                    "status": self.status.value,
                    "added_at": self.added_at,
                    "download_id": self.download_id,
                    "locked_by": self.locked_by,
                    "priority": self.priority
                }
            }
        )
    
    def update_status(self, status: SeedrStatus):
        self.status = status
        if status in [SeedrStatus.DOWNLOADING, SeedrStatus.IDLE]:
            self.locked_by = None
        self.save()

    def mark_as_downloading(self, download: Download, response: "SeedrAddDownloadResponse"):
        self.download_id = download.id
        self.added_at = datetime.now(tz=pytz.utc)
        self.status = SeedrStatus.DOWNLOADING
        self.locked_by = None

        with mongo_client.start_session() as session:
            with session.start_transaction():
                self.save()
                self.download.mark_as_processing(response.name)

    def mark_as_idle(self):
        self.status = SeedrStatus.IDLE
        self.added_at = None
        self.download_id = None
        self.locked_by = None
        self.save()
    
    def mark_as_uploading(self, locked_by: str):
        self.locked_by = locked_by
        self.status = SeedrStatus.UPLOADING
        self.save()
    
    def mark_as_failed(self, soft=False):
        with mongo_client.start_session() as session:
            with session.start_transaction():
                self.mark_as_idle()
                self.download.mark_as_failed(soft=soft)

    def mark_as_completed(self):
        with mongo_client.start_session() as session:
            with session.start_transaction():
                self.mark_as_idle()
                self.download.delete()
    
    def reset(self):
        with mongo_client.start_session() as session:
            with session.start_transaction():
                self.mark_as_idle()
                self.download.mark_as_pending()
    
    def download_timeout(self, timeout: int = 60 * 60) -> bool:
        if self.added_at and self.added_at + timedelta(seconds=timeout) < datetime.now(tz=pytz.utc):
            self.mark_as_idle()
            return True

        return False
    
    def get_download(self) -> Download | None:
        if self.download_id:
            raw_download = downloads.find_one({"_id": self.download_id})
            if raw_download:
                return Download(downloads, raw_download)
        return None
    
    @property
    def download(self) -> Download | None:
        if not self.__download:
            self.__download = self.get_download()
        return self.__download


class SeedrAddDownloadResponse:
    code: int
    error: str | None
    name: str | None
    success: bool

    def __init__(self, dict: dict):
        print(dict)
        self.code = dict["code"]
        self.error = dict.get("error") or dict.get("result")
        self.name = dict.get("title")
        self.success = self.code == 200 and dict["result"] == True


class SeedrFile:
    name: str
    size: int
    folder_id: str
    id: int

    def __init__(self, client: Seedr, file: dict):
        self.client = client
        self.file = file
        self.name = file["name"]
        self.size = int(file["size"])
        self.folder_id = file["folder_id"]
        self.id = file["folder_file_id"]

    def get_download_link(self):
        return self.client.get_download_link(self.id)


class SeedrFolder:
    id: str
    name: str

    def __init__(self, client: Seedr, folder: dict):
        self.client = client
        self.folder = folder
        self.id = folder["id"]
        self.name = folder["name"]

    def list(self):
        return self.client.list(self.id)


class SeedrTorrent:
    id: str
    name: str
    progress: float
    size: int
    stopped: bool

    def __init__(self, torrent: dict):
        self.torrent = torrent
        self.id = torrent["id"]
        self.name = torrent["name"]
        self.progress = float(torrent["progress"])
        self.size = int(torrent["size"])
        self.stopped = bool(torrent["stopped"])


class SeedrList:
    folders: List[SeedrFolder]
    files: List[SeedrFile]
    torrents: List[SeedrTorrent]

    def __init__(self, client: Seedr, list: dict):
        self.folders = [SeedrFolder(client, folder) for folder in list["folders"]]
        self.files = [SeedrFile(client, file) for file in list["files"]]
        self.torrents = [SeedrTorrent(torrent) for torrent in list["torrents"]]