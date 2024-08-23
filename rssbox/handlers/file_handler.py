import logging
import os
from datetime import datetime, timezone

import humanize
import requests
from deta import Deta, _Base

from rssbox.config import Config
from rssbox.modules.seedr import SeedrFile, SeedrFolder
from rssbox.utils import delete_file, md5hash

logger = logging.getLogger(__name__)


class FileHandler:
    def __init__(self, deta: Deta, files: _Base):
        self.deta = deta
        self.files = files

    def upload(self, seedr_object: SeedrFile | SeedrFolder) -> int:
        if isinstance(seedr_object, SeedrFile):
            return self.process_file(seedr_object)
        elif isinstance(seedr_object, SeedrFolder):
            return self.process_folder(seedr_object)

    def process_folder(self, folder: SeedrFolder):
        files_uploaded = 0

        folder_list = folder.list()
        for file in folder_list.files:
            count = self.process_file(file)
            files_uploaded += count
        for f in folder_list.folders:
            count = self.process_folder(f)
            files_uploaded += count

        return files_uploaded

    def process_file(self, file: SeedrFile):
        if self.check_extension(file.name):
            self.download_file(file)
            self.upload_file(file)
            return 1
        return 0

    def check_extension(self, name: str):
        _, ext = os.path.splitext(name)
        if ext[1:].lower() in Config.FILTER_EXTENSIONS:
            return True
        return False

    def download_file(self, file: SeedrFile) -> str:
        filepath = self.get_filepath(file)
        if os.path.exists(filepath) and os.path.getsize(filepath) == file.size:
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
        drive = self.deta.Drive(drive_name)
        logger.info(
            f"Uploading {file.name} ({humanize.naturalsize(file.size)}) to {drive_name}"
        )
        result = drive.put(name=file.name, path=filepath)

        self.files.insert(
            {
                "name": file.name,
                "size": file.size,
                "hash": drive_name,
                "created_at": datetime.now(tz=timezone.utc).isoformat(),
                "downloads_count": 0,
            }
        )
        logger.info(
            f"Uploaded {file.name} ({humanize.naturalsize(file.size)}) to {drive_name}"
        )

        self.remove_file(file)
        return result

    def get_filepath(self, file: SeedrFile) -> str:
        return os.path.join(self.get_filedir(file), file.name)

    def get_filedir(self, file: SeedrFile) -> str:
        return os.path.join(Config.DOWNLOAD_PATH, str(file.id))

    def remove_file(self, file: SeedrFile):
        filedir = self.get_filedir(file)
        delete_file(filedir)
