from enum import Enum


class DownloadStatus(Enum):
    PENDING = "PENDING"
    PROCESSING = "PROCESSING"
    COMPLETED = "COMPLETED"
    TIMEOUT = "TIMEOUT"
    ERROR = "ERROR"


class SeedrStatus(Enum):
    IDLE = "IDLE"
    PROCESSING = "PROCESSING"
    DOWNLOADING = "DOWNLOADING"
    LOCKED = "LOCKED"
    UPLOADING = "UPLOADING"
    COMPLETED = "COMPLETED"
    ERROR = "ERROR"
