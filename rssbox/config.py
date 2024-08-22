from dotenv import load_dotenv
import os
from sys import argv

load_dotenv()

class Config:
    RSS_URL = os.environ["RSS_URL"]
    MONGO_URL = os.environ["MONGO_URL"]
    DETA_KEY = os.environ["DETA_KEY"]

    DEFAULT_FILTER_EXTENSIONS = "mkv,mp4,avi,mpg,mpeg,webm,flv,wmv,mov,m4v,3gp,ogv,mkv,avi,mpg,mpeg,webm,flv,wmv,mov,m4v,3gp,ogv"

    FILTER_EXTENSIONS = os.environ.get("FILTER_EXTENSIONS", DEFAULT_FILTER_EXTENSIONS)
    FILTER_EXTENSIONS = FILTER_EXTENSIONS.replace(" ", "").split(",")

    DOWNLOAD_PATH = os.environ.get("DOWNLOAD_PATH", "downloads")
    DOWNLOAD_PATH = os.path.abspath(DOWNLOAD_PATH)

    LOG_FILE = os.environ.get("LOG_FILE", "rssbox.log")
    DEBUG = "--debug" in argv or "--verbose" in argv or os.environ.get("LOG_LEVEL", "INFO") == "DEBUG"
    LOG_LEVEL = "DEBUG" if DEBUG else "INFO"