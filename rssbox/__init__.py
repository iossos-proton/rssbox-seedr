from dotenv import load_dotenv
from deta import Deta
import os
import logging
from pymongo import MongoClient
from bson.codec_options import CodecOptions
from apscheduler.schedulers.background import BackgroundScheduler

from rssbox.config import Config

load_dotenv()



if not os.path.exists(Config.DOWNLOAD_PATH):
    os.makedirs(Config.DOWNLOAD_PATH)


if os.path.exists(Config.LOG_FILE):
    with open(Config.LOG_FILE, "w") as f:
        pass


logging.basicConfig(
    level=Config.LOG_LEVEL,
    format="%(asctime)s - %(levelname)s - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler(Config.LOG_FILE),
    ],
)
logging.getLogger("apscheduler").setLevel(logging.WARNING)
logging.getLogger("deta").setLevel(logging.WARNING)
logging.getLogger("pymongo").setLevel(logging.WARNING)
logging.getLogger("urllib3").setLevel(logging.WARNING)

deta = Deta(Config.DETA_KEY)
files = deta.Base("files")


mongo_client = MongoClient(Config.MONGO_URL)
options = CodecOptions(tz_aware=True)

mongo = mongo_client.get_database("rssbox", codec_options=options)
accounts = mongo.get_collection("accounts", codec_options=options)

downloads = mongo.get_collection("downloads", codec_options=options)
downloads.create_index([("url", 1)], unique=True)

watchrss_database = mongo.get_collection("watchrss", codec_options=options)
workers = mongo.get_collection("workers", codec_options=options)

scheduler = BackgroundScheduler(timezone="UTC")
scheduler.start()
