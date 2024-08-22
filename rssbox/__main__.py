from .seedr_client import SeedrClient
from .modules.watchrss import WatchRSS
from rssbox import accounts, downloads, watchrss_database, workers, scheduler
from .modules.download import Download
from rssbox.config import Config
import logging

logger = logging.getLogger(__name__)


def on_new_entries(entries):
    logger.info(f"{len(entries)} new entries")
    for entry in entries:
        try:
            Download.from_entry(client=downloads, entry=entry).create()
        except Exception as e:
            logging.error(f"Error while adding download to database: {e}")

    return True


watchrss = WatchRSS(url=Config.RSS_URL, db=watchrss_database, callback=on_new_entries, check_confirmation=True)
watchrss.check()

seedr_client = SeedrClient(accounts, downloads, workers, scheduler)
seedr_client.start()