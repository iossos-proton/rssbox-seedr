import logging

from rssbox import (
    accounts,
    deta,
    downloads,
    files,
    scheduler,
    watchrss_database,
    workers,
)
from rssbox.config import Config

from .modules.download import Download
from .modules.watchrss import WatchRSS
from .seedr_client import SeedrClient

logger = logging.getLogger(__name__)


def on_new_entries(entries):
    logger.info(f"{len(entries)} new entries")
    for entry in entries:
        try:
            Download.from_entry(client=downloads, entry=entry).create()
        except Exception as e:
            logging.error(f"Error while adding download to database: {e}")

    return True


watchrss = WatchRSS(
    url=Config.RSS_URL,
    db=watchrss_database,
    callback=on_new_entries,
    check_confirmation=True,
)
watchrss.check()
scheduler.add_job(watchrss.check, "interval", minutes=1, id="watchrss_check")

seedr_client = SeedrClient(accounts, downloads, workers, scheduler, deta, files)
seedr_client.start()
scheduler.shutdown(wait=True)