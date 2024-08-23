import logging
from datetime import datetime, timezone
from time import mktime, struct_time
from typing import Callable, List

from feedparser import FeedParserDict, parse
from pymongo.collection import Collection

logger = logging.getLogger(__name__)


class WatchRSS:
    def __init__(
        self,
        url: str,
        db: Collection,
        callback: Callable[[List[FeedParserDict]], bool],
        id: str = None,
        last_saved_on: datetime | None = None,
        check_confirmation: bool = False,
    ):
        """
        :param url: RSS feed url
        :param callback: callback function to call when new entries are found (entries are passed as a list)
        :param id: id to use to save the last saved on timestamp (defaults to url)
        :param last_saved_on: last saved on timestamp (defaults to now if not provided and not saved in db)
        :param check_confirmation: whether to check for confirmation from the callback function (defaults to False)
        :param database_path: path to the database file (defaults to watchrss.data.json)
        """
        self.url = url
        self.id = id or url
        self.callback = callback
        self.check_confirmation = check_confirmation
        self.db = db
        if last_saved_on:
            self.update_last_saved_on(last_saved_on)
        elif not self.db.find_one({"_id": self.id}):
            self.update_last_saved_on(datetime.now(tz=timezone.utc))
        else:
            self.update_last_saved_on()

    def update_last_saved_on(self, new_last_saved_on: datetime | None = None):
        """
        Updates the last saved on timestamp

        :param new_last_saved_on: new last saved on timestamp to use
        """
        if new_last_saved_on:
            self.db.update_one(
                {"_id": self.id},
                {"$set": {"last_saved_on": new_last_saved_on}},
                upsert=True,
            )
            self.last_saved_on = new_last_saved_on
        else:
            result = self.db.find_one({"_id": self.id})
            self.last_saved_on = result["last_saved_on"] if result else datetime.now()

    def struct_to_datetime(self, struct: struct_time) -> datetime:
        """
        Converts a struct_time to a datetime
        """
        return datetime.fromtimestamp(mktime(struct)).replace(tzinfo=timezone.utc)

    def check(self):
        """
        Checks for new entries in the RSS feed
        """

        self.update_last_saved_on()

        parsed = parse(self.url)
        entries = [
            entry
            for entry in parsed.entries
            if self.struct_to_datetime(entry.published_parsed) > self.last_saved_on
        ]

        logger.debug("There are {} new entries".format(len(entries)))
        last_saved_on = self.struct_to_datetime(parsed.entries[0].published_parsed)

        if entries:
            try:
                confirm = self.callback(entries)
                if self.check_confirmation:
                    if confirm:
                        self.update_last_saved_on(last_saved_on)
                    else:
                        logger.warning(
                            "Callback returned False, not updating last_saved_on timestamp"
                        )
                else:
                    self.update_last_saved_on(last_saved_on)
            except Exception:
                logger.exception(
                    "Error while calling callback, not updating last_saved_on timestamp"
                )
