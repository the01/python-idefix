# -*- coding: UTF-8 -*-

__author__ = "d01"
__email__ = "jungflor@gmail.com"
__copyright__ = "Copyright (C) 2013-23, Florian JUNG"
__license__ = "MIT"
__version__ = "0.3.0"
__date__ = "2023-06-21"
# Created: 2013-08-04 24:00

import datetime
import multiprocessing
import multiprocessing.pool
import os
import threading
from typing import Any, Dict, List, Literal, Optional, Tuple, Union

from floscraper.webscraper import WEBConnectException, WebScraper
from flotils import StartException, StartStopable
from flotils.loadable import load_file, Loadable, save_file
from requests.compat import urljoin

from .dao.mysql import SqlConnector
from .errors import DAOException, IDFXException, ValueException
from .model import Manga, User

ThreadingMode = Literal["pool", "threaded"]
""" Available threading modes """


def to_utc(dt: datetime.datetime) -> datetime.datetime:
    """ Transform to naive utc """
    if dt.tzinfo is not None:
        diff = dt.tzinfo.utcoffset(dt)

        if diff is not None:
            dt = (dt - diff).replace(tzinfo=None)

    return dt


class IDFXManga(Loadable, StartStopable):
    """ Class for retrieving manga info """

    def __init__(self, settings: Optional[Dict[str, Any]] = None) -> None:
        """ Constructor """
        if settings is None:
            settings = {}

        super().__init__(settings)

        self._read_path: Optional[str] = self.join_path_prefix(
            settings.get('manga_path', None)
        )
        self.dao: Optional[SqlConnector] = None

        if 'dao' in settings:
            self.dao = SqlConnector(settings['dao'])
        else:
            self.warning("No DAO")

        self.scrapers: List[WebScraper] = self._load_scrapers(
            settings.get('scrapers', [])
        )
        self._threading_mode: Optional[ThreadingMode] = None
        self.pool = None
        self.pool_size: int = settings.get('pool_size', multiprocessing.cpu_count())
        self.threading_mode: Optional[ThreadingMode] = settings.get(
            'threading_mode', None
        )

    def _load_scrapers(self, mixed: List[Union[str, WebScraper]]) -> List[WebScraper]:
        res = []

        for mix in mixed:
            if isinstance(mix, str):
                sett = {
                    'path_prefix': self._pre_path,
                    'settings_file': mix
                }
                mix = WebScraper(sett)

            res.append(mix)

        return res

    @property
    def threading_mode(self) -> Optional[ThreadingMode]:
        """ Threading mode in use """
        return self._threading_mode

    @threading_mode.setter
    def threading_mode(self, value: Optional[ThreadingMode]) -> None:
        """ Set threading_mode and setup """
        if self._threading_mode == value:
            return

        if self.pool:
            # TODO: stop
            self.pool = None
        if value == "pool":
            self.pool = multiprocessing.pool.ThreadPool(
                processes=self.pool_size
            )
        self._threading_mode = value

    def load_manga_file(
            self, user: Optional[User] = None, path: Optional[str] = None
    ) -> Tuple[User, List[Manga]]:
        """
        Load manga list from file
        Either using exact path or user and base path

        :param user: User to load for
        :param path: Base path / exact path to load from
        :return: Loaded user, mangas
        :raises ValueException: No user/manga data
        :raises IOError: Loading manga file failed
        """
        if not path:
            path = self._read_path

        if not path:
            path = ""

        user_id: Optional[str] = None

        if user:
            user_id = user.uuid

            if not user_id:
                user_id = f"{user.lastname}_{user.firstname}"

        if not path or not os.path.isfile(path):
            path = os.path.join(path, f"idfx_manga_{user_id}.yaml")

        if not os.path.isfile(path):
            self.error(f"File not found {path}")

            raise IOError(f"File not found {path}")

        try:
            loaded = load_file(path)
        except IOError:
            raise
        except Exception as e:
            self.exception("Failed to load manga file")

            raise IOError(e)

        if "user" not in loaded:
            raise ValueException("User missing")
        if "mangas" not in loaded:
            raise ValueException("Mangas missing")

        mangas: List[Manga] = []

        for mdict in loaded['mangas']:
            m: Manga = Manga.from_dict(mdict)

            if m.updated:
                m.updated = to_utc(m.updated)
            if m.created:
                m.created = to_utc(m.created)

            mangas.append(m)

        return User.from_dict(loaded['user']), mangas

    def save_manga_file(
            self,
            user: User,
            mangas: List[Manga],
            path: Optional[str] = None,
            readable: bool = False,
    ) -> None:
        """
        Save manga list to file

        :param user: User to save for
        :param mangas: List of mangas of user
        :param path: Base path to save to or _read_path (default: None)
        :param readable: Format content to be human readable (default: False)
        """
        if not path:
            path = self._read_path
        if not path:
            path = ""

        user_id = user.uuid

        if not user_id:
            user_id = f"{user.lastname}_{user.firstname}"

        temp_path = os.path.join(path, f"idfx_manga_{user_id}.json")

        if os.path.exists(os.path.basename(temp_path)):
            path = temp_path

        save_file(path, {
            'user': user.to_dict(),
            'mangas': [
                m.to_dict()
                for m in sorted(
                    mangas, key=lambda a: a.name if a.name is not None else ""
                )
            ]
        }, readable)

    def create_manga(
            self, user: User, manga: Manga
    ) -> Union[List[Manga], int]:
        """
        New manga for user

        :param user: User to create for
        :param manga: Manga to add to user
        :return: If multiple found, list of mangas to select or affected rows
        :rtype: list[idefix.model.Manga] | int
        :raises IDFXException: Failure
        :raises ValueException: Failure
        """
        self.debug(f"Adding manga '{manga}' for user '{user}'")

        if self.dao is None:
            raise IDFXException("No dao set")

        if not user or not user.uuid:
            raise ValueException("Invalid user")
        if not manga or not (manga.uuid or manga.name):
            raise ValueException("Invalid manga")

        if not manga.uuid:
            # Is unknown manga?
            mangas = self.dao.manga_get(Manga(name=manga.name))

            if not mangas:
                # Create new manga
                self.info(f"Creating new manga: {manga.name}")

                if self.dao.manga_create(manga) == 0:
                    raise DAOException("Manga not created")

                mangas = self.dao.manga_get(manga)

            if not mangas:
                raise ValueException("Manga not found")

            if len(mangas) > 1:
                return mangas

            manga.uuid = mangas[0].uuid
            manga.created = mangas[0].created
            manga.updated = mangas[0].updated

        res = self.dao.read_create(user, manga)

        if not res:
            raise DAOException("Manga read not created")

        self.dao.commit()

        return res

    def _do_scrap(  # noqa: C901
            self, scraper: WebScraper
    ) -> Optional[Dict[str, Any]]:
        """
        Perform thread-safe scrap step and update _upd

        :param scraper: Used scraper
        """
        try:
            res = scraper.scrap()
        except WEBConnectException as e:
            self.error(f"{e}")

            return None
        except Exception:
            self.exception("Failed to scrap")

            # raise?
            return None

        res.scraped = scraper.shrink(res.scraped)
        url: str = scraper.url
        url = url.removeprefix("https://")
        url = url.removeprefix("http://")
        url = url.removeprefix("www.")
        url = url.rstrip("/")

        if "chapter" not in res.scraped:
            self.error("Failed to retrieve mangas {}".format(url))

            return None

        mangas: Dict[str, Dict[str, Union[str, float, None]]] = {}

        for a in res.scraped['chapter']:
            name = link = ""
            number: Optional[float] = None

            # if value == list -> assumes correct one at index 0
            for val in a:
                if val == "number":
                    # Handle multiple chapters separatly
                    continue

                if isinstance(a[val], list) and len(a[val]) > 0:
                    a[val] = a[val][0]

            if "number" in a:
                numbers = a['number']

                if not isinstance(numbers, list):
                    numbers = [numbers]

                number = -1

                for n in numbers:
                    try:
                        num_candidate = float(f"{n}".strip())

                        if num_candidate > number:
                            number = num_candidate
                    except Exception:
                        # Not a number -> skip
                        pass

                if number == -1:
                    number = None

            if not number:
                # No number -> skip
                continue

            if "name" in a:
                name = f"{a['name']}".strip()
            if "link" in a:
                link = urljoin(scraper.url, f"{a['link']}".strip())

            key = name.lower()
            mangas.setdefault(key, {
                'chapter': 0, 'url': link, 'name': name
            })
            old = mangas[key]['chapter']

            if not isinstance(old, float) or not isinstance(old, int):
                continue

            if old < number:
                mangas[key]['chapter'] = number
                mangas[key]['url'] = link

        return mangas

    def create_index(self) -> Optional[Dict[str, Manga]]:
        """
        Use scrapers to create index of available mangas

        :return: Available mangas with chapter and location info
        """
        if not self.scrapers:
            self.error("No scrapers found")

            return None

        if self.threading_mode == "pool" and self.pool:
            self.debug("pool")
            results = self.pool.map(self._do_scrap, self.scrapers)
        elif self.threading_mode == "threaded":
            self.debug("threaded")
            results = []

            def fn(scraper):
                results.append(self._do_scrap(scraper))

            threads = [
                threading.Thread(target=fn, args=(scraper,))
                for scraper in self.scrapers
            ]
            for t in threads:
                t.start()
            for t in threads:
                t.join()
        else:
            # Not threaded
            self.debug("sequential")
            results = [
                self._do_scrap(scraper)
                for scraper in self.scrapers
            ]

        index: Dict[str, Manga] = {}

        for result in results:
            if not result:
                # Skip no mangas/errors
                continue

            for key, value in result.items():
                index.setdefault(
                    key, Manga(
                        name=value['name'], chapter=value['chapter']
                    )
                )
                m = index[key]
                m.updated = datetime.datetime.utcnow()

                if m.chapter and m.chapter == value['chapter']:
                    m.urls.append(value['url'])
                if not m.chapter or m.chapter < value['chapter']:
                    m.urls = [value['url']]
                    m.chapter = value['chapter']

        return index

    def check(
            self, user_mangas: List[Manga], web_mangas: Dict[str, Manga]
    ) -> List[Manga]:
        """
        Check for user mangas if there is a new chapter available

        :param user_mangas: Mangas the user is reading
        :param web_mangas: Mangas that are available on the web
        :return: Mangas with updates
        """
        res = []

        for m in user_mangas:
            if m.name is None or m.name.lower() not in web_mangas:
                # No update for this manga or no manga info
                continue

            w = web_mangas[m.name.lower()]

            if m.chapter is not None \
                    and w.chapter is not None \
                    and m.chapter < w.chapter:
                w.uuid = m.uuid
                res.append(w)

        return res

    def check_multiple(
            self,
            user_mangas: List[Tuple[
                Manga, List[Tuple[str, int]]
            ]],
            web_mangas: Dict[str, Manga],
    ) -> Dict[str, List[Manga]]:
        """
        Check several users for manga updates simultaneous

        :param user_mangas: Manga -> all users and latest chapters read
        :param web_mangas: Mangas that are available on the web
        :return: Dict with user -> List of new mangas
        """
        res: Dict[str, List[Manga]] = {}

        for manga, uuid_chapter in user_mangas:
            if manga.name is None or manga.name.lower() not in web_mangas:
                # No update for this manga or no manga info
                continue

            w = web_mangas[manga.name.lower()]
            w.uuid = manga.uuid

            for uuid, chapter in uuid_chapter:
                if uuid not in res:
                    res[uuid] = []

                if w.chapter is not None and chapter < w.chapter:
                    res[uuid].append(w)

        return res

    def start(self, blocking: bool = False) -> None:
        """
        Start interface

        :param blocking: Run start until done
        """
        if self.dao:
            try:
                self.dao.start(False)
            except Exception:
                self.exception("Failed to start dao")

                raise StartException("Dao start failed")
        else:
            self.warning("No dao to start")

        super().start(blocking)

    def stop(self) -> None:
        """ Stop interface """
        super().stop()

        if self.dao:
            try:
                self.dao.stop()
            except Exception:
                self.exception("Failed to stop dao")
        else:
            self.warning("No dao to stop")
