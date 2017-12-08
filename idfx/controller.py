# -*- coding: UTF-8 -*-
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function
from __future__ import unicode_literals

__author__ = "d01"
__email__ = "jungflor@gmail.com"
__copyright__ = "Copyright (C) 2013-17, Florian JUNG"
__license__ = "MIT"
__version__ = "0.2.0"
__date__ = "2017-07-12"
# Created: 2013-08-04 24:00

import threading
import multiprocessing
import multiprocessing.pool
import os
import datetime

from flotils.loadable import Loadable, save_file, load_file
from floscraper.webscraper import WebScraper, WEBConnectException
from requests.compat import urljoin
from dateutil.tz import tzutc

from .model import Manga, User
from .dao.mysql import SqlConnector
from .errors import AlreadyExistsException, DAOException, ValueException,\
    NoDAOException, IDFXException


def to_utc(dt):
    dt = dt.astimezone(tzutc())
    return dt.replace(tzinfo=None)


class IDFXManga(Loadable):
    """ Class for retrieving manga info """

    def __init__(self, settings=None):
        if settings is None:
            settings = {}
        super(IDFXManga, self).__init__(settings)

        self._read_path = self.join_path_prefix(
            settings.get('manga_path', None)
        )

        self.dao = None
        """ :type : None | idefix.dao.mysql.SqlConnector """
        if 'dao' in settings:
            self.dao = SqlConnector(settings['dao'])
        else:
            self.warning("No DAO")
        self.scrapers = settings.get('scrapers', None)
        pool_size = settings.get('pool_size', multiprocessing.cpu_count())

        self.pool = multiprocessing.pool.ThreadPool(
            processes=pool_size
        )

    def load_manga_file(self, user=None, path=None):
        """
        Load manga list from file
        Either using exact path or user and base path

        :param user: User to load for
        :type user: None | idefix.model.User
        :param path: Base path / exact path to load from
        :type path: None | str | unicode
        :return: Loaded user, mangas
        :rtype: idefix.model.Idefix, list[idefix.model.Manga]
        """
        if not path:
            path = self._read_path
        if not path:
            path = ""
        user_id = None
        if user:
            user_id = user.uuid
            if not user_id:
                user_id = "{}_{}".format(user.lastname, user.firstname)

        if not path or not os.path.isfile(path):
            path = os.path.join(path, "idfx_manga_{}.yaml".format(user_id))

        if not os.path.isfile(path):
            self.error("File not found {}".format(path))
            raise IOError("File not found {}".format(path))

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
        mangas = []
        for mdict in loaded['mangas']:
            m = Manga.from_dict(mdict)
            """ :type : idefix.model.Manga """
            if m.updated:
                m.updated = to_utc(m.updated)
            if m.created:
                m.created = to_utc(m.created)
            mangas.append(m)
        return User.from_dict(loaded['user']), mangas

    def save_manga_file(self, user, mangas, path=None, readable=False):
        """
        Save manga list to file

        :param user: User to save for
        :type user: idefix.model.User
        :param mangas: List of mangas of user
        :type mangas: list[idefix.model.Manga]
        :param path: Base path to save to or _read_path (default: None)
        :type path: None | str | unicode
        :param readable: Format content to be human readable (default: False)
        :type readable: bool
        :rtype: None
        """
        if not path:
            path = self._read_path
        if not path:
            path = ""
        user_id = user.uuid
        if not user_id:
            user_id = "{}_{}".format(user.lastname, user.firstname)
        temp_path = os.path.join(path, "idfx_manga_{}.json".format(user_id))

        if os.path.exists(os.path.basename(temp_path)):
            path = temp_path

        save_file(path, {
            'user': user.to_dict(),
            'mangas': [m.to_dict() for m in sorted(mangas, key=lambda a: a.name)]
        }, readable)

    def create_manga(self, user, manga):
        """
        New manga for user

        :param user: User to create for
        :type user: idefix.model.User
        :param manga: Manga to add to user
        :type manga: idefix.model.Manga
        :return: If multiple found, list of mangas to select or affected rows
        :rtype: list[idefix.model.Manga] | int
        :raises IDFXException: Failure
        :raises ValueException: Failure
        """
        self.debug("Adding manga '{}' for user '{}'".format(manga, user))
        if not user or not user.uuid:
            raise ValueException("Invalid user")
        if not manga or not (manga.uuid or manga.name):
            raise ValueException("Invalid manga")
        if not manga.uuid:
            # Is unknown manga?
            mangas = self.dao.manga_get(Manga(name=manga.name))
            if not mangas:
                # Create new manga
                self.info("Creating new manga: {}".format(manga.name))
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

    def _do_scrap(self, scraper):
        """
        Perform thread-safe scrap step and update _upd

        :param scraper: Used scraper
        :type scraper: WebScraper
        :rtype: None
        """
        try:
            res = scraper.scrap()
        except WEBConnectException as e:
            self.error("{}".format(e))
            return None
        except:
            self.exception("Failed to scrap")
            return None #raise?

        res.scraped = scraper.shrink(res.scraped)
        url = scraper.url
        url = url.lstrip("https://")
        url = url.lstrip("http://")
        url = url.lstrip("www.")
        url = url.rstrip("/")

        if "chapter" not in res.scraped:
            self.error("Failed to retrieve mangas {}".format(url))
            return None

        mangas = {}
        for a in res.scraped['chapter']:
            number = name = link = ""
            # if value == list -> assumes correct one at index 0
            for val in a:
                if isinstance(a[val], list) and len(a[val]) > 0:
                    a[val] = a[val][0]

            if "number" in a:
                try:
                    number = float("{}".format(a['number']).strip())
                except:
                    # Not a number -> skip
                    pass
            if not number:
                # No number -> skip
                continue

            if "name" in a:
                name = "{}".format(a['name']).strip()
            if "link" in a:
                link = urljoin(scraper.url, "{}".format(a['link']).strip())

            key = name.lower()
            mangas.setdefault(key, {
                'chapter': 0, 'url': link, 'name': name
            })
            old = mangas[key]['chapter']

            if old < number:
                mangas[key]['chapter'] = number
                mangas[key]['url'] = link
        return mangas

    def create_index(self):
        if not self.scrapers:
            self.error("No scrapers found")
            return None

        for i, scraper in enumerate(self.scrapers):
            if not hasattr(scraper, 'scrap'):
                try:
                    sett = {
                        'path_prefix': self._prePath,
                        'settings_file': scraper
                    }
                    scraper = WebScraper(sett)
                    self.scrapers[i] = scraper
                except:
                    self.exception(
                        "Failed to load scrapper {}".format(scraper)
                    )
                    continue
        results = self.pool.map(self._do_scrap, self.scrapers)
        index = {}
        """ :type : dict[str | unicode, idefix.model.Manga] """
        for result in results:
            if not result:
                # Skip no mangas/errors
                continue
            for key, value in result.items():
                index.setdefault(
                    key, Manga(name=value['name'], chapter=value['chapter'])
                )
                m = index[key]
                m.updated = datetime.datetime.utcnow()
                if m.chapter and m.chapter == value['chapter']:
                    m.urls.append(value['url'])
                if not m.chapter or m.chapter < value['chapter']:
                    m.urls = [value['url']]
                    m.chapter = value['chapter']
        return index

    def check(self, user_mangas, web_mangas):
        """
        Check for user mangas if there is a new chapter available

        :param user_mangas: Mangas the user is reading
        :type user_mangas: list[idefix.model.Manga]
        :param web_mangas: Mangas that are available on the web
        :type web_mangas: dict[str | unicode, idefix.model.Manga]
        :return: Mangas with updates
        :rtype: list[idefix.model.Manga]
        """
        res = []
        for m in user_mangas:
            if m.name.lower() not in web_mangas:
                # No update for this manga
                continue
            w = web_mangas[m.name.lower()]
            if m.chapter < w.chapter:
                w.uuid = m.uuid
                res.append(w)
        return res
