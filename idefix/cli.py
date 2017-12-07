# -*- coding: UTF-8 -*-
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function
from __future__ import unicode_literals

__author__ = "d01"
__email__ = "jungflor@gmail.com"
__copyright__ = "Copyright (C) 2017, Florian JUNG"
__license__ = "MIT"
__version__ = "0.1.0"
__date__ = "2017-12-01"
# Created: 2017-12-01 21:15

import logging
import logging.config
import argparse

from flotils import get_logger
from flotils.logable import default_logging_config
from requests.compat import urlparse

from .controller import IDFXManga


logger = get_logger()
logging.captureWarnings(True)


def setup_parser():
    """
    Create and init argument parser

    :return: Argument parser
    :rtype: argparse.ArgumentParser
    """
    parser = argparse.ArgumentParser(prog="idefix")
    parser.add_argument(
        "--debug", action="store_true",
        help="Use debug level output"
    )
    parser.add_argument(
        "--version", action="version", version="%(prog)s " + __version__
    )
    parser.add_argument(
        "-s", "--settings", nargs="?",
        help="Settings file"
    )
    parser.add_argument(
        "--pre_path", nargs="?", default=None,
        help="Base path to use"
    )
    parser.add_argument(
        "--manga_file", nargs="?", default=None,
        help="Manga file to use"
    )
    parser.add_argument(
        "-c", "--check", nargs="?", const=True, default=False,
        help="Check for new mangas"
    )
    parser.add_argument(
        "--chapter", nargs="?", type=int, default=None,
        help="Selected chapter"
    )
    parser.add_argument(
        "-r", "--read", nargs="?", const="", default=None,
        help="Run a show update; Default: all - check for all shows or "
             "specify a show"
    )
    parser.add_argument(
        "--sync", action="store_true",
        help="Sync with database"
    )

    return parser


def format_mangas(mangas):
    """

    :param mangas:
    :type mangas: list[idefix.model.Manga]
    :return:
    :rtype: str | unicode
    """
    if not mangas:
        return ""

    res = []

    for m in mangas:
        url_parts = ""
        if m.urls:
            url_parts = ": " + ",".join([urlparse(u).netloc for u in m.urls])
        int_chap = m.chapter
        if int_chap:
            try:
                int_chap = int(m.chapter)
            except:
                int_chap = m.chapter
        if int_chap != m.chapter:
            int_chap = m.chapter
        res.append("{} ({}){}".format(
            m.name, int_chap, url_parts
        ))
    return "\n".join(res)


def main():
    logging.config.dictConfig(default_logging_config)
    logging.getLogger().setLevel(logging.INFO)

    parser = setup_parser()
    args = parser.parse_args()

    if args.debug:
        logging.getLogger().setLevel(logging.DEBUG)
    instance = IDFXManga({
        'settings_file': args.settings,
        'path_prefix': args.pre_path
    })

    # instance.start(blocking=False)
    user, mangas = instance.load_manga_file(path=args.manga_file)
    new = None
    dirty = False

    if args.check:
        new = instance.check(mangas, instance.create_index())
        # logger.debug(new)
        if new:
            logger.info("\n" + format_mangas(new))
        else:
            logger.info("No updates")
    if args.read is not None:
        if new is None:
            new = instance.check(mangas, instance.create_index())
        if new:
            for m in mangas:
                for n in new:
                    if not((n.name or m.name)
                           and n.name.lower() == m.name.lower()):
                        if not((n.uuid or m.uuid) and n.uuid == m.uuid):
                            continue
                    m.chapter = n.chapter
                    m.updated = n.updated
                    m.urls = n.urls
                    dirty = True
                    logger.info("Read " + format_mangas([m]))
        else:
            logger.info("Nothing read")
    if args.sync:
        instance.dao.start(False)
        db_mangas = instance.dao.read_get(user)
        db_dict = {db.name.lower(): db for db in db_mangas}
        file_dict ={m.name.lower(): m for m in mangas}
        db_set = set(db_dict.keys())
        file_set = set(file_dict.keys())
        missing_db = [file_dict[key] for key in (file_set - db_set)]
        missing_file = [db_dict[key] for key in (db_set - file_set)]
        upd_db = []

        for key in (db_set & file_set):
            d = db_dict[key]
            m = file_dict[key]
            if not m.uuid:
                m.uuid = d.uuid
                dirty = True
            if not m.created:
                m.created = d.created
                dirty = True
            if not m.updated:
                m.updated = d.updated
                dirty = True
            if not m.name:
                m.name = d.name
                dirty = True
            #logger.debug("--")
            #logger.debug(d.updated)
            #logger.debug(m.updated)
            if d.updated < m.updated:
                # m newer
                d.name = m.name
                d.chapter = m.chapter
                d.updated = m.updated
                upd_db.append(d)
            elif d.updated > m.updated:
                # d newer
                m.name = d.name
                m.chapter = d.chapter
                m.updated = d.updated
                dirty = True
            elif d.chapter != m.chapter:
                if m.chapter is None or m.chapter < d.chapter:
                    m.chapter = d.chapter
                    dirty = True
                elif d.chapter is None or d.chapter < m.chapter:
                    d.chapter = m.chapter
                    upd_db.append(d)
        if missing_db:
            logger.debug("Missing from db:\n" + format_mangas(missing_db))
            for d in missing_db:
                instance.create_manga(user, d)
        if upd_db:
            logger.debug("Update in db:\n" + format_mangas(upd_db))
            for d in upd_db:
                instance.dao.read_update(user, d)
        if missing_file:
            logger.debug("Missing from file:\n" + format_mangas(missing_file))
            for m in missing_file:
                mangas.append(m)
                dirty = True
        instance.dao.stop()

    if dirty:
        logger.debug("Dirty")
        instance.save_manga_file(user, mangas, readable=True)
        pass
