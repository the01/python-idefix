# -*- coding: UTF-8 -*-

__author__ = "d01"
__email__ = "jungflor@gmail.com"
__copyright__ = "Copyright (C) 2017-23, Florian JUNG"
__license__ = "MIT"
__version__ = "0.1.0"
__date__ = "2021-05-06"
# Created: 2017-12-01 21:15

import argparse
import datetime
import logging
import logging.config
from typing import List

from flotils import get_logger
from flotils.logable import default_logging_config
from requests.compat import urlparse

from .controller import IDFXManga
from .model import Manga


logger = get_logger()
logging.captureWarnings(True)


def setup_parser() -> argparse.ArgumentParser:
    """
    Create and init argument parser

    :return: Argument parser
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
        help="Mark all as read / specific"
    )
    parser.add_argument(
        "--sync", action="store_true",
        help="Sync with database"
    )
    parser.add_argument(
        "--setup", action="store_true",
        help="Setup database database"
    )
    parser.add_argument(
        "--add", nargs="?", default=None,
        help="Add manga"
    )

    return parser


def format_mangas(mangas: List[Manga]) -> str:
    """

    :param mangas: Mangas to format
    :return: Formatted Text
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
                int_chap = int(int_chap)
            except Exception:
                int_chap = m.chapter

        if int_chap != m.chapter:
            int_chap = m.chapter

        res.append(f"{m.name} ({int_chap}){url_parts}")

    return "\n".join(res)


def main() -> int:  # noqa: C901
    """ Run main code for cli """
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
    try:
        user, mangas = instance.load_manga_file(path=args.manga_file)
    except Exception:
        logger.exception("Failed to load manga file")

        return quit(1)

    if instance.dao is None:
        logger.error("A dao is required to run")

        return quit(1)

    new = None
    dirty = False

    if args.add:
        n = Manga(name=args.add)
        found = []

        if n.name is not None:
            found = [
                m
                for m in mangas
                if m.name is not None and n.name.lower() == m.name.lower()
            ]

        if found:
            logger.info(f"Manga already added ({found[0].name})")
        else:
            n.updated = datetime.datetime.utcnow()
            n.created = datetime.datetime.utcnow()
            mangas.append(n)
            dirty = True

    if args.check:
        temp_index = instance.create_index()

        if temp_index is not None:
            new = instance.check(
                mangas, temp_index
            )

        if new:
            logger.info("\n" + format_mangas(new))
        else:
            logger.info("No updates")

    if args.read is not None:
        if new is None:
            temp_index = instance.create_index()

            if temp_index is not None:
                new = instance.check(
                    mangas, temp_index
                )

        if new:
            r = args.read.lower()

            for m in mangas:
                if m.name is None:
                    # Will not process manga without name
                    continue

                if r and not m.name.lower().startswith(r):
                    continue

                for n in new:
                    if n.name is None:
                        # Will not process manga without name
                        continue

                    if not (
                            (n.name or m.name) and n.name.lower() == m.name.lower()
                    ) and not (
                            (n.uuid or m.uuid) and n.uuid == m.uuid
                    ):
                        continue

                    m.chapter = n.chapter
                    m.updated = n.updated
                    m.urls = n.urls
                    dirty = True
                    logger.info("Read " + format_mangas([m]))
        else:
            logger.info("Nothing read")

    if args.setup:
        if not instance.dao._is_running:
            instance.dao.start(False)

        instance.dao.setup()

    if args.sync:
        if not instance.dao._is_running:
            instance.dao.start(False)

        db_mangas = instance.dao.read_get(user)
        db_dict = {
            db.name.lower(): db
            for db in db_mangas
            if db.name is not None
        }
        file_dict = {
            m.name.lower(): m
            for m in mangas
            if m.name is not None
        }
        db_set = set(db_dict.keys())
        file_set = set(file_dict.keys())
        missing_db = [
            file_dict[key]
            for key in (file_set - db_set)
        ]
        missing_file = [
            db_dict[key]
            for key in (db_set - file_set)
        ]
        upd_db = []

        for key in (db_set & file_set):
            d = db_dict[key]
            m = file_dict[key]

            if not m.uuid:
                logger.debug(f"NoUUID {d.uuid} -> {m.uuid}")
                m.uuid = d.uuid
                dirty = True
            if not m.created:
                logger.debug(f"NoCtd {d.created} -> {m.created}")
                m.created = d.created
                dirty = True
            if not m.updated:
                logger.debug(f"NoUpd {d.updated} -> {m.updated}")
                m.updated = d.updated
                dirty = True
            if not m.name:
                logger.debug(f"NoName {d.name} -> {m.name}")
                m.name = d.name
                dirty = True
            if d.updated is not None and m.updated is not None \
                    and d.updated < m.updated:
                # m newer
                logger.debug(f"UpdD {m} -> {d}")
                d.name = m.name
                d.chapter = m.chapter
                d.updated = m.updated
                upd_db.append(d)
            elif d.updated is not None and m.updated is not None \
                    and d.updated > m.updated:
                # d newer
                logger.debug(f"UpdM {d} -> {m}")
                m.name = d.name
                m.chapter = d.chapter
                m.updated = d.updated
                dirty = True
            elif d.chapter != m.chapter:
                if m.chapter is None \
                        or (d.chapter is not None and m.chapter < d.chapter):
                    logger.debug(f"ChapM {d.chapter} -> {m.chapter}")
                    m.chapter = d.chapter
                    dirty = True
                elif d.chapter is None or d.chapter < m.chapter:
                    logger.debug(f"ChapD {m.chapter} -> {d.chapter}")
                    d.chapter = m.chapter
                    upd_db.append(d)

        if missing_db:
            logger.debug("Missing from db:\n" + format_mangas(missing_db))

            for d in missing_db:
                if not d.uuid:
                    dirty = True

                instance.create_manga(user, d)

        if upd_db:
            logger.debug("Update in db:\n" + format_mangas(upd_db))

        #     for d in upd_db:
        #         instance.dao.read_update(user, d)

        if missing_file:
            logger.debug("Missing from file:\n" + format_mangas(missing_file))

            for m in missing_file:
                mangas.append(m)
                dirty = True

        instance.dao.stop()

    if dirty:
        logger.debug("Dirty")
        instance.save_manga_file(user, mangas, readable=True)

    return 0
