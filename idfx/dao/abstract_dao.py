# -*- coding: UTF-8 -*-
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function
from __future__ import unicode_literals

__author__ = "d01"
__copyright__ = "Copyright (C) 2015-17, Florian JUNG"
__license__ = "All rights reserved"
__version__ = "0.1.1"
__date__ = "2017-11-27"
# Created: 2015-03-13 12:13

from abc import ABCMeta, abstractmethod
import datetime

from flotils.loadable import Loadable


class DAOException(Exception):
    pass


class AbstractMangaDao(Loadable):
    __metaclass__ = ABCMeta

    def __init__(self, settings=None):
        if settings is None:
            settings = {}
        super(AbstractMangaDao, self).__init__(settings)

    @abstractmethod
    def connect(self):
        """
        Connect to datasource

        :rtype: None
        :raises DAOException: On failure
        """
        raise NotImplementedError("Please implement")

    @abstractmethod
    def disconnect(self):
        """
        Disconnect from datasource

        :rtype: None
        :raises DAOException: On failure
        """
        raise NotImplementedError("Please implement")

    @abstractmethod
    def create_location(self, location):
        """
        Create the location if not exists

        :param location: save location
        :type location: str | unicode
        :rtype: None
        :raises DAOException: On failure
        """
        raise NotImplementedError("Please implement")

    @abstractmethod
    def create(self, manga):
        """
        Create a new manga

        :param manga: Manga to create
        type manga: idefix.model.Manga
        :return: Response (TODO: standardize)
        :rtype: None
        :raises DAOException: On failure
        """
        raise NotImplementedError("Please implement")

    @abstractmethod
    def update(self, manga):
        """
        Update a manga

        :param manga: Manga to update
        type manga: idefix.model.Manga
        :return: Response (TODO: standardize)
        :rtype: None
        :raises DAOException: On failure
        """
        raise NotImplementedError("Please implement")

    @abstractmethod
    def delete(self, manga):
        """
        Delete a manga

        :param manga: Manga to delete
        type manga: idefix.model.Manga
        :return: Response (TODO: standardize)
        :rtype: None
        :raises DAOException: On failure
        """
        raise NotImplementedError("Please implement")

    @staticmethod
    def format_dict(val, manga):
        if not manga:
            #print "no post"
            return val
        if not isinstance(val, dict):
            #print "not dict"
            return val
        if "_manga" not in val:
            #print "no _post"
            return val
        res = getattr(manga, val['_post'])
        if "+" in val:
            append = val['+']
            if isinstance(res, datetime.datetime):
                res = res.date()
            if isinstance(res, datetime.date):
                res = res + datetime.timedelta(**append)
            else:
                res += append
        if "-" in val:
            append = val['-']
            if isinstance(res, datetime.datetime):
                res = res.date()
            if isinstance(res, datetime.date):
                res = res - datetime.timedelta(**append)
            else:
                res -= append
        return res

    @abstractmethod
    def query(self, query, manga=None):
        """
        Query the datasource

        :param query: Query to apply
        location: str
            location in the database - None | "" for all (default: None)
        query: dict
            field_name: dict
                < | <=: value   smaller than
                > | >=: value   greater than
                ==: value       equals to
                range: dict
                    <: value    upper bound
                    >: value    lower bound
        sort_by: list[dict]
            field_name: asc | desc
        :type query: dict
        :param manga: If present insert into query (default: None)
        :type manga: idefix.model.Manga | None
        :return: Resulting list of mangas
        rtype: collections.Iterable[rss.post.Post]
        :raises DAOException: On failure
        """
        raise NotImplementedError("Please implement")

    @abstractmethod
    def flush(self, location=None):
        """
        Persist changes to datasource

        :param location: Which location to flush - None | "" for all
                (default: None)
        :type location: str | unicode
        :return: Response (TODO: standardize)
        :rtype: None
        :raises DAOException: On failure
        """
        raise NotImplementedError("Please implement")
