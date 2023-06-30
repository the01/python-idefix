# -*- coding: UTF-8 -*-

__author__ = "d01"
__copyright__ = "Copyright (C) 2015-23, Florian JUNG"
__license__ = "All rights reserved"
__version__ = "0.1.1"
__date__ = "2023-06-18"
# Created: 2015-03-13 12:13

from abc import ABCMeta, abstractmethod
import datetime
from typing import Any, Dict, List, Optional, Union

from flotils import Loadable, StartStopable

from ..model import Manga


class DAOException(Exception):  # noqa: N818
    """ Exception from DAO """


# Not used
class AbstractMangaDao(Loadable, StartStopable):
    """ Base class for dao to load mangas """

    __metaclass__ = ABCMeta

    def __init__(self, settings: Optional[Dict[str, Any]] = None):
        """ Constructor """
        if settings is None:
            settings = {}

        super().__init__(settings)

    @abstractmethod
    def connect(self) -> None:
        """
        Connect to datasource

        :raises DAOException: On failure
        """
        raise NotImplementedError("Please implement")

    @abstractmethod
    def disconnect(self) -> None:
        """
        Disconnect from datasource

        :raises DAOException: On failure
        """
        raise NotImplementedError("Please implement")

    @abstractmethod
    def create_location(self, location: str) -> None:
        """
        Create the location if not exists

        :param location: save location
        :raises DAOException: On failure
        """
        raise NotImplementedError("Please implement")

    @abstractmethod
    def create(self, manga: Manga) -> None:
        """
        Create a new manga

        :param manga: Manga to create
        :return: Response (TODO: standardize)
        :raises DAOException: On failure
        """
        raise NotImplementedError("Please implement")

    @abstractmethod
    def update(self, manga: Manga) -> None:
        """
        Update a manga

        :param manga: Manga to update
        :return: Response (TODO: standardize)
        :raises DAOException: On failure
        """
        raise NotImplementedError("Please implement")

    @abstractmethod
    def delete(self, manga: Manga) -> None:
        """
        Delete a manga

        :param manga: Manga to delete
        :return: Response (TODO: standardize)
        :raises DAOException: On failure
        """
        raise NotImplementedError("Please implement")

    @staticmethod
    def format_dict(val: Union[Any, Dict[str, Any]], manga: Manga) -> Any:
        """ TODO """
        if not manga:
            # "no post"
            return val
        if not isinstance(val, dict):
            # "not dict"
            return val
        if "_manga" not in val:
            # "no _post"
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
    def query(self, query: str, manga: Optional[Manga] = None) -> List[Manga]:
        """
        Query the datasource

        :param query: Query to apply
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
        :return: Resulting list of mangas
        :raises DAOException: On failure
        """
        raise NotImplementedError("Please implement")

    @abstractmethod
    def flush(self, location: Optional[str] = None) -> None:
        """
        Persist changes to datasource

        :param location: Which location to flush - None | "" for all
                (default: None)
        :return: Response (TODO: standardize)
        :raises DAOException: On failure
        """
        raise NotImplementedError("Please implement")
