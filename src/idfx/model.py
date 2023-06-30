# -*- coding: UTF-8 -*-

__author__ = "d01"
__email__ = "jungflor@gmail.com"
__copyright__ = "Copyright (C) 2017-23, Florian JUNG"
__license__ = "MIT"
__version__ = "0.1.0"
__date__ = "2023-06-19"
# Created: 2017-11-27 21:16

import abc
import datetime
from typing import Any, Dict, List, Optional


def format_vars(instance: Any) -> str:
    """
    Get variables of instance and list them with their values
    as comma separated string
    """
    attrs = vars(instance)

    return ", ".join(
        f"{key}={value}"
        for key, value in attrs.items()
    )


class FromToDictBase(object):
    """ Save to and load from dict """

    __metaclass__ = abc.ABCMeta

    @classmethod
    def from_dict(cls, d: Dict[str, Any]):
        """ Instantiate class from dict """
        new = cls()

        if not d:
            return new

        attrs = vars(new)

        for key in d:
            if key in attrs:
                # both in dict and this class
                setattr(new, key, d[key])

        return new

    def to_dict(self) -> Dict[str, Any]:
        """ Represent class as dict """
        attrs = vars(self)
        res = {}

        for key, value in attrs.items():
            if isinstance(value, FromToDictBase):
                res[key] = value.to_dict()
            else:
                res[key] = value

        return res


class PrintableBase(object):
    """ Make class nicely printable """

    __metaclass__ = abc.ABCMeta

    def __str__(self):
        """ For users """
        return f"<{self.__class__.__name__}>({format_vars(self)})"

    def __repr__(self):
        """ For programmer """
        return self.__str__()


class Entry(PrintableBase, FromToDictBase):
    """ Base class for models/dto """

    def __init__(
            self,
            uuid: Optional[str] = None,
            created: Optional[datetime.datetime] = None,
            updated: Optional[datetime.datetime] = None,
    ) -> None:
        """ Constructor """
        super().__init__()

        self.uuid: Optional[str] = uuid
        """ Unique identifier """
        self.created: Optional[datetime.datetime] = created
        """ When was entry created """
        self.updated: Optional[datetime.datetime] = updated
        """ When was entry last updated """


class Manga(Entry):
    """ Dto for manga """

    def __init__(
            self,
            uuid: Optional[str] = None,
            name: Optional[str] = None,
            chapter: Optional[float] = None,
    ) -> None:
        """ Constructor """
        super().__init__(uuid)

        self.name: Optional[str] = name
        """ Manga name """
        self.chapter: Optional[float] = chapter
        """ Latest chapter read by user """
        self.latest_chapter: float = 0.0
        """ Latest chapter found anywhere """
        self.urls: List[str] = []
        """ List of urls where this chapter can be found """


class User(Entry):
    """ Dto for user """

    ROLE_ADMIN = 0
    """ Admin user """
    ROLE_USER = 1
    """ Regular user """

    def __init__(
            self,
            uuid: Optional[str] = None,
            firstname: Optional[str] = None,
            lastname: Optional[str] = None
    ) -> None:
        """ Constructor """
        super().__init__(uuid)

        self.firstname: Optional[str] = firstname
        """ User first name """
        self.lastname: Optional[str] = lastname
        """ User last name """
        self.role: int = User.ROLE_USER
        """ User permissions/role """
