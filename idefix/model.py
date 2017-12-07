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
__date__ = "2017-11-30"
# Created: 2017-11-27 21:16

import abc


def format_vars(instance):
    attrs = vars(instance)
    return ", ".join("{}={}".format(key, value) for key, value in attrs.items())


class FromToDictBase(object):
    __metaclass__ = abc.ABCMeta

    @classmethod
    def from_dict(cls, d):
        new = cls()
        if not d:
            return new
        attrs = vars(new)
        for key in d:
            if key in attrs:
                # both in dict and this class
                setattr(new, key, d[key])
        return new

    def to_dict(self):
        attrs = vars(self)
        res = {}
        for key, value in attrs.items():
            if isinstance(value, FromToDictBase):
                res[key] = value.to_dict()
            else:
                res[key] = value
        return res


class PrintableBase(object):
    __metaclass__ = abc.ABCMeta

    def __str__(self):
        return "<{}>({})".format(self.__class__.__name__, format_vars(self))

    def __unicode__(self):
        return self.__str__()

    def __repr__(self):
        return self.__str__()


class Entry(PrintableBase, FromToDictBase):

    def __init__(self, uuid=None, created=None, updated=None):
        super(Entry, self).__init__()
        self.uuid = uuid
        """ :type : str | unicode """
        self.created = created
        """ :type : datetime.datetime """
        self.updated = updated
        """ :type : datetime.datetime """


class Manga(Entry):

    def __init__(self, uuid=None, name=None, chapter=None):
        super(Manga, self).__init__(uuid)
        self.name = name
        """ :type : str | unicode """
        self.chapter = chapter
        """ Latest chapter read by user
            :type : float """
        self.latest_chapter = 0
        """ Latest chapter found anywhere
            :type : int """
        self.urls = []
        """ :type : list[str | unicode] """


class User(Entry):
    ROLE_ADMIN = 0
    ROLE_USER = 1

    def __init__(self, uuid=None, firstname=None, lastname=None):
        super(User, self).__init__(uuid)
        self.firstname = firstname
        """ :type : str | unicode """
        self.lastname = lastname
        """ :type : str | unicode """
        self.role = User.ROLE_USER
