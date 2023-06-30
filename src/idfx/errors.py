# -*- coding: UTF-8 -*-

__author__ = "d01"
__email__ = "jungflor@gmail.com"
__copyright__ = "Copyright (C) 2017-23, Florian JUNG"
__license__ = "MIT"
__version__ = "0.1.0"
__date__ = "2017-11-30"
# Created: 2017-11-27 21:09


# Wants exceptions to end with 'Error'
class IDFXException(Exception):  # noqa: N818
    """ IDefix base exception """


class FileException(IDFXException):  # noqa: N818
    """ File related exception (IOError?) """


class NoDAOException(IDFXException):  # noqa: N818
    """ No DAO available """


class DAOException(IDFXException):  # noqa: N818
    """ Base DAO exception """


class ValueException(DAOException, ValueError):  # noqa: N818
    """ DAO value error """


class AlreadyExistsException(DAOException):  # noqa: N818
    """ Entry already exits """
