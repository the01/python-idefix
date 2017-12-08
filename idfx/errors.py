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
# Created: 2017-11-27 21:09


class IDFXException(Exception):
    """ IDefix base exception """
    pass


class FileException(IDFXException):
    """ File related exception (IOError?) """
    pass


class NoDAOException(IDFXException):
    """ No DAO available """
    pass

class DAOException(IDFXException):
    """ Base DAO exception """
    pass


class ValueException(DAOException, ValueError):
    """ DAO value error """
    pass


class AlreadyExistsException(DAOException):
    """ Entry already exits """
    pass
