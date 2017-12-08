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
# Created: 2017-12-01 19:26

from flotils.loadable import load_file

from .model import Manga


def read_manga_file(path):
    """
    Read mangas from old manga file

    :param path: Read from file
    :type path: str | unicode
    :return: Read mangas
    :rtype: idefix.model.Manga
    """
    res = []

    loaded = load_file(path)

    for l in loaded.values():
        res.append(Manga(name=l['name'], chapter=l['chapter']))

    return res
