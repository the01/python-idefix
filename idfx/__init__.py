# -*- coding: UTF-8 -*-

__author__ = "d01"
__email__ = "jungflor@gmail.com"
__copyright__ = "Copyright (C) 2017-21, Florian JUNG"
__license__ = "MIT"

__date__ = "2021-05-06"
# Created: 2017-11-27 21:08

from .__version__ import __version__
from .controller import IDFXManga
from .model import Manga, User

__all__ = ["IDFXManga", "Manga", "User"]
