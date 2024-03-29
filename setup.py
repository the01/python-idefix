#!/usr/bin/env python
# -*- coding: UTF-8 -*-

__author__ = "d01 <Florian Jung>"
__email__ = "jungflor@gmail.com"
__copyright__ = "Copyright (C) 2015-21, Florian JUNG"
__license__ = "MIT"
__version__ = "0.3.0"
__date__ = "2021-05-06"
# Created: ?

try:
    from setuptools import setup
except ImportError:
    from distutils.core import setup
import sys
import os
import re


if sys.argv[-1] == "build":
    os.system("python setup.py clean sdist bdist bdist_egg bdist_wheel")


def get_version():
    """
    Parse the version information from the init file
    """
    version_file = os.path.join("idfx", "__version__.py")
    initfile_lines = open(version_file, "rt").readlines()
    version_reg = r"^__version__ = ['\"]([^'\"]*)['\"]"

    for line in initfile_lines:
        mo = re.search(version_reg, line, re.M)
        if mo:
            return mo.group(1)

    raise RuntimeError(
        "Unable to find version string in {}".format(version_file)
    )


def get_file(path):
    with open(path, "r") as f:
        return f.read()


def split_external_requirements(requirements):
    external = []
    # External dependencies
    pypi = []
    # Dependencies on pypi

    for req in requirements:
        if req.startswith("-e git+"):
            # External git link
            external.append(req.lstrip("-e git+"))
        else:
            pypi.append(req)
    return pypi, external


version = get_version()
readme = get_file("README.rst")
history = get_file("HISTORY.rst")
pypi, external = split_external_requirements(
    get_file("requirements.txt").split("\n")
)

assert version is not None
assert readme is not None
assert history is not None
assert pypi is not None
assert external is not None

setup(
    name="idfx",
    version=version,
    description="Manga chapter checker",
    long_description=readme + "\n\n" + history,
    author="the01",
    author_email="jungflor@gmail.com",
    url="https://github.com/the01/python-idefix",
    packages=[
        "idfx",
        "idfx.dao"
    ],
    install_requires=pypi,
    dependency_links=external,
    entry_points={
        'console_scripts': [
            "idefix=idfx.cli:main",
        ]
    },
    license="MIT License",
    keywords="idefix manga",
    classifiers=[
        "Development Status :: 4 - Beta",
        "Intended Audience :: Developers",
        "License :: OSI Approved :: MIT License",
        "Natural Language :: English",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.6",
        "Programming Language :: Python :: 3.7",
        "Programming Language :: Python :: 3.8",
        "Programming Language :: Python :: 3.9",
    ]
)
