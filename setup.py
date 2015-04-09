#!/usr/bin/env python

import os

from setuptools import setup, find_packages

from version import get_git_version
VERSION, SOURCE_LABEL = get_git_version()
PROJECT = 'rejester'
AUTHOR = 'Diffeo, Inc.'
AUTHOR_EMAIL = 'support@diffeo.com'
DESC = ('redis-based python client library and command line tools '
        'for managing tasks executed by a group of configurable workers')
LICENSE = 'MIT/X11 license http://opensource.org/licenses/MIT'
URL = 'http://github.com/diffeo/rejester'


def read_file(file_name):
    with open(os.path.join(os.path.dirname(__file__), file_name), 'r') as f:
        return f.read()


setup(
    name=PROJECT,
    version=VERSION,
    description=DESC,
    license=LICENSE,
    long_description=read_file('README.md'),
    author=AUTHOR,
    author_email=AUTHOR_EMAIL,
    url=URL,
    packages=find_packages(),
    classifiers=[
        'Development Status :: 3 - Alpha',
        'Environment :: Console',
        'Environment :: No Input/Output (Daemon)',
        'Intended Audience :: Developers',
        'Topic :: Utilities',
        # MIT/X11 license http://opensource.org/licenses/MIT
        'License :: OSI Approved :: MIT License',
        'Operating System :: POSIX',
        'Programming Language :: Python',
        'Programming Language :: Python :: 2',
        'Programming Language :: Python :: 2.7',
        'Programming Language :: Python :: 2 :: Only',
        'Topic :: System :: Distributed Computing',
    ],
    install_requires=[
        'dblogger >= 0.4.0',
        'yakonfig >= 0.6.0',
        'pyyaml',
        'redis',
        'psutil',
        'python-daemon<2',
        'setproctitle',
        'cbor',
    ],
    extras_require={
        'unittest': ['pytest', 'pytest-diffeo'],
    },
    entry_points={
        'console_scripts': [
            'rejester = rejester.run:main',
            'rejester_worker = rejester.run_multi_worker:main',
            'rejester_test = rejester.tests.run:main [unittest]',
        ],
    },
    zip_safe=False,  # so we can get into examples
)
