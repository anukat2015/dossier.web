#!/usr/bin/env python

import os

from setuptools import setup, find_packages

from version import get_git_version
VERSION, SOURCE_LABEL = get_git_version()
PROJECT = 'dossier.web'
AUTHOR = 'Diffeo, Inc.'
AUTHOR_EMAIL = 'support@diffeo.com'
URL = 'http://github.com/dossier/dossier.web'
DESC = 'DossierStack web services'


def read_file(file_name):
    file_path = os.path.join(
        os.path.dirname(__file__),
        file_name
        )
    return open(file_path).read()


setup(
    name=PROJECT,
    version=VERSION,
    description=DESC,
    license='MIT',
    long_description=read_file('README.rst'),
    author=AUTHOR,
    author_email=AUTHOR_EMAIL,
    url=URL,
    packages=find_packages(),
    namespace_packages=['dossier'],
    classifiers=[
        'Development Status :: 3 - Alpha',
        'Topic :: Utilities',
        'License :: OSI Approved :: MIT License',
    ],
    install_requires=[
        'bottle >= 0.12',
        'dblogger',
        'dossier.fc >= 0.2.0',
        'dossier.label >= 0.4.0',
        'dossier.store >= 0.3.1',
        'kvlayer >= 0.5.7',
        'pytest',
        'nilsimsa >= 0.3.4',
        'uwsgi >= 2',
        'yakonfig >= 0.7.2',
    ],
    include_package_data=True,
    zip_safe=False,
    entry_points={
        'console_scripts': [
            'dossier.web = dossier.web.run:main',
        ],
    },
)
