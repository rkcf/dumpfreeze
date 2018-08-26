#!/usr/bin/env python

import os
from setuptools import setup, find_packages
import dumpfreeze

setup(
    name='dumpfreeze',
    description='Create MySQL dumps and backup to Amazon Glacier',
    version=dumpfreeze.__version__,
    url='https://github.com/rkcf/dumpfreeze',
    author_email='rkcf@rkcf.me',
    author='Andrew Steinke',
    long_description=open('README.md').read(),
    packages=find_packages(),
    data_files=[(os.path.join(os.environ.get('HOME'), '.dumpfreeze'),
                ['data/inventory.db'])],
    license='MIT',
    install_requires=['boto3', 'click'],
    entry_points={
        'console_scripts': [
            'dumpfreeze = dumpfreeze.main'
        ]
    },
    )
