#!/usr/bin/env python

from setuptools import setup


setup(
    name='dumpfreeze',
    description='Create MySQL dumps and backup to Amazon Glacier',
    version=0.1,
    url='https://github.com/rkcf/dumpfreeze',
    author_email='rkcf@rkcf.me',
    author='Andrew Steinke',
    long_description=open('README.md').read(),
    packages=['dumpfreeze'],
    license='MIT',
    install_requires=['boto3'],
    entry_points={
        'console_scripts': [
            'dumpfreeze = dumpfreeze.dumpfreeze:main'
        ]
    }
    )
