#!/usr/bin/env python

from distutils.core import setup

install_requires = ['enum34>=1.1.6','ping>=0.2']


setup(name='group3',
      version='0.1',
      description='Efficient client server scripts',
      author='Group3',
      packages=['group3.client', 'group3.server', 'group3'],
      install_requires=install_requires,
     )