#!/usr/bin/env python

from distutils.core import setup

setup(name='ukai',
      version='0.2',
      description='Location Aware Distributed Storage for Virtual Machine Disks',
      author='Keiichi SHIMA',
      author_email='keiichi@iijlab.net',
      url='http://github.com/keiichishima/ukai/',
      packages=['libukai'],
      requires=['fusepy (>=2.0.2)', 'netifaces (>=0.6)', 'redis (>=2.10)'],
      scripts=['scripts/ukai_server',
               'scripts/ukai_fuse',
               'scripts/ukai_admin',
               ],
      data_files=[('share/doc/ukai', ['ukai_logo.png'])],
      classifiers=[
        'Development Status :: 3 - Alpha',
        'Environment :: Console',
        'Intended Audience :: Developers',
        'Intended Audience :: Information Technology',
        'Intended Audience :: System Administrators',
        'License :: OSI Approved :: BSD License',
        'Natural Language :: English',
        'Operating System :: MacOS :: MacOS X',
        'Operating System :: POSIX :: Linux',
        'Programming Language :: Python :: 2.7',
        'Topic :: System :: Filesystems',
        ],
)

