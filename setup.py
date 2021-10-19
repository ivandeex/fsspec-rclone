#!/usr/bin/env python

import setuptools
import versioneer

with open("README.md", "r") as f:
    long_description = f.read()

setuptools.setup(
    name="fsspec-rclone",
    version=versioneer.get_version(),
    cmdclass=versioneer.get_cmdclass(),
    classifiers=[
        'Development Status :: 4 - Beta',
        'Intended Audience :: Developers',
        "License :: OSI Approved :: MIT License",
        'Operating System :: OS Independent',
        'Programming Language :: Python :: 3.6',
        'Programming Language :: Python :: 3.7',
        'Programming Language :: Python :: 3.8',
    ],
    description="Filesystem-spec interface over rclone",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/ivandeex/fsspec-rclone/",
    author="Ivan Andreev",
    author_email="ivandeex@gmail.com",
    license='MIT',
    keywords='fsspec, rclone',
    packages=['fsspec_rclone'],
    python_requires='>= 3.6',
    install_requires=[open('requirements.txt').read().strip().split('\n')],
    entry_points={
        'fsspec.specs': [
            'rclone=fsspec_rclone.RcloneSpecFS',
        ],
    },
)
