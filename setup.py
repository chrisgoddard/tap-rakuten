#!/usr/bin/env python
from setuptools import setup

setup(
    name="tap-rakuten",
    version="0.1.0",
    description="Singer.io tap for extracting data from Rakuten (LinkShare) reports.",
    author="Chris Goddard",
    url="https://github.com/chrisgoddard",
    classifiers=["Programming Language :: Python :: 3 :: Only"],
    py_modules=["tap_rakuten"],
    install_requires=[
        "singer-python==5.4.1",
        "requests==2.21.0"
    ],
    entry_points="""
    [console_scripts]
    tap-rakuten=tap_rakuten:main
    """,
    packages=["tap_rakuten"],
)
