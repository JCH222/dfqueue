# coding: utf8

from setuptools import setup, find_packages
from version import version

setup(
    name='dfqueue',
    version=version,
    install_requires="pandas",
    packages=find_packages(),
    author="Jean-Charles Hardy",
    author_email="",
    description="Content management decorators for pandas Dataframes",
    long_description=open('README.md').read(),
    include_package_data=True,
    url='https://github.com/JCH222/dfqueue',
    classifiers=[
        "Programming Language :: Python :: 3.6",
    ],
    license="MIT",
)
