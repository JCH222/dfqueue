# coding: utf8

# noinspection PyProtectedMember
from setuptools import setup, find_packages, convert_path

package_name = 'dfqueue'

meta_data = {}
meta_data_file_path = convert_path(package_name + '/__meta__.py')
with open(meta_data_file_path) as meta_data_file:
    exec(meta_data_file.read(), meta_data)

setup(
    name=package_name,
    version=meta_data['__version__'],
    install_requires="pandas>=0.23.4",
    packages=find_packages(),
    author=meta_data['__author__'],
    author_email=meta_data['__author_email__'],
    description=meta_data['__description__'],
    long_description=open('README.md').read(),
    include_package_data=True,
    url=meta_data['__github_url__'],
    classifiers=meta_data['__classifiers__'],
    license=meta_data['__licence__'],
    keywords=meta_data['__keywords__']
)
