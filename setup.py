from setuptools import setup, find_packages
import os, codecs, re

pkg_name = 'graphene'

here = os.path.abspath(os.path.dirname(__file__))

def read(*parts):
    with codecs.open(os.path.join(here, *parts), 'r') as fp:
        return fp.read()

def find_version(*file_paths):
    version_file = read(*file_paths)
    version_match = re.search(r"^__version__ = ['\"]([^'\"]*)['\"]",
                              version_file, re.M)
    if version_match:
        return version_match.group(1)
    raise RuntimeError("Unable to find version string.")

setup(
    name=pkg_name, 
    version=find_version(pkg_name, "__init__.py"),
    description="General Graph generator tool from raw data using abstract mappers",
    packages=find_packages(),
    license='',
    long_desription='...TBD...'
    )
