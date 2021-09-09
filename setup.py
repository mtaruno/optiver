from setuptools import setup, find_packages
from codecs import open
from os import path

__version__ = "0.0.1"

here = path.abspath(path.dirname(__file__))

setup(
    name="optiver",
    version=__version__,
    description="Realized volatility Kaggle competition",
    url="",
    download_url="",
    license="MIT",
    packages=find_packages(),
    include_package_data=True,
    author="Matthew Taruno",
    # install_requires=install_requires,
    setup_requires=[],
    # dependency_links=dependency_links,
    author_email="matthew.taruno@gmail.com",
)
