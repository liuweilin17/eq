import os
from setuptools import setup

# Utility function to read the README file.
# Used for the long_description.  It's nice, because now 1) we have a top level
# README file and 2) it's easier to type in the README file than to put a raw
# string in below ...
def read(fname):
    return open(os.path.join(os.path.dirname(__file__), fname)).read()

setup(
    name = "problem_tools",
    version = "1.0",
    author = "Weilin Liu",
    author_email = "liuweilin17@gmail.com",
    description = ("Tools for work sample problems"),
    license = "BSD",
    packages=['tools'],
    install_requires=['pyspark-pandas']
)
