"""
Historical Functions - S3
==========

Consumer of Historical's S3 tracking -- dumps a JSON of all buckets in an account, along with the configuration.

"""
import sys
import os.path

from setuptools import setup, find_packages


ROOT = os.path.realpath(os.path.join(os.path.dirname(__file__)))

# When executing the setup.py, we need to be able to import ourselves, this
# means that we need to add the src/ directory to the sys.path.
sys.path.insert(0, ROOT)

about = {}
with open(os.path.join(ROOT, "s3", "__about__.py")) as f:
    exec(f.read(), about)


install_requires = [
    'historical==0.1.1',
    'retrying==1.3.3',
    'click==6.7'
]

tests_require = [
    'pytest==3.1.3',
    'moto==1.1.24',
    'coveralls==1.1',
    'factory-boy==2.9.2'
]

setup(
    name=about["__title__"],
    version=about["__version__"],
    author=about["__author__"],
    author_email=about["__email__"],
    url=about["__uri__"],
    description=about["__summary__"],
    long_description='See README.md',
    packages=find_packages(),
    include_package_data=True,
    zip_safe=False,
    install_requires=install_requires,
    extras_require={
        'tests': tests_require
    },
    entry_points={
        'console_scripts': [
            's3report = s3.cli:cli',
        ]
    },
    keywords=['aws', 'account_management', "s3"]
)
