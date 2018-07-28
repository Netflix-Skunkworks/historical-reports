"""
Historical Reports
==================

A collection of reports using data collected by Historical.

"""
import sys
import os.path

from setuptools import setup, find_packages


ROOT = os.path.realpath(os.path.join(os.path.dirname(__file__)))

# When executing the setup.py, we need to be able to import ourselves, this
# means that we need to add the src/ directory to the sys.path.
sys.path.insert(0, ROOT)

about = {}
with open(os.path.join(ROOT, "historical_reports", "__about__.py")) as f:
    exec(f.read(), about)


install_requires = [
    'historical>=0.3.8',
    'retrying==1.3.3',
    'click==6.7'
]

tests_require = [
    'pytest==3.1.3',
    'pytest-cov>=2.5.1',
    'moto>=1.3.2',
    'coveralls==1.1',
    'factory-boy==2.9.2',
    'tox==2.9.1'
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
            's3report = historical_reports.s3.cli:cli',
        ]
    },
    keywords=['aws', 'account_management', "s3"]
)
