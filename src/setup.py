import os
from setuptools import setup, find_packages
from main import datatrace

readme = open(os.path.join(os.path.dirname(__file__), "README.md"), "r", encoding="utf-8")

with open('../requirements.txt') as f:
    required = f.read().splitlines()

setup(
    name="datatrace",#datatrace
    version=datatrace.__version__,
    description="Librería para identificar la trazabilidad de tablas y campos en scripts PLSQL Oracle.",
    long_description=readme.read(),
    long_description_content_type="text/markdown",
    author="Data Excellence",
    author_email="mlarico@bcp.com.pe",
    package_dir={'': 'main'},
    packages=find_packages(where='main', exclude=["tests"]),
    python_requires=">=3.10",
    install_requires=required,
    extras_require={
        "dev": ["check-manifest"],
        "test": ["coverage"]
    },
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: BCP License"
    ]
)