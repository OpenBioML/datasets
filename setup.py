#!/usr/bin/env python

"""The setup script."""

from setuptools import find_packages, setup

requirements = [
    "rdkit",
    "selfies",
    "requests",
    "deepchem==2.6.0.dev20210910040306",
]

test_requirements = [
    "pytest>=3",
]

setup(
    author="Zach Nussbaum",
    author_email="zanussbaum@gmail.com",
    python_requires=">=3.6",
    classifiers=[
        "Development Status :: 2 - Pre-Alpha",
        "Intended Audience :: Developers",
        "License :: OSI Approved :: MIT License",
        "Natural Language :: English",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.6",
        "Programming Language :: Python :: 3.7",
        "Programming Language :: Python :: 3.8",
    ],
    description="Datasets for OpenBioML",
    install_requires=requirements,
    license="MIT license",
    include_package_data=True,
    keywords="datasets",
    name="openbioml_datasets",
    packages=find_packages(include=["datasets", "datasets.*"]),
    test_suite="tests",
    tests_require=test_requirements,
    url="https://github.com/OpenBioML/datasets",
    version="0.1.0",
    zip_safe=False,
)