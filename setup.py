from pathlib import Path
from setuptools import setup, find_packages
import json


install_requires = [
    "python-dotenv",
]

classifiers = [
    "Development Status :: 4 - Beta",
    "Intended Audience :: Developers",
    "License :: OSI Approved :: MIT License",
    "Natural Language :: English",
    "Programming Language :: Python",
    "Programming Language :: Python :: 3"
]


def get_version():
    with open("config/version.json", "r") as f:
        version = json.load(f)
        return f"{version['major']}.{version['minor']}.{version['patch']}"


setup(
    name="pkrfilesorter",
    version=get_version(),
    author="Alexandre MANGWA",
    author_email="alex.mangwa@gmail.com",
    description="A package to sort poker files in adapted directories",
    long_description=Path("README.md").read_text(),
    long_description_content_type="text/markdown",
    url="https://github.com/manggy94/PokerFileSorter",
    project_urls={
        "Bug Tracker": "https://github.com/manggy94//issues",
        "Documentation": "https://pkrfilesorter.readthedocs.io/en/latest/"
    },
    classifiers=classifiers,
    packages=find_packages(where="pkrfilesorter"),
    python_requires=">=3.10",
    install_requires=install_requires
)
