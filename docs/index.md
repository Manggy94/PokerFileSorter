# PkrFileSorter

[![Documentation Status](https://readthedocs.org/projects/pkrfilesorter/badge/?version=latest)](https://pkrfilesorter.readthedocs.io/en/latest/?badge=latest)
[![PyPI version](https://badge.fury.io/py/pkrfilesorter.svg)](https://badge.fury.io/py/pkrfilesorter)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)

## Description
A package to sort poker files in adapted directories

PokerFileSorter is a simple tool to sort poker files in adapted directories.

## Table of Contents

- [Description](#description)
- [Getting Started](#getting-started)
- [Usage](#usage)
- [Documentation](#documentation)
- [License](#license)

## Getting Started

To install the package, use the following command:

```sh
pip install pkrfilesorter
```

## Usage

### Base Examples

Here are some simple examples of using PokerFileSorter:

Sort a poker file to a directory with custom directories:

```python
from pkrfilesorter.file_sorter import FileSorter

sorter = FileSorter("path/to/poker/history/dir", "path/to/sorted/dir")
sorter.copy_files()
```

## Documentation

Read the doc on [ReadTheDocs](https://pkrfilesorter.readthedocs.io/en/latest/)

## License

This project is licensed under the *MIT* license. See the [LICENSE](LICENSE.txt) file for more details.