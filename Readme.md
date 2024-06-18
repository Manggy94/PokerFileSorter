# 
## Description
A package to sort poker files in adapted directories

PokerFileSorter is a simple tool to sort poker files in adapted directories.

## Table of Contents

- [Description](#description)
- [Getting Started](#getting-started)
- [Usage](#usage)

## Getting Started

To install the package, use the following command:

```sh
pip install pkrfilesorter
```

If you want to download the source code, you can do so by cloning the repository:

```sh
git clone https://github.com/Manggy94/PokerFileSorter.git
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

Setting source directories and destination directories from a .env file:

.env:
```env
SOURCE_DIR="path/to/poker/history/dir"
DESTINATION_DIR="path/to/sorted/dir"
```

En cli:
```sh
python -m pkrfilesorter.main
```