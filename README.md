# siphon

## Introduction

`siphon` is a python package to more easily store/transfer data between Pandas and Postgres.

## Table of Contents

- [siphon](#siphon)
  - [Introduction](#introduction)
  - [Table of Contents](#table-of-contents)
  - [Installation](#installation)
    - [Install with pip](#install-with-pip)
    - [Install from source](#install-from-source)
    - [Tests](#tests)
  - [Passing the Connection String](#passing-the-connection-string)
  - [Issues](#issues)

## Installation

### Install with pip

Run `pip install git+git://github.com/cfergusonlee/siphon.git`

### Install from source

`git clone https://github.com/cfergusonlee/siphon.git`

Run `python setup.py install`

### Tests

Tests cover a handful of datatypes:
- String
- Int
- Float
- Varchar Array
- Boolean
- Datetime

# Passing the Connection String

There are two ways to pass a connection string to the PostgresConnection object:

1.  Pass the environment variable name
    > `>>> with PostgresConnection(database_var='YOUR_CONNECTION_STRING_VARIABLE') as con:`
2.  Pass the connection string directly.
    > `>>> with PostgresConnection(database_url='YOUR_CONNECTION_STRING_VALUE') as con:`

## Issues

Report bugs and feature requests
[here](https://github.com/cfergusonlee/siphon/issues).
