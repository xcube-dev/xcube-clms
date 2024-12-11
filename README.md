# xcube-clms

[![Unittest xcube-clms](https://github.com/xcube-dev/xcube-clms/actions/workflows/unittest.yml/badge.svg)](https://github.com/xcube-dev/xcube-clms/actions/workflows/unittest.yml)
[![Codecov xcube-clms](https://codecov.io/gh/xcube-dev/xcube-clms/graph/badge.svg?token=n6X9zQIkXb)](https://codecov.io/gh/xcube-dev/xcube-clms)
[![Code style: black](https://img.shields.io/badge/code%20style-black-000000.svg)](https://github.com/psf/black)
[![License](https://img.shields.io/github/license/dcs4cop/xcube-smos)](https://github.com/xcube-dev/xcube-clms/blob/main/LICENSE)

The `xcube-clms` package is a Python package and a
[xcube plugin](https://xcube.readthedocs.io/en/latest/plugins.html) that
introduces
a [data store](https://xcube.readthedocs.io/en/latest/api.html#data-store-framework)
called `clms` to xcube. This data store enables access
to datasets hosted on [CLMS](https://land.copernicus.eu/en) through the
[CLMS API](https://eea.github.io/clms-api-docs/introduction.html).

## Setup <a name="setup"></a>

### Installing the xcube-clms plugin from the repository <a name="install_source"></a>

To install xcube-clms directly from the git repository, clone the repository,
`cd` into `xcube-clms`, and follow the steps below:

```bash
conda env create -f environment.yml
conda activate xcube-clms
pip install .
```

This sets up a new conda environment, installs all the dependencies required
for `xcube-clms`, and then installs `xcube-clms` directly from the repository
into the environment.

### Create credentials to access the CLMS API

Create the credentials as a `json` file required for the CLMS API following
the [documentation](https://eea.github.io/clms-api-docs/authentication.html).
The credentials will be required during the initialization of the CLMS data
store. Please follow the instructions in the
`example/notebooks/CLMSDataStoreTutorial.ipynb`,
on how to pass the credentials from the `json` file to the store.

## Testing <a name="testing"></a>

To run the unit test suite:

```bash
pytest
```