# xcube-clms

[![Unittest xcube-clms](https://github.com/xcube-dev/xcube-clms/actions/workflows/unittest.yml/badge.svg)](https://github.com/xcube-dev/xcube-clms/actions/workflows/unittest.yml)
[![Anaconda-Server Badge](https://anaconda.org/conda-forge/xcube-clms/badges/version.svg)](https://anaconda.org/conda-forge/xcube-clms)
[![Codecov xcube-clms](https://codecov.io/gh/xcube-dev/xcube-clms/graph/badge.svg?token=n6X9zQIkXb)](https://codecov.io/gh/xcube-dev/xcube-clms)
[![Code style: black](https://img.shields.io/badge/code%20style-black-000000.svg)](https://github.com/psf/black)
[![License](https://img.shields.io/github/license/dcs4cop/xcube-smos)](https://github.com/xcube-dev/xcube-clms/blob/main/LICENSE)

The `xcube-clms` Python package provides an
[xcube data store](https://xcube.readthedocs.io/en/latest/api.html#data-store-framework)
that enables access to datasets hosted by the
[Copernicus Land Monitoring Service (CLMS)](https://land.copernicus.eu/en).
The data store is called `"clms"` and implemented as
an [xcube plugin](https://xcube.readthedocs.io/en/latest/plugins.html).
It uses the [CLMS API](https://eea.github.io/clms-api-docs/introduction.html)
under the hood.

## Setup <a name="setup"></a>

### Installing the xcube-clms plugin from the repository <a name="install_source"></a>

To install xcube-clms directly from the git repository, clone the repository,
`cd` into `xcube-clms`, and follow the steps below:

```bash
git clone https://github.com/xcube-dev/xcube-clms.git
cd xcube-clms
conda env create -f environment.yml
conda activate xcube-clms
pip install .
```

This sets up a new conda environment, installs all the dependencies required
for `xcube-clms`, and then installs `xcube-clms` directly from the repository
into the environment.

### Installing the xcube-clms plugin from the conda-forge

This method assumes that you have an existing environment, and you want to
install `xcube-clms` into it.
With the existing environment activated, execute this command:

```bash
mamba install --channel conda-forge xcube-clms
```

If xcube and any other necessary dependencies are not already instelled, they
will be installed automatically.

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

## Some notes on the strategy of unit-testing for some tests

The unit test suite
uses [pytest-recording](https://pypi.org/project/pytest-recording/) to mock
https requests via the Python
library requests. During development an actual HTTP request is performed and the
responses are saved in cassettes/**.yaml files. During testing, only the
cassettes/**.yaml files are used without an actual HTTP request. During
development, to save the responses to cassettes/**.yaml, run:

```bash
pytest -v -s --record-mode new_episodes
```

Note that --record-mode new_episodes overwrites all cassettes. If one only wants
to write cassettes which are not saved already, --record-mode once can be used.
pytest-recording supports all records modes given
by [VCR.py](https://vcrpy.readthedocs.io/en/latest/usage.html#record-modes.
After recording the
cassettes, testing can be then performed as usual.

## Additional Notes about the data store

This data store currently only supports some dataset sources from the CLMS API:

- EEA
- CDSE

NOTE: More dataset sources will be supported in newer versions

### EEA

This data store introduces the initial mechanism of preloading data, including
cache management, downloading, and file processing.
This uses the experimental Preload API from the xcube data store.

This new addition of a preload interface is due to the nature of the CLMS API
which allows the user to create data requests, with undetermined time to wait in
the queue for the request to be processed, followed by downloading zip files,
unzipping them, extracting them in a cache and processing them which can be then
finally opened using a cache data store for `EEA pre-packaged` data sources.
The default is `file` data store stored at `/clms_cache` location in your `cwd`,
but the users are free to choose their data store of their liking.

Preloading allows the data store to request the datasets for download to the
CLMS API in both blocking/non-blocking way which handles sending the download
request, queueing for download, waiting in the queue, periodically checking for
the request status, downloading the data, extracting and post-processing it.

The preload mechanism can be used using
`.preload_data(*data_ids, **preload_params)` on the CLMS data store instance.

### CDSE

For datasets available via the `CDSE` source, they can be lazily loaded
directly using `open_data(...)`.

Currently we only support the following dataset(s):

- Daily Surface Soil
  Moisture (https://land.copernicus.eu/en/products/soil-moisture/daily-surface-soil-moisture-v1.0#general_info)

## CLMS API

- Requires an EU account to register on the CLMS site.
- Once registered, the user should create an access token json file as
  described [here](https://eea.github.io/clms-api-docs/authentication.html)

## CLMS API issues

This API has some problems as listed below

- The datasets which are made available via requests, contain a download link to
  a zip file, which is valid only for 3 days. But we found that this is not true
  and we cannot rely on this time to make sure that the download link still
  works. So, we have to create a workaround to manage our own expiry times. This
  issue has been raised with the CLMS service desk. Quoting their reply For the
  first issue mentioned by you:

  ```
  The status is completed and there is indicated that there are 2 days for
  expiring, but the download link is already expired, we are going to
  investigate this bug.
  ```
- We use the API to figure out if a certain data_id has already been requested
  to the CLMS server and its status so that we can get the download link
  directly or if it has not been requested yet or expired, we request it. But
  this is also not possible because although on their web UI, we cannot see the
  old downloads that have expired, the API does return the expired requests
  which were completed and do not contain any information that they are expired
  or when they will expire. Quoting the CLMS helpdesk replies

  ```
  For the second issue mentioned by you: the @datarequest_search endpoint does
  not seem to be working as expected, we are going to consult the API experts so
  to check its functioning and in case an improvement is feasible in our side,
  we´ll let you know.
  ```

  and its follow up after a week

  ```
  After having analysed the possibility to improve the status of the
  downloads, our team answers the following: Currently, our download system is
  not able to extract information on whether the link has expired or not,
  therefore our API does not provide this information.. Due to this, we had to
  create workarounds to figure out if a certain dataset's link was expired or
  not.
  ```
- The cancel endpoint for the API does not work and the issue was raised with
  the helpdesk team as well. Quoting their reply

  ```
  Recently a new firewall of the CLMS Portal machine has been setup. This new
  firewall is blocking some of the process cancelation request. We've detected
  the issue and working with the IT team to solve it
  ```

- CLMS API will has now started moving datasets to CDSE and WEkEO infrastructure
  which leads to the previously working datasets via CLMS to give an error. Upon
  requesting them with more information, this is what they replied:

  ```
  The future of the CLMS website is to have the Global datasets and the Pan
  European datasets centralised in as few repositories as possible (which is not
  the current situation). For this, some of the datasets (and new recently
  produced datasets) have been moved from EEA´s infrastructure to WEKEO´s
  infrastructure.
  
  Other datasets (Global datasets for the moment) are being moved to CDSE and the
  CLMS website is being adapted to this new situation.
  Regarding the datasets for which in the dataset page the explanation is that the
  dataset is only accessible through the WEKEO external site, the CLMS API can´t
  be used right now.
  
  WEKEO has increased the download limits imposed to the CLMS website to retrieve
  data from there, so maybe in a near future we´ll be able to offer the datasets
  from there in a seamless manner, and the direct download of these datasets will
  be enabled from the CLMS website (but it is not the case right now).
  
  For the datasets that are being migrated to CDSE, we are currently analysing the
  required adaptations on the CLMS website for users to keep downloading the data
  in a seamless manner. For the moment we don’t directly offer any dataset through
  CDSE.
  ```