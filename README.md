# xcube-clms

[![Unittest xcube-clms](https://github.com/xcube-dev/xcube-clms/actions/workflows/unittest.yml/badge.svg)](https://github.com/xcube-dev/xcube-clms/actions/workflows/unittest.yml)
[![Codecov xcube-clms](https://codecov.io/gh/xcube-dev/xcube-clms/graph/badge.svg?token=n6X9zQIkXb)](https://codecov.io/gh/xcube-dev/xcube-clms)
[![Code style: black](https://img.shields.io/badge/code%20style-black-000000.svg)](https://github.com/psf/black)
[![License](https://img.shields.io/github/license/dcs4cop/xcube-smos)](https://github.com/xcube-dev/xcube-clms/blob/main/LICENSE)

The `xcube-clms` Python package provides an 
[xcube data store](https://xcube.readthedocs.io/en/latest/api.html#data-store-framework)
that enables access to datasets hosted by the 
[Copernicus Land Monitoring Service (CLMS)](https://land.copernicus.eu/en).
The data store is called `"clms"` and implemented as an [xcube plugin](https://xcube.readthedocs.io/en/latest/plugins.html).
It uses the [CLMS API](https://eea.github.io/clms-api-docs/introduction.html)
under the hood.

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

## Additional Notes about the data store

This data store introduces the initial mechanism of preloading data, including cache management, downloading, and file processing.
This is currently experimental and will be changing in the newer versions.

This new additon of a preload interface is due to the nature of the CLMS API which allows the user to create data requests, with undetermined time to wait in the queue for the request to be processed, followed by downloading zip files, unzipping them, extracting them in a cache which can be then opened using a file store.

Preloading allows the data store to request the datasets for download to the CLMS API (in this data store) in a non-blocking way which handles sending the download request, queueing for download, waiting in the queue, periodically checking for the request status, downloading the data, extracting and post-processing it.

The preload mechanism can be used using `.preload_data(*data_ids)` on the CLMS data store instance.

The following classes (components) are responsible for this mechanism:

**CLMS**

- Serves as the main interface to interact with the CLMS API. This class coordinates with the PreloadData class to preload the data into a local filestore.

**CacheManager**

- Manages the local cache of preloaded data.
- Maintains a dictionary (cache) that maps data_ids to their respective file paths.
- Handles file store from the xcube data store in a local directory and refreshes the cache when necessary.

**DownloadTaskManager**

- Handles the download process, including managing download requests and checking their statuses.
- Retrieves task statuses based on dataset and file IDs or task IDs, determining whether the download is pending, completed, or cancelled.
- Initiates data downloads in chunks and manages zip file extraction, looking specifically for geo data. Definition of geo data is defined in the function docstring in the notes.

**ClmsApiTokenHandler**

- Handles the creation and refreshing of the CLMS API token given the credentials which can be obtained following the steps here

**FileProcessor**

- Handles the postprocessing of downloaded data, extracting, stacking and storing geo files from downloaded zip files.

**PreloadData**

- The main class responsible for orchestrating the preloading of datasets.
- It coordinates with _CacheManager_, _DownloadTaskManager_, _ClmsApiTokenHandler_ and _FileProcessor_ classes to handle the complete process of caching, data downloading, making sure token is valid and post-processing of downloaded data.
- Utilizes threading for handling multiple data preloading tasks concurrently.
- Uses notebook.tqdm for displaying progress bars

## CLMS API

- Requires an EU account to register on the CLMS site.
- Once registered, the user should create an access token json file as described ![here](https://eea.github.io/clms-api-docs/authentication.html)
- The user can now use this json credentials file with the CLMS store (in development)

## CLMS API issues
This API has some problems as listed below

- The datasets which are made available via requests, contain a download link to a zip file, which is valid only for 3 days. But we found that this is not true and we cannot rely on this time to make sure that the download link still works. So, we have to create a workaround to manage our own expiry times. This issue has been raised with the CLMS service desk. Quoting their reply For the first issue mentioned by you: `The status is completed and there is indicated that there are 2 days for expiring, but the download link is already expired, we are going to investigate this bug.`
- We use the API to figure out if a certain data_id has already been requested to the CLMS server and its status so that we can get the download link directly or if it has not been requested yet or expired, we request it. But this is also not possible because although on their web UI, we cannot see the old downloads that have expired, the API does return the expired requests which were completed and do not contain any information that they are expired or when they will expire. Quoting the CLMS helpdesk replies `For the second issue mentioned by you: the @datarequest_search endpoint does not seem to be working as expected, we are going to consult the API experts so to check its functioning and in case an improvement is feasible in our side, we´ll let you know.` and its follow up after a week `After having analysed the possibility to improve the status of the downloads, our team answers the following: Currently, our download system is not able to extract information on whether the link has expired or not, therefore our API does not provide this information.. Due to this, we had to create workarounds to figure out if a certain dataset's link was expired or not.`
- The cancel endpoint for the API does not work and the issue was raised with the helpdesk team as well. Quoting their reply `Recently a new firewall of the CLMS Portal machine has been setup. This new firewall is blocking some of the process cancelation request. We've detected the issue and working with the IT team to solve it`.
