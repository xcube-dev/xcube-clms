## Changes in 1.1.0 (in development)

### Enhancements

* The `xcube-clms` now supports datasets from the `LEGACY` sources of the
  [CLMS API.](https://eea.github.io/clms-api-docs/download.html#auxiliary-api-to-get-direct-download-links-for-non-eea-hosted-datasets)

### Other changes

* Performed internal code refactoring for improved maintainability

## Changes in 1.0.0

### Major Changes

* The `preload_data` method now returns a store containing the preloaded data.
  After preloading, the returned store may be used to access the data. (#21)

### Other Changes

* `clms_store.list_data_ids()` now only returns data ids that have downloadable
  assets. (#21)
* Ensures consistency of data_ids between the CLMS data store and the preloaded
  data store. (#21)
* Preload jobs now always restart from scratch, regardless of whether a dataset
  was previously preloaded. Any existing preloaded data will be overwritten.

## Changes in 0.2.2

### Other Changes

* Dependency update: Upgraded `xcube` to version `>=1.8.1` to ensure
  compatibility

## Changes in 0.2.1

### Enhancements

* Improved performance in the `preprocessing` step of the `preload` API by
  adjusting the chunk size to be a divisor of the dataset size, rather than
  using the original chunk size of the GeoTIFF file.

## Changes in 0.2.0

### Enhancements

* Implemented the new experimental `Preload` API in xcube for improved
  performance.
* Preload progress is now displayed in a user-friendly table format. This
  display can be disabled.
* All preloaded data is now stored in the `.zarr` format.

## Changes in 0.1.0 (not released)

* Initial version of CLMS Data Store with a new experimental Preload API for
  preloading the datasets.
