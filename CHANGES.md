## Changes in 1.0.0

### Major Changes

* The `preload_data` method now returns a store containing the preloaded data.
  After preloading, the returned store may be used to access the data.

### Other Changes

* `clms_store.list_data_ids()` now only returns data ids that have downloadable
  assets.
* Ensured consistency of data_ids between the CLMS data store and the preloaded
  data store returned from it.
* Preload jobs now always restart from the beginning to prevent treating
  incomplete or corrupted files as successfully preloaded.

## Changes in 0.2.2

### Other Changes

* Dependency update: Upgraded `xcube` to version `>=1.8.1` to ensure
  compatibility

## Changes in 0.2.1

### Enhancements

* Improved performance in the `preprocessing` step of the `preload` API by
  adjusting the chunk size to be a divisor of the dataset size, rather than
  using
  the original chunk size of the GeoTIFF file.

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
