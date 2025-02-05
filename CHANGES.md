## Changes in 0.2.2

### Other Changes

* Dependency update: Upgraded `xcube` to version `>=1.8.1` to ensure
  compatibility

## Changes in 0.2.1

### Enhancements

* Improved performance in the `preprocessing` step of the `preload` API by
  adjusting the chunk size to be a divisor of the dataset size, rather than using
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
