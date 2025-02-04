## Changes in 0.2.1

### Enhancements

* Improved performance in the `preprocessing` step of the `Preload` API by
  deriving the chunk size instead of using the original chunk size of the file,
  thus reducing processing time.

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