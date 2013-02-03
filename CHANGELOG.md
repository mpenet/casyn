# Changelog

## 1.0.0

* Breaking change! Changed public api for composites the composite
  function is now `c*composite` and the format for composites
  definitions in the schemas changed as well:
  A composite schema used to be a vector, ex: `[:utf-8 :long :double]`
  it is now a map, ex: `{:composite [:utf-8 :long :double]}`.

* Add `shutdown` function to stop the cluster and clean its ressources
  (pools, connections, etc)

* Update dependencies to cassandra 1.2.1

* Add support for `atomic_batch_mutate` and `trace_next_query`

* Add 1.2 collection types support.

## 0.9.7

* Fix issue where mutation on the same row, from differents spec
  would be ignored.

## 0.9.6

*  casyn.api/batch-mutate signature updated to take a vector of
   mutations specs instead of the inverted nested map from the Thrift API.

   It used to be:
   ```clojure
   {"somekey" {"col-family" [mutations...]}
    "somekey" {"col-family" [mutations...]}
    "somekey" {"col-family" [mutations...]}}
   ```

   Now:
   ```clojure
   [["col-family" "somekey" [mutations...]
    ["col-family" "somekey" [mutations...]
    ["col-family" "somekey" [mutations...]]
   ```
