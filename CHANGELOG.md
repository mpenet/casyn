# Changelog

## 1.1.3

* update dependencies (lamina, hayt, cassandra-all)

## 1.1.2

* fix yammer/metric-core dependency breakage

## 1.1.0

* Add [Hayt](https://github.com/mpenet/hayt) 0.1.1 to the dependencies

## 1.0.2

* Update to lamina "0.5.0-beta12" (perf improvements)

## 1.0.1

* Removed clj-time from the main dependencies

## 1.0.0

* Add `shutdown` function to stop the cluster and clean its ressources
  (pools, connections, etc)

* Update dependencies to cassandra 1.2.1, useful 0.8.8

* Add support for `atomic_batch_mutate` and `trace_next_query`

* Composites performance improvements

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
