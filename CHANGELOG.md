# Changelog

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
