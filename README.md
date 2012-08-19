# casyn

Clojure client for Cassandra based on Thrift AsyncClient.

[![Build Status](https://secure.travis-ci.org/mpenet/casyn.png?branch=master)](http://travis-ci.org/mpenet/casyn)

It relies on the perf Lamina branch that hasnt been officialy released yet.
It is a work in progress,

The majority of the [Cassandra Api](http://wiki.apache.org/cassandra/API) is
supported, this includes CQL support.

Pooling is using Apache commons pools, but it is open to other
implementations from clojure Protocols/multimethods, the same is true for almost
every part of the library (cluster, balancer, codecs, failover).

Contributions and suggestions are welcome.

See clj-hector or some of the very mature java clients available if
you need a production ready library right now.

## Installation

Casyn uses Leinigen 2, but it is compatible with 1.x

Add the following dependency on your project.clj:

```clojure
[cc.qbits/casyn "0.1.3"]
```

Note: It runs on Clojure 1.4+

## Usage

Start by creating a playground for this introduction:

```clojure
(use 'casyn.core)

(add-keyspace (make-client)
               "Keyspace1"
               "SimpleStrategy"
               [["colFamily1"]
               ["colFamily2"
               :default-validation-class :counter
               :replicate-on-write true]]
               :strategy-options {"replication_factor" "1"})
user> < ... >
```

```clojure
(require '[lamina.core :as l])

(def cl (make-cluster "localhost" 9160 "Keyspace1"))

;; We now create a client function for our future requests
;; This will manage the node selection, connection pooling, and client
;; workflow for every command. It also allows you to set failover and
;; timeout at this level, separately from the cluster definition

(def c (client-fn cl))
```

API calls return result-channels.
This is an example of this. From there you have multiple choices
you can just have the call block and wait for a result/error by dereferencing it.

```clojure
@(c get-row "colFamily1" "1")
```

or since we want to play asynchronously register a callback

```clojure
(l/on-realized (c get-row "colFamily1" "1")
           #(println "It worked, row:" %)
           #(println "It failed, error:" %))
```

or use a pipeline to compose async/sync operations.
Here a write then reading the entire row.
A pipeline also returns a result-channel and can be nested with other
pipelines, making async workflow and error handling easier to deal with.

```clojure

@(l/run-pipeline
  (c insert-column "colFamily1" "1" "n0" "value0")
  {:error-handler (fn [_] (println "snap, something went wrong"))}
  (fn [_] (c get-row "colFamily1" "1")))


user> (#casyn.types.Column{:name #<byte[] [B@7cc09980>, :value #<byte[] [B@489de27c>, :ttl 0, :timestamp 1332535710069564})
  ```

[Lamina](https://github.com/ztellman/lamina) offers a lot of possibilities. I encourage you to check what is possible with it.


Cassandra/Thrift column name/values are returned as bytes, but you can supply a schema for
decoding.
Encoding of clojure data is automatic.
Encoding and decoding is open and extendable, see codecs.clj.
This also works for CQL queries.

The same example as before with a simple schema:

```clojure
(defschema test-schema
  :row :string
  :super :string
  :columns {:default [:string :string]
            ;; when a column with the age name is encountered it will
            ;; overwride the defaults for decoding
            :exceptions {"age" :long}})

@(l/run-pipeline
  (c insert-column "colFamily1" "1" "n0" "value0" :consistency :all)  ;; consistency is tunable per query
  (fn [_] (c get-row "colFamily1" "1"))
  #(decode-result % test-schema))

user> (#casyn.types.Column{:name "n0", :value "value0", :ttl 0, :timestamp 1332536503948650})
```

Schema supports `:string` `:long`  `:float`  `:double` `:int` `:boolean` `:keyword` `:clojure` `:bytes` `:date` `:uuid`

These are also extendable from a multimethod.

Composite types are also supported, use the same type definitions but in a vector (they can be used as keys, names, values):

```clojure
(defschema test-schema
  :row :string
  :columns {:default [:string :string]
            :exceptions {"age" :long
                         "test-composite-type" [:string :clojure :int]}})
```

To create composite values just use the `composite` function, it will just mark the collection as composite in its metadata, and encode it when you execute the query.
That means you could also create this composite collection beforehand, modify it without having to worry about anything (as long as the metadata is perserved).

```clojure
(c insert-column "colFamily1" "1" (composite ["meh" 1 :something 3.14 {:foo "bar"}] "value0"))
```

If you want a collection of columns to be turned into a regular map
you can use `cols->map` , :name and :value are then mapped to
key/value. You would no longer have access to the additional data such as
ttl or timestamp on the column.
Or you can just pass `true` as a third parameter to decode-result.


```clojure
@(l/run-pipeline
  (c get-row "colFamily1" "1")
  #(decode-result % test-schema true))

user> {"foo" "bar", "baz" "quux"}
```

## Documentation

See the [API documentation](http://mpenet.github.com/casyn/) or [tests](https://github.com/mpenet/casyn/blob/master/test/casyn/test/core.clj) for more details.

## License

Copyright (C) 2012 Max Penet

Distributed under the Eclipse Public License, the same as Clojure.
