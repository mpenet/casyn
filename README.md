# casyn

Clojure client for Cassandra using Thrift AsyncClient.

[![Build Status](https://secure.travis-ci.org/mpenet/casyn.png?branch=master)](http://travis-ci.org/mpenet/casyn)

It relies on the perf branch of
[Lamina](https://github.com/ztellman/lamina) which hasn't been
officialy released yet.

The entire [Cassandra Thrift Api](http://wiki.apache.org/cassandra/API) is
supported, this includes CQL support.

Pooling is using Apache commons pools, but it is open to other
implementations from clojure Protocols/multimethods, the same is true for almost
every part of the library (cluster, balancer, codecs, failover).

It is a work in progress. Contributions and suggestions are welcome.


## Installation

Casyn uses Leinigen 2, but it is compatible with 1.x

Add the following dependency on your project.clj:

```clojure
[cc.qbits/casyn "0.1.4"]
```

or if you want to try the dev version:

```clojure
[cc.qbits/casyn "0.1.5-SNAPSHOT"]
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


Cassandra/Thrift column name/values are returned as bytes, but you can
supply a schema for decoding.
Encoding of clojure data is automatic.
Encoding/decoding is open and extendable, see codecs.clj.
This also works for CQL queries.

The same example as before with a simple schema:

```clojure
(defschema test-schema
  :row :string
  :columns {:default [:keyword :string]
            ;; when a column with the age name is encountered it will
            ;; overwride the defaults for decoding
            :exceptions {:age :long
                         :created :date}})

@(c put "colFamily1" "7" {:age 35 :name "Max" :created (java.util.Date.)})
@(c get-row "colFamily1" "7" :schema test-schema)

user> (#casyn.types.Column{:name :age, :value 35, :ttl 0, :timestamp 1332536503948650}
       #casyn.types.Column{:name :name, :value "Max", :ttl 0, :timestamp 1332536503948652})
       #casyn.types.Column{:name :created, :value #inst "2012-08-22T22:34:41.079-00:00", :ttl 0, :timestamp 1332536503948651}
```

A collection of columns can be turned into a regular map just pass `:as-map true`.


```clojure
@(c get-row "colFamily1" "7" :schema test-schema :as-map)

user> {:age 35
       :name "Max"
       :created #inst "2012-08-22T22:34:41.079-00:00"}
```

Supported types are `:string` `:long`  `:float`  `:double` `:int` `:boolean` `:keyword` `:bytes` `:date` `:uuid :composite` `:clj`

These are extendable from a multimethod.

Composite types are also supported and use the same type definitions
(they can be used as keys, names, values):

```clojure
(defschema test-schema
  :row :string
  :columns {:default [:string :string] ;; column name/value
            :exceptions {"test-composite-type" [:string :clj :int]}})
```

To create composite values just use the `composite` function or reader literal, it will
mark the collection as composite and encode it accordingly when you execute the
query.

```clojure
(c insert-column "colFamily1" "1" #composite["meh" 1 :something 3.14 {:foo "bar"}] "value0")
  ;; consistency is tunable per query
  :consistency :all)
```

As shown in the previous example you can also store clojure data
direclty (it is the fallback or the encoding protocol), this will be
done via [Nippy](https://github.com/ptaoussanis/nippy), you will just
need to indicate :clj as decoding type in the schema.

### Convenience macros

`with-consistency` or `with-client` can be used to bind their
respective values if you prefer that to explicit arguments.

```clojure
(with-client c
  (execute-cql-query "SELECT * FROM test_cf;" :schema test-schema))

(with-consistency :all
  @(c get-row "colFamily1" "1")
  @(c get-row "colFamily1" "2"))
 ```

## Documentation

See the [API documentation](http://mpenet.github.com/casyn/) or [tests](https://github.com/mpenet/casyn/blob/master/test/casyn/test/core.clj) for more details.

## License

Copyright (C) 2012 Max Penet

Distributed under the Eclipse Public License, the same as Clojure.
