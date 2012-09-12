# casyn [![Build Status](https://secure.travis-ci.org/mpenet/casyn.png?branch=master)](http://travis-ci.org/mpenet/casyn)

Clojure client for Cassandra using Thrift AsyncClient.

The entire [Cassandra Thrift Api (1.1.5)](http://wiki.apache.org/cassandra/API) is
supported, this includes CQL support.

Pooling is using Apache commons pools, but it is open to other
implementations from clojure Protocols/multimethods, the same is true for almost
every part of the library (cluster, balancer, codecs, failover).

Contributions, suggestions and bug reports  are welcome.


## Installation

Casyn uses Leinigen 2, but it is compatible with 1.x

Add the following dependency on your project.clj:

```clojure
[cc.qbits/casyn "0.1.6"]
```

or if you want to try the dev version:

```clojure
[cc.qbits/casyn "0.1.7-SNAPSHOT"]
```

Note: It runs on Clojure 1.4+ and is being tested with Cassandra 1.1.5
(it should work fine with 1.x.x versions).

## Usage

Start by creating a playground:

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
;; timeout at this level or inherit thesse from cluster settings.

(def c (client-fn cl))
```

[More details about cluster configuration](http://mpenet.github.com/casyn/casyn.cluster.core.html)

API calls return [Lamina](https://github.com/ztellman/lamina) result-channels.
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

but it is advised to  use a pipeline to compose async/sync operations.
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

[Lamina](https://github.com/ztellman/lamina) offers a lot of possibilities.


Cassandra/Thrift column name/values are returned as bytes, but you can
supply a schema for decoding.
Encoding of clojure data is automatic.
Encoding/decoding is open and extendable, see codecs.clj.
This also works with CQL queries.

A simple example with a schema:

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

A collection of columns can be turned into a regular map just pass `:output :as-map`.


```clojure
@(c get-row "colFamily1" "7" :schema test-schema :output :as-map)

user> {:age 35
       :name "Max"
       :created #inst "2012-08-22T22:34:41.079-00:00"}
```

Supported types are `:string` `:long`  `:float`  `:double` `:int` `:boolean` `:keyword` `:bytes` `:date` `:uuid` `:time-uuid` `:composite` `:clj`

TimeUUIDs are supported from [tardis](https://github.com/mpenet/tardis), you will need to use its API to create Type 1 UUIDs, from there encoding/decoding is automatic.

Joda time support is available, you need to require/use `casyn.codecs.joda`, and use `:date-time` in your schemas.

Composite types are also supported and use the same type definitions
(they can be used as keys, names, values):

```clojure
(defschema test-schema
  :row :string
  :columns {:default [[:string :long :double] :string]}})
```

To create composite values just use the `composite` function or reader literal, it will
mark the collection as composite and encode it accordingly when you execute the
query.

```clojure
(c insert-column "colFamily1" "1"  (composite ["meh" 1001  3.14]) "value0")
  ;; consistency is tunable per query
  :consistency :all)
```

As shown in the previous example you can also store clojure data
direclty (it is the fallback of the encoding protocol), this will be
done via [Nippy](https://github.com/ptaoussanis/nippy), you will just
need to indicate :clj as decoding type in the schema.

### Convenience macros

`with-consistency` can be used if you prefer that to explicit arguments.

```clojure
(with-consistency :all
  @(c get-row "colFamily1" "1")
  @(c get-row "colFamily1" "2"))
 ```

## Documentation

See the [API documentation](http://mpenet.github.com/casyn/) or [tests](https://github.com/mpenet/casyn/blob/master/test/casyn/test/core.clj) for more details.

Some useful pages:

* [commands API](http://mpenet.github.com/casyn/casyn.api.html)

* [ddl API](http://mpenet.github.com/casyn/casyn.ddl.html)

* [cluster](http://mpenet.github.com/casyn/casyn.cluster.core.html)

* [Mailing list](https://groups.google.com/forum/#!forum/casyn)

Note: (almost) the entire lib is aliased from `casyn.core` so that you
can have everything you need with a single `require`, as seen on the examples.

## YourKit

Casyn is being developed with the help of YourKit profiler.

> YourKit is kindly supporting open source projects with its full-featured Java Profiler.
YourKit, LLC is the creator of innovative and intelligent tools for profiling
Java and .NET applications. Take a look at YourKit's leading software products:
[YourKit Java Profiler](http://www.yourkit.com/java/profiler/index.jsp)
[YourKit .NET Profiler](http://www.yourkit.com/.net/profiler/index.jsp).

## License

Copyright (C) 2012 Max Penet

Distributed under the Eclipse Public License, the same as Clojure.
