# casyn
[![Build Status](https://secure.travis-ci.org/mpenet/casyn.png?branch=master)](http://travis-ci.org/mpenet/casyn)

Clojure client for Cassandra using Thrift AsyncClient.

The entire [Cassandra Thrift Api (1.1.9)](http://wiki.apache.org/cassandra/API) is
supported, this includes CQL support.
See [commands API](http://mpenet.github.com/casyn/qbits.casyn.api.html) for details.

Pooling is using Apache commons pools, but it is open to other
implementations from clojure Protocols/multimethods, the same is true for almost
every part of the library (cluster, balancer, codecs, failover).

Contributions, suggestions and bug reports  are welcome.


## Installation

Casyn uses Leinigen 2, but it is compatible with 1.x

Add the following dependency on your project.clj:

```clojure
[cc.qbits/casyn "1.0.0-rc4"]
```

Note: It runs on Clojure 1.4+ and is being tested with Cassandra 1.1.9
(it should work fine with 1.x.x versions).

## Usage

### Basics

```clojure
(use 'qbits.casyn)

(def cl (make-cluster "localhost" 9160 "Keyspace1"))

;; We now create a client function for our future requests
;; This will manage the node selection, connection pooling, and client
;; workflow for every command. It also allows you to set failover and
;; timeout at this level or inherit these from cluster settings.

(def c (client-fn cl))
```

[More details about cluster configuration](http://mpenet.github.com/casyn/casyn.cluster.core.html)
and [client-fn](http://mpenet.github.com/casyn/casyn.client.html)

API calls return [Lamina](https://github.com/ztellman/lamina) result-channels.
This is an example of this. From there you have multiple choices
you can just have the call block and wait for a result/error by dereferencing it.

```clojure
@(c get-row "colFamily1" "1")
```

or since we want to play asynchronously register a callback

```clojure
(require '[lamina.core :as l])

(l/on-realized (c get-row "colFamily1" "1")
           #(println "It worked, row:" %)
           #(println "It failed, error:" %))
```

but it is often better to  use a pipeline to compose async/sync operations.
Here a write then reading the entire row.
A pipeline also returns a result-channel and can be nested with other
pipelines, making async workflow and error handling easier to deal with.

```clojure
@(l/run-pipeline
  (c insert-column "colFamily1" "1" "n0" "value0")
  {:error-handler (fn [_] (println "snap, something went wrong"))}
  (some-other-async-operation)
  (fn [result] (c get-row "colFamily1" (:id result))))

user> ({:name #<byte[] [B@7cc09980>
        :value #<byte[] [B@489de27c>
        :ttl 0
        :timestamp 1332535710069564})
```

[Lamina](https://github.com/ztellman/lamina) offers a lot of possibilities.

### Encoding/decoding - Schemas

Cassandra/Thrift column name/values are returned as bytes, but you can
supply a schema for decoding.
Encoding of clojure data is automatic.
Encoding/decoding is open and extendable, see codecs.clj.
This also works with CQL query results.

A simple example with a schema:

```clojure
(defschema test-schema
  :row :utf-8 ;; the row key will be decoded as utf-8
  :columns {:default [:keyword :utf-8]
            ;; :keyword type will be the default decoder for the column name
            ;; and :utf-8 will be the decoder type of the column value

            ;; When a column with the age name is encountered it will
            ;; overwride the defaults for decoding, the name the column is the
            ;; key and the value its type. In this example the key is a keyword,
            ;; but this depends on the column :default you set
            ;; earlier, it could be of any type.
            :exceptions {:age :long
                         :created :date
                         :code :clj}})

@(c put "colFamily1" "7"
    {:age 35
     :name "Max"
     :created (java.util.Date.)
     :code {:foo [{:bar "baz"}]}})

@(c get-row "colFamily1" "7" :schema test-schema)

user> ({:name :age, :value 35, :ttl 0, :timestamp 1332536503948650}
       {:name :name, :value "Max", :ttl 0, :timestamp 1332536503948652})
       {:name :created, :value #inst "2012-08-22T22:34:41.079-00:00", :ttl 0, :timestamp 1332536503948651
       {:name :code, :value {:foo [{:bar "baz"}]}, :ttl 0, :timestamp 1332536503948652}}
```

A collection of columns can be turned into a regular map just pass `:as :map`.


```clojure
@(c get-row "colFamily1" "7" :schema test-schema :as :map)

user> {:age 35
       :name "Max"
       :created #inst "2012-08-22T22:34:41.079-00:00"
       :code {:foo [{:bar "baz"}]}}
```

#### Supported types
`:utf-8` `:ascii` `:long` `:float` `:double` `:int` `:boolean`
`:keyword` `:bytes` `:date` `:uuid` `:time-uuid` `:composite` `:clj`

Note about ASCII:
Clojure Strings are by default encoded as utf-8, ASCII strings must be passed as
Bytes ex: `(.getBytes "meh" "US-ASCII")`, specifying `:ascii` on the
schema allow their automatic decoding though. If you want the
read/write behavior to be symetrical just use `:bytes` as schema type
and handle this on your side.

#### TimeUUIDs

TimeUUIDs are supported from
[tardis](https://github.com/mpenet/tardis), you will need to use its
API to create Type 1 UUIDs, from there encoding/decoding is automatic.

#### Joda Time

Joda time support is available, you need to require/use
`qbits.casyn.codecs.joda-time`, and use `:date-time` in your schemas.

#### Composite types

Composite types are also supported and use the same type definitions
(they can be used as keys, names, values), instead of specifying a
single type value in the schema just use a vector or types:
Here the column name will be a composite or 3 different types.

```clojure
(defschema test-schema
  :row :utf-8
  :columns {:default [[:utf-8 :long :double] :utf-8]}})
```

To create composite values just use the `composite` function or
`#casyn/composite` reader literal, it will mark the collection as
composite and encode it accordingly when you execute the query.

```clojure
(c insert-column "colFamily1" "1"  (composite ["meh" 1001  3.14]) "value0"))
```

Querying using composite values is also supported, for a brief
overview see
[tests.clj](https://github.com/mpenet/casyn/blob/master/test/qbits/casyn/test/core.clj#L294)
or the
[documentation](http://mpenet.github.com/casyn/qbits.casyn.codecs.composite.html).

#### Clojure serialization

As shown in the previous example you can also store clojure data
direclty (it is the fallback of the encoding protocol), this will be
done via [Nippy](https://github.com/ptaoussanis/nippy), you will just
need to indicate :clj as decoding type in the schema.


#### Extending type support

The Joda time support is a [good example](https://github.com/mpenet/casyn/blob/master/src/qbits/casyn/codecs/joda_time.clj)
of how it is achieved.

You need to do two things, extend the encoding protocol, and add a
`defmethod` definition for decoding with the keyword you will use in your
schemas.

```clojure
(ns qbits.casyn.codecs.joda-time
  "Encoding/decoding of org.joda.time.DateTime instances"
  (:require
   [qbits.casyn.codecs :refer [ByteBufferEncodable
                         bytes->clojure
                         clojure->byte-buffer]]
   [clj-time.coerce :as ct-c]))

(extend-protocol ByteBufferEncodable
  org.joda.time.DateTime
  (clojure->byte-buffer [dt]
    (clojure->byte-buffer (ct-c/to-long dt))))

(defmethod bytes->clojure :date-time [_ b]
  (ct-c/from-long (bytes->clojure :long b)))

```

<!-- ##### Advanced -->

<!-- The default Object extension of the ByteBufferEncodable protocol -->
<!-- allows you to go further: you can use metadata on the values to -->
<!-- use another encoder. This is what is used for composite types, and -->
<!-- what will be used to handle cassandra 1.2 collection types. -->
<!-- It will dispatch on the qbits.casyn.codecs/meta-encodable multimethod -->
<!-- with `(-> value meta :casyn :type)` of your value. -->

<!-- See -->
<!-- [composite.clj](https://github.com/mpenet/casyn/blob/master/src/qbits/casyn/codecs/composite.clj) -->
<!-- for details. -->


### Consistency

Consistency is done per query, and is supported on all commands.

```clojure
(c insert-column "cf" "1" "foo" "bar" :consistency :all)
```

Or using the following macros:

`with-consistency` can be used if you prefer that to explicit arguments.

```clojure
(with-consistency :all
  @(c get-row "colFamily1" "1")
  @(c get-row "colFamily1" "2"))
 ```

The possible values are: `:all` `:any` `:each-quorum` `:local-quorum`
`:one` `:quorum` `:three` `:two`

Refer to
[Cassandra API doc](http://wiki.apache.org/cassandra/API#Write) for
details.

## Documentation

See the [API documentation](http://mpenet.github.com/casyn/) or [tests](https://github.com/mpenet/casyn/blob/master/test/qbits/casyn/test/core.clj) for more details.

Some useful pages:

* [commands API](http://mpenet.github.com/casyn/qbits.casyn.api.html)

* [ddl API](http://mpenet.github.com/casyn/qbits.casyn.ddl.html)

* [cluster](http://mpenet.github.com/casyn/qbits.casyn.cluster.core.html)

* [Mailing list](https://groups.google.com/forum/#!forum/casyn)

Note: (almost) the entire lib is aliased from `qbits.casyn` so that you
can have everything you need with a single `require`, as seen on the examples.

## Changelog

See [CHANGELOG.md](https://github.com/mpenet/casyn/blob/master/CHANGELOG.md)

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
