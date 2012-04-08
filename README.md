# casyn

Clojure client library for Cassandra based on Thrift AsyncClient

It is a work in progress and a very early release.
It relies on the perf Lamina branch that hasnt been officialy released yet.

See clj-hector or some of the very mature java clients available for real world use.

At the moment the api is relatively low level, it will be improved.

The majority of the [Cassandra Api](http://wiki.apache.org/cassandra/API) is
supported though.
The only missing parts are get_indexed_slices and helpers to deal with
composite types.

Pooling is using Apache commons pools, but it is open to other
implementations from clojure Protocols/multimethods, the same is true for almost
every part of the library (cluster, balancer, codecs, failover).

Some stuff is still in a very early stage such node discovery, and
error handling, I am still figuring this out from advanced clients out there.

Contributions and suggestions are welcome.

## Usage

   Start by creating a playground for this introduction:

   ```clojure
(require '[casyn.client :as client]
         '[casyn.ddl :as ddl])

(ddl/add-keyspace (client/make-client)
                  "Keyspace1"
                  "SimpleStrategy"
                  [["colFamily1"]
                  ["colFamily2"
                  :default-validation-class :counter
                  :replicate-on-write true]]
                  :strategy-options {"replication_factor" "1"})
user> < ... >
   ```

   Every call will return a [Result Channel](https://github.com/ztellman/lamina/wiki/Result-Channels) which relies on a
   org.apache.thrift.async.AsyncMethodCallback under the hood. So it
   will return immediately and emit a success or error at some point.

   ```clojure
(require '[casyn.core :as core]
         '[casyn.client :as client]
         '[casyn.cluster :as cluster]
         '[lamina.core :as l])

(def cl (cluster/make-cluster "localhost" 9160 "Keyspace1"))

;; We now create a client "executor" for our future requests
;; This will manage the node selection, connection pooling, and client
;; workflow for every command. It also allows you to set failover and
;; timeout at this level, separately from the cluster definition

(def cx (client/client-executor cl))
```

   API calls return result-channels.
   This is an example of this. From there you have multiple choices
   you can just have the call block and wait for a result/error by dereferencing it

   ```clojure
   @(cx core/set-keyspace "Keyspace1")
   ```

   or since we want to play asynchronously register a callback

   ```clojure
(on-success (cx core/set-keyspace "Keyspace1")
            #(println %))
   ```

   or use a pipeline to compose async/sync operations.
   Here a write then reading the entire row.
   A pipeline also returns a result-channel and can be nested with other
   pipelines, making async workflow easier to deal with.

   ```clojure

(lc/run-pipeline
  (cx core/insert-column "1" "colFamily1" (core/column "n0" "value0"))
  {:error-handler (fn [_] (println "snap, something went wrong"))}
  (fn [_] (cx core/get-row "1" "colFamily1")))

user> < .. >
user> #casyn.types.Column{:name #<byte[] [B@7cc09980>, :value #<byte[] [B@489de27c>, :ttl 0, :timestamp 1332535710069564}
  ```

  [Lamina](https://github.com/ztellman/lamina) offers a lot of possibilities, I encourage you to check what is possible with it.


  Cassandra/Thrift column name/values are returned as bytes, but you can supply a schema for
  decoding.
  Encoding of clojure data is automatic.
  Encoding and decoding open and extendable, see codecs.clj.

  The same example as before with a simple schema:

  ```clojure
(require '[casyn.schema :as s])

(s/defschema test-schema
  :row :string
  :super :string
  :columns
{:default [:string :string]
 ;; when a column with the age name is encountered it will overwrite the defaults for decoding
 :exceptions {"age" :long}})

(lc/run-pipeline
  (cx core/insert-column "1" "colFamily1" (core/column "n0" "value0") :consistency :all)
  :on-error (fn [_] (println "something went wrong"))
  (fn [_] (cx core/get-row "1" "colFamily1"))
  #(s/decode-result % test-schema))

 user> #casyn.types.Column{:name "n0", :value "value0", :ttl 0, :timestamp 1332536503948650}
   ```

   Schema supports `:string` `:long`  `:float`  `:double` `:int`  `:clojure` `:bytes`
   These are also extendable from a multimethod.

   See See [tests](https://github.com/mpenet/casyn/blob/master/test/casyn/test/core.clj) and  [core.clj](https://github.com/mpenet/casyn/blob/master/src/casyn/core.clj) for more details.


## License

Copyright (C) 2012 Max Penet

Distributed under the Eclipse Public License, the same as Clojure.
