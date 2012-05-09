# casyn

Clojure client library for Cassandra based on Thrift AsyncClient

It is a work in progress and a very early release.
It relies on the perf Lamina branch that hasnt been officialy released yet.

See clj-hector or some of the very mature java clients available for real world use.

At the moment the api is relatively low level, it will be improved.

The majority of the [Cassandra Api](http://wiki.apache.org/cassandra/API) is
supported though.

Pooling is using Apache commons pools, but it is open to other
implementations from clojure Protocols/multimethods, the same is true for almost
every part of the library (cluster, balancer, codecs, failover).

Some stuff is still in a very early stage such node discovery, and
error handling, I am still figuring this out from advanced clients out there.

Contributions and suggestions are welcome.

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
   you can just have the call block and wait for a result/error by dereferencing it

   ```clojure
   @(c get-row "1" "colFamily1")
   ```

   or since we want to play asynchronously register a callback

   ```clojure
   (on-success (c get-row "1" "colFamily1")
               #(println "It worked, row:" %))
   ```

   or use a pipeline to compose async/sync operations.
   Here a write then reading the entire row.
   A pipeline also returns a result-channel and can be nested with other
   pipelines, making async workflow easier to deal with.

   ```clojure

(l/run-pipeline
  (c insert-column "1" "colFamily1" (column "n0" "value0"))
  {:error-handler (fn [_] (println "snap, something went wrong"))}
  (fn [_] (c get-row "1" "colFamily1")))

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

(defschema test-schema
  :row :string
  :super :string
  :columns
{:default [:string :string]
 ;; when a column with the age name is encountered it will overwrite the defaults for decoding
 :exceptions {"age" :long}})

(l/run-pipeline
  (c insert-column "1" "colFamily1" (column "n0" "value0") :consistency :all)
  :on-error (fn [_] (println "something went wrong"))
  (fn [_] (c get-row "1" "colFamily1"))
  #(decode-result % test-schema))

 user> #casyn.types.Column{:name "n0", :value "value0", :ttl 0, :timestamp 1332536503948650}
   ```

   Schema supports `:string` `:long`  `:float`  `:double` `:int` `:boolean` `:keyword` `:clojure` `:symbol` `:bytes`
   These are also extendable from a multimethod.

   If you want a collection of columns to be turned into a regular map
   you can use `cols->map` , :name and :value are then mapped to
   key/value. You have no longer access to the additional data such as
   ttl or timestamp on the column though.

   See See [tests](https://github.com/mpenet/casyn/blob/master/test/casyn/test/core.clj),  [api.clj](https://github.com/mpenet/casyn/blob/master/src/casyn/api.clj) and [codox doc](http://mpenet.github.com/casyn/) for more details.

## License

Copyright (C) 2012 Max Penet

Distributed under the Eclipse Public License, the same as Clojure.
