(ns casyn.api
  "Thrift API and related utils. Most of the fns here translate almost directly
to their thrift counterpart and most of the time return Thrift instances.
See: http://wiki.apache.org/cassandra/API \nand
http://javasourcecode.org/html/open-source/cassandra/cassandra-0.8.1/org/apache/cassandra/thrift/Cassandra.AsyncClient.html"
  (:require
   [lamina.core :as lc]
   [casyn.utils :as utils]
   [casyn.codecs :as codecs]
   [casyn.schema :as schema])

  (:import
   [org.apache.cassandra.thrift
    Column SuperColumn CounterSuperColumn CounterColumn ColumnPath
    ColumnOrSuperColumn ColumnParent Mutation Deletion SlicePredicate
    SliceRange KeyRange AuthenticationRequest Cassandra$AsyncClient
    IndexClause IndexExpression IndexOperator ConsistencyLevel Compression]
   [org.apache.thrift.async AsyncMethodCallback TAsyncClient]
   [java.nio ByteBuffer]))

(def ^:dynamic consistency-default :one)

(defn consistency-level [c]
  ((or c consistency-default)
   {:all ConsistencyLevel/ALL
    :any ConsistencyLevel/ANY
    :each-quorum ConsistencyLevel/EACH_QUORUM
    :local-quorum ConsistencyLevel/LOCAL_QUORUM
    :one ConsistencyLevel/ONE
    :quorum ConsistencyLevel/QUORUM
    :three ConsistencyLevel/THREE
    :two ConsistencyLevel/TWO}))

;; Async helper

(defmacro wrap-result-channel
  "Wraps a form in a Lamina result-channel, and make the last arg of the form an
   AsyncMethodCallback with error/complete callback bound to the result-channel"
  [form]
  (let [thrift-cmd-call (gensym)
        result-hint (format "org.apache.cassandra.thrift.Cassandra$AsyncClient$%s_call"
                            (-> form first str (subs 1)))]
    `(let [result-ch# (lc/result-channel)]
       (~@form ^"org.apache.thrift.async.AsyncMethodCallback"
               (reify AsyncMethodCallback
                 (onComplete [_ ~thrift-cmd-call]
                   (lc/success result-ch#
                                (.getResult ~(with-meta thrift-cmd-call {:tag result-hint}))))
                 (onError [_ error#]
                   (lc/error result-ch# error#))))
       (lc/run-pipeline
        result-ch#
        {:error-handler (fn [_#])}
        codecs/thrift->clojure))))

;; Objects

(defn column
  "Returns a Thrift Column instance"
  [name value & {:keys [ttl timestamp]}]
  (let [col (Column. ^ByteBuffer (codecs/clojure->byte-buffer name))]
    (.setValue col ^ByteBuffer (codecs/clojure->byte-buffer value))
    (.setTimestamp col (or timestamp (utils/ts)))
    (when ttl (.setTtl col (int ttl)))
    col))

(defn counter-column
  "Returns a Thrift CounterColumn instance"
  ^CounterColumn
  [name value]
  (CounterColumn. (codecs/clojure->byte-buffer name)
                  (long value)))

(defn super-column
  "Returns a Thrift SuperColumn instance"
  ^SuperColumn
  [name columns]
  (SuperColumn. (codecs/clojure->byte-buffer name)
                (map #(apply column %) columns)))

(defn counter-super-column
  ""
  [name columns]
  (CounterSuperColumn. (codecs/clojure->byte-buffer name)
                       (map #(apply counter-column %) columns)))

(defn column-parent
  "Returns a Thrift ColumnParent instance, api fns accept a vector or a single
 value that will be applied to this fn"
  ^ColumnParent
  ([^String cf sc]
     (doto (ColumnParent. cf)
       (.setSuper_column ^ByteBuffer (codecs/clojure->byte-buffer sc))))
  ([^String cf] (ColumnParent. cf))
  ([] (ColumnParent.)))

(defn super-column-path
  "Returns a Thrift SuperColumnPath instance, api fns accept a vector or a single
 value that will be applied to this"
  [^String cf sc]
  (doto (ColumnPath. cf)
    (.setSuper_column ^ByteBuffer (codecs/clojure->byte-buffer sc))))

(defn column-path
  "Returns a Thrift ColumnPath instance, api fns accept a vector or a single
 value that will be applied to this fn"
  ^ColumnPath
  ([^String cf sc c]
     (doto ^ColumnPath (column-path cf c)
       (.setSuper_column ^ByteBuffer (codecs/clojure->byte-buffer sc))))
  ([^String cf c]
     (doto ^ColumnPath (column-path cf)
       (.setColumn ^ByteBuffer (codecs/clojure->byte-buffer c))))
  ([^String cf]
     (ColumnPath. cf)))

(defn columns-by-names
  ""
  [& column-names]
  (doto (SlicePredicate.)
    (.setColumn_names (map codecs/clojure->byte-buffer column-names))))

(defn columns-by-range
  "Returns a Thrift SlicePredicate instance for a range of columns"
  [& {:keys [start finish reversed count]}]
  (doto (SlicePredicate.)
    (.setSlice_range (SliceRange. (codecs/clojure->byte-buffer start)
                                  (codecs/clojure->byte-buffer finish)
                                  (boolean reversed)
                                  (int (or count 100))))))

(defn key-range
  "Returns a Thrift KeyRange instance for a range of keys"
  [& {:keys [start-token start-key end-token end-key count-key]}]
  (let [kr (KeyRange.)]
    (when start-token (.setStart_token kr ^String start-token))
    (when end-token (.setEnd_token kr ^String end-token))
    (when start-key (.setStart_key kr ^ByteBuffer (codecs/clojure->byte-buffer start-key)))
    (when end-key (.setEnd_key kr ^ByteBuffer (codecs/clojure->byte-buffer end-key)))
    (when count-key (.setCount kr (int count-key)))
    kr))

(defmacro doto-mutation
  [& body]
  `(doto (Mutation.)
     (.setColumn_or_supercolumn
      (doto (ColumnOrSuperColumn.)
        ~@body))))

(defn column-mutation
  ""
  [& c]
  (doto-mutation
   (.setColumn (apply column c))))

(defn counter-column-mutation
  ""
  [n value]
  (doto-mutation
   (.setCounter_column (counter-column n value))))

(defn super-column-mutation
  ""
  [& sc]
  (doto-mutation
   (.setSuper_column (apply super-column sc))))

(defn super-counter-column-mutation
  ""
  [& sc]
  (doto-mutation
   (.setCounter_super_column ^CounterSuperColumn (apply counter-super-column sc))))

(defn ^Deletion deletion
  ""
  ^SlicePredicate
  ([slice-pred ^ByteBuffer sc]
     (doto ^Deletion (deletion  slice-pred)
       (.setSuper_column sc)))
  ([slice-pred]
     (doto (Deletion.)
       (.setTimestamp (utils/ts))
       (.setPredicate slice-pred))))

(defn delete-mutation
  ""
  [& args] ;; expects a pred and opt sc
  (doto (Mutation.)
    (.setDeletion (apply deletion args))))


;; Secondary indexes

(def index-operators
  {:eq? IndexOperator/EQ
   :lt? IndexOperator/LT
   :gt? IndexOperator/GT
   :lte? IndexOperator/LTE
   :gte? IndexOperator/GTE})

(defn index-expressions
  [expressions]
  (map (fn [[op k v]]
         (IndexExpression. (codecs/clojure->byte-buffer k)
                           (index-operators op)
                           (codecs/clojure->byte-buffer v)))
       expressions))

(defn index-clause
  "Defines one or more IndexExpressions for get_indexed_slices. An
  IndexExpression containing an EQ IndexOperator must be present"
  [expressions & {:keys [start-key count]
      :or {count 100}}]
  (IndexClause. (index-expressions expressions)
                (codecs/clojure->byte-buffer start-key)
                (int count)))

;; API

(defn login
  ""
  [^Cassandra$AsyncClient client ^AuthenticationRequest auth-req]
  (wrap-result-channel (.login client auth-req)))

(defn set-keyspace
  ""
  [^Cassandra$AsyncClient client ^String ks]
  (wrap-result-channel (.set_keyspace client ks)))

(defn get-column
  ""
  [^Cassandra$AsyncClient client cf row-key c
   & {:keys [consistency]}]
  (wrap-result-channel
   (.get client
         ^ByteBuffer (codecs/clojure->byte-buffer row-key)
         (column-path cf c)
         (consistency-level consistency))))

(defn get-super-column
  ""
  [^Cassandra$AsyncClient client cf row-key c sc
   & {:keys [consistency]}]
  (wrap-result-channel
   (.get client
         ^ByteBuffer (codecs/clojure->byte-buffer row-key)
         (column-path cf sc c)
         (consistency-level consistency))))

(defn get-slice
  ""
  [^Cassandra$AsyncClient client cf row-key slice-predicate
   & {:keys [consistency]}]
  (wrap-result-channel
   (.get_slice client
               (codecs/clojure->byte-buffer row-key)
               (column-parent cf)
               slice-predicate
               (consistency-level consistency))))

(defn mget-slice
  ""
  [^Cassandra$AsyncClient client cf row-keys slice-predicate
   & {:keys [consistency]}]
  (wrap-result-channel
   (.multiget_slice client
                    (map codecs/clojure->byte-buffer row-keys)
                    (column-parent cf)
                    slice-predicate
                    (consistency-level consistency))))

(defn get-super-slice
  ""
  [^Cassandra$AsyncClient client cf row-key sc slice-predicate
   & {:keys [consistency]}]
  (wrap-result-channel
   (.get_slice client
               (codecs/clojure->byte-buffer row-key)
               (column-parent cf sc)
               slice-predicate
               (consistency-level consistency))))

(defn mget-super-slice
  ""
  [^Cassandra$AsyncClient client cf row-keys sc slice-predicate
   & {:keys [consistency]}]
  (wrap-result-channel
   (.multiget_slice client
                    (map codecs/clojure->byte-buffer row-keys)
                    (column-parent cf sc)
                    slice-predicate
                    (consistency-level consistency))))

(defn get-count
  ""
  [^Cassandra$AsyncClient client cf row-key slice-predicate
   & {:keys [consistency]}]
  (wrap-result-channel
   (.get_count client
               (codecs/clojure->byte-buffer row-key)
               (column-parent cf)
               slice-predicate
               (consistency-level consistency))))

(defn mget-count
  ""
  [^Cassandra$AsyncClient client cf row-keys slice-predicate
   & {:keys [consistency]}]
  (wrap-result-channel
   (.multiget_count client
                    (map codecs/clojure->byte-buffer row-keys)
                    (column-parent cf)
                    slice-predicate
                    (consistency-level consistency))))

(defn get-super-count
  ""
  [^Cassandra$AsyncClient client cf row-key sc slice-predicate
   & {:keys [consistency]}]
  (wrap-result-channel
   (.get_count client
               (codecs/clojure->byte-buffer row-key)
               (column-parent cf sc)
               slice-predicate
               (consistency-level consistency))))

(defn mget-super-count
  ""
  [^Cassandra$AsyncClient client cf row-keys sc slice-predicate
   & {:keys [consistency]}]
  (wrap-result-channel
   (.multiget_count client
                    (map codecs/clojure->byte-buffer row-keys)
                    (column-parent cf sc)
                    slice-predicate
                    (consistency-level consistency))))

(defn insert-column
  ""
  [^Cassandra$AsyncClient client cf row-key name value
   & {:keys [consistency counter ttl timestamp]}]
  (wrap-result-channel
   (.insert client
            (codecs/clojure->byte-buffer row-key)
            (column-parent cf)
            (if counter
              (counter-column name value)
              (column name value :ttl ttl :timestamp timestamp))
            (consistency-level consistency))))

(defn insert-super-column
  ""
  [^Cassandra$AsyncClient client cf row-key sc cols
   & {:keys [consistency]}]
  (wrap-result-channel
   (.insert client
            (codecs/clojure->byte-buffer row-key)
            (column-parent cf sc)
            (super-column sc cols)
            (consistency-level consistency))))

(defn increment
  ""
  [^Cassandra$AsyncClient client cf row-key column-name value
   & {:keys [consistency]}]
  (wrap-result-channel
   (.add client
         (codecs/clojure->byte-buffer row-key)
         (column-parent cf)
         (counter-column column-name value)
         (consistency-level consistency))))

(defn increment-super
  ""
  [^Cassandra$AsyncClient client cf row-key sc column-name value
   & {:keys [consistency]}]
  (wrap-result-channel
   (.add client
         (codecs/clojure->byte-buffer row-key)
         (column-parent cf sc)
         (counter-column column-name value)
         (consistency-level consistency))))

(defn remove-column
  ""
  [^Cassandra$AsyncClient client cf row-key
   & {:keys [timestamp consistency]
      :or {timestamp (utils/ts)}}]
  (wrap-result-channel
   (.remove client
            (codecs/clojure->byte-buffer row-key)
            (column-path cf)
            timestamp
            (consistency-level consistency))))

(defn remove-counter
  ""
  [^Cassandra$AsyncClient client cf row-key c
   & {:keys [consistency]}]
  (wrap-result-channel
   (.remove_counter client
                    (codecs/clojure->byte-buffer row-key)
                    (column-path cf c)
                    (consistency-level consistency))))

(defn remove-super-column
  ""
  [^Cassandra$AsyncClient client cf row-key sc c
   & {:keys [consistency]}]
  (wrap-result-channel
   (.remove_counter client
                    (codecs/clojure->byte-buffer row-key)
                    (column-path cf sc c)
                    (consistency-level consistency))))

(defn remove-super-counter
  ""
  [^Cassandra$AsyncClient client cf row-key sc c
   & {:keys [consistency]}]
  (wrap-result-channel
   (.remove_counter client
                    (codecs/clojure->byte-buffer row-key)
                    (column-path cf sc c)
                    (consistency-level consistency))))

(defn batch-mutate
  ""
  [^Cassandra$AsyncClient client mutations
   & {:keys [consistency]}]
  (wrap-result-channel
   (.batch_mutate client
                  (reduce-kv (fn [m k v]
                               (assoc m (codecs/clojure->byte-buffer k) v))
                          {}
                          mutations)
                  (consistency-level consistency))))

(defn get-range-slice
  ""
  [^Cassandra$AsyncClient client column-parent-args slice-predicate key-range-args
   & {:keys [consistency]}]
  (wrap-result-channel
   (.get_range_slices client
                      (if (sequential? column-parent-args)
                        (apply column-parent column-parent-args)
                        (column-parent column-parent-args))
                      slice-predicate
                      (apply key-range key-range-args)
                      (consistency-level consistency))))

(defn get-indexed-slice
  ""
  [^Cassandra$AsyncClient client column-parent-args index-clause-args slice-predicate
   & {:keys [consistency]}]
  (wrap-result-channel
   (.get_indexed_slices client
                        (if (sequential? column-parent-args)
                          (apply column-parent column-parent-args)
                          (column-parent column-parent-args))
                        (index-clause index-clause-args)
                        slice-predicate
                        (consistency-level consistency))))

(defn truncate
  ""
  [^Cassandra$AsyncClient client cf]
  (wrap-result-channel (.truncate client cf)))

(defn describe-cluster-name
  ""
  [^Cassandra$AsyncClient client]
  (wrap-result-channel (.describe_cluster_name client)))

(defn describe-keyspace
  ""
  [^Cassandra$AsyncClient client ks]
  (wrap-result-channel (.describe_keyspace client ks)))

(defn describe-keyspaces
  ""
  [^Cassandra$AsyncClient client]
  (wrap-result-channel (.describe_keyspaces client)))

(defn describe-partitioner
  ""
  [^Cassandra$AsyncClient client]
  (wrap-result-channel (.describe_partitioner client)))

(defn describe-ring
  ""
  [^Cassandra$AsyncClient client ks]
  (wrap-result-channel (.describe_ring client ks)))

(defn describe-schema-versions
  ""
  [^Cassandra$AsyncClient client]
  (wrap-result-channel (.describe_schema_versions client)))

(defn describe-snitch
  ""
  [^Cassandra$AsyncClient client]
  (wrap-result-channel (.describe_snitch client)))

(defn describe-splits
  ""
  [^Cassandra$AsyncClient client cf start-token end-token keys-per-split]
  (wrap-result-channel (.describe_splits client
                                         cf
                                         start-token end-token
                                         keys-per-split)))

(defn describe-version
  ""
  [^Cassandra$AsyncClient client]
  (wrap-result-channel (.describe_version client)))

(defn execute-cql
  ""
  [^Cassandra$AsyncClient client query]
  (wrap-result-channel
   (.execute_cql_query client
                       (codecs/clojure->byte-buffer query)
                       Compression/GZIP)))


;; Sugar

(defn get-rows
  ""
  [^Cassandra$AsyncClient client cf row-keys
   & {:keys [consistency start finish reversed count]}]
  (mget-slice client
              cf
              row-keys
              (columns-by-range :start start
                                :finish finish
                                :reversed reversed
                                :count count)
              :consistency consistency))

(defn get-row
  ""
  [^Cassandra$AsyncClient client cf row-key
   & {:keys [consistency start finish reversed count]}]
  (get-slice client
             cf
             row-key
             (columns-by-range :start start
                               :finish finish
                               :reversed reversed
                               :count count)
             :consistency consistency))

(defn put
  "Accepts cols as vectors or maps to be applied to cols
  constructors (use maps for simple key vals, use vectors if you need
  to set ttl or timestamp"
  [^Cassandra$AsyncClient client cf row-key columns
   & {:keys [consistency counters super-columns super-counters]}]
  (batch-mutate
   client
   {row-key
    {cf (map #(apply
               (cond
                 super-columns super-column-mutation
                 super-counters super-counter-column-mutation
                 counters counter-column-mutation
                 :else column-mutation)
               %)
             columns)}}
   :consistency consistency))

(defn get-super-rows
  ""
  [^Cassandra$AsyncClient client cf row-keys sc
   & {:keys [consistency start finish reversed count]}]
  (mget-super-slice client
                    cf
                    row-keys
                    sc
                    (columns-by-range :start start
                                      :finish finish
                                      :reversed reversed
                                      :count count)
                    :consistency consistency))

(defn get-super-row
  ""
  [^Cassandra$AsyncClient client cf row-key sc
   & {:keys [consistency start finish reversed count]}]
  (get-super-slice client
                   cf
                   row-key
                   sc
                   (columns-by-range :start start
                                     :finish finish
                                     :reversed reversed
                                     :count count)
                   :consistency consistency))
