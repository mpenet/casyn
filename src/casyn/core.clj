(ns casyn.core
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
    IndexClause ConsistencyLevel Compression]
   [org.apache.thrift.async AsyncMethodCallback TAsyncClient]
   [java.nio ByteBuffer]))

(def ^:dynamic consistency-default :all)

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
        result-hint (->> form first str rest (apply str)
                         (format "org.apache.cassandra.thrift.Cassandra$AsyncClient$%s_call"))]
    `(let [result-ch# (lc/result-channel)]
       (~@form ^"org.apache.thrift.async.AsyncMethodCallback"
               (reify AsyncMethodCallback
                 (onComplete [_ ~thrift-cmd-call]
                   (lc/success result-ch#
                                (.getResult ~(with-meta thrift-cmd-call {:tag result-hint}))))
                 (onError [_ error#]
                   (lc/error result-ch# error#))))
       (lc/run-pipeline result-ch# codecs/thrift->clojure))))

(defmacro defcommand [name args form]
  (let [client (gensym)]
    `(defn ~name [~(with-meta client {:tag "Cassandra$AsyncClient"})
                  ~@args]
       (wrap-result-channel (~(first form) ~client ~@(rest form))))))

;; Objects

(defn column
  "Returns a Thrift Column instance"
  [name value & {:keys [ttl timestamp]
                 :or {timestamp (utils/ts)}}]
  (let [col (Column. ^ByteBuffer (codecs/clojure->byte-buffer name))]
    (.setValue col ^ByteBuffer (codecs/clojure->byte-buffer value))
    (.setTimestamp col timestamp)
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
                (map (partial apply column) columns)))

(defn counter-super-column
  ""
  [name columns]
  (CounterSuperColumn. (codecs/clojure->byte-buffer name)
                       (map (partial apply counter-column) columns)))

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

(defn apply-fn-args [f]
  (fn [args]
    (if (sequential? args)
      (apply f args)
      (f args))))

(def column-path-from-args (apply-fn-args column-path))
(def column-parent-from-args (apply-fn-args column-parent))

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

  ;; TODO
(defn index-clause
  ""
  []
  )

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
  [^Cassandra$AsyncClient client row-key column-path-args
   & {:keys [consistency]}]
  (wrap-result-channel
   (.get client
         ^ByteBuffer (codecs/clojure->byte-buffer row-key)
         (column-path-from-args column-path-args)
         (consistency-level consistency))))

(defn get-slice
  ""
  [^Cassandra$AsyncClient client row-key column-parent-args slice-predicate
   & {:keys [consistency]}]
  (wrap-result-channel
   (.get_slice client
               (codecs/clojure->byte-buffer row-key)
               (column-parent-from-args column-parent-args)
               slice-predicate
               (consistency-level consistency))))

(defn mget-slice
  ""
  [^Cassandra$AsyncClient client row-keys column-parent-args slice-predicate
   & {:keys [consistency]}]
  (wrap-result-channel
   (.multiget_slice client
                    (codecs/clojure->byte-buffer row-keys)
                    (column-parent-from-args column-parent-args)
                    slice-predicate
                    (consistency-level consistency))))

(defn get-count
  ""
  [^Cassandra$AsyncClient client row-key column-parent-args slice-predicate
   & {:keys [consistency]}]
  (wrap-result-channel
   (.get_count client
               (codecs/clojure->byte-buffer row-key)
               (column-parent-from-args column-parent-args)
               slice-predicate
               (consistency-level consistency))))

(defn mget-count
  ""
  [^Cassandra$AsyncClient client row-keys column-parent-args slice-predicate
   & {:keys [consistency]}]
  (wrap-result-channel
   (.multiget_count client
                    (codecs/clojure->byte-buffer row-keys)
                    (column-parent-from-args column-parent-args)
                    slice-predicate
                    (consistency-level consistency))))

(defn insert-column
  ""
  [^Cassandra$AsyncClient client row-key column-parent-args column
   & {:keys [consistency]}]
  (wrap-result-channel
   (.insert client
            (codecs/clojure->byte-buffer row-key)
            (column-parent-from-args column-parent-args)
            column
            (consistency-level consistency))))

(defn add
  ""
  [^Cassandra$AsyncClient client row-key column-parent-args column-name value
   & {:keys [consistency]}]
  (wrap-result-channel
   (.add client
         (codecs/clojure->byte-buffer row-key)
         (column-parent-from-args column-parent-args)
         (counter-column column-name value)
         (consistency-level consistency))))

(defn remove-column
  ""
  [^Cassandra$AsyncClient client row-key column-path-args
   & {:keys [timestamp consistency]
      :or {timestamp (utils/ts)}}]
  (wrap-result-channel
   (.remove client
            (codecs/clojure->byte-buffer row-key)
            (column-path-from-args column-path-args)
            timestamp
            (consistency-level consistency))))

(defn remove-counter
  ""
  [^Cassandra$AsyncClient client row-key column-path-args
   & {:keys [consistency]}]
  (wrap-result-channel
   (.remove_counter client
                    (codecs/clojure->byte-buffer row-key)
                    (column-path-from-args column-path-args)
                    (consistency-level consistency))))

(defn batch-mutate
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
                      (column-parent-from-args column-parent-args)
                      slice-predicate
                      (apply key-range key-range-args)
                      (consistency-level consistency))))

(defn get-indexed-slice
  ""
  [^Cassandra$AsyncClient client column-parent-args slice-predicate index-clause
   & {:keys [consistency]}]
  (wrap-result-channel
   (.get_indexed_slices client
                        (column-parent-from-args column-parent-args)
                        slice-predicate
                        index-clause
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
  [^Cassandra$AsyncClient client row-keys column-parent-args
   & {:keys [consistency start finish reversed count]}]
  (mget-slice client
              row-keys
              column-parent-args
              (columns-by-range :start start
                                :finish finish
                                :reversed reversed
                                :count count)
              :consistency consistency))

(defn get-row
  ""
  [^Cassandra$AsyncClient client row-key column-parent-args
   & {:keys [consistency start finish reversed count]}]
  (get-slice client
             row-key
             column-parent-args
             (columns-by-range :start start
                               :finish finish
                               :reversed reversed
                               :count count)
             :consistency consistency))

(defn put
  "Accepts cols as vectors or maps to be applied to cols
  constructors (use maps for simple key vals, use vectors if you need
  to set ttl or timestamp"
  [^Cassandra$AsyncClient client row-key cf columns
   & {:keys [consistency counters super-columns super-counters]}]
  (batch-mutate
   client
   {row-key
    {cf (map (partial apply
                      (cond
                        super-columns super-column-mutation
                        super-counters super-counter-column-mutation
                        counters counter-column-mutation
                        :else column-mutation))
             columns)}}
   :consistency consistency))

(def delete-row remove-column)