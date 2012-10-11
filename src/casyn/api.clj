(ns casyn.api
  "Thrift API and related utils. Most of the fns here translate almost directly
to their thrift counterpart and most of the time return Thrift instances.
See: http://wiki.apache.org/cassandra/API \nand
http://javasourcecode.org/html/open-source/cassandra/cassandra-0.8.1/org/apache/cassandra/thrift/Cassandra.AsyncClient.html"
  (:require
   [lamina.core :as lc]
   [casyn.utils :as utils]
   [casyn.executor :as x]
   [casyn.codecs :as codecs]
   [casyn.types :as t]
   [casyn.schema :as schema]
   [clojure.walk :as w])

  (:import
   [org.apache.cassandra.thrift
    Column SuperColumn CounterSuperColumn CounterColumn ColumnPath
    ColumnOrSuperColumn ColumnParent Mutation Deletion SlicePredicate
    SliceRange KeyRange AuthenticationRequest Cassandra$AsyncClient
    IndexClause IndexExpression IndexOperator ConsistencyLevel Compression]
   [org.apache.thrift.async AsyncMethodCallback]
   [java.nio ByteBuffer]
   [casyn.client Client]))

(def ^:dynamic *consistency-default* :one)

(defn consistency-level [c]
  ((or c *consistency-default*)
   {:all          ConsistencyLevel/ALL
    :any          ConsistencyLevel/ANY
    :each-quorum  ConsistencyLevel/EACH_QUORUM
    :local-quorum ConsistencyLevel/LOCAL_QUORUM
    :one          ConsistencyLevel/ONE
    :quorum       ConsistencyLevel/QUORUM
    :three        ConsistencyLevel/THREE
    :two          ConsistencyLevel/TWO}))

(defmacro with-consistency
  "Binds consistency level for the enclosed body"
  [consistency & body]
  `(binding [casyn.api/*consistency-default* ~consistency]
     ~@body))

(defn ^:private api-fn?
  [v]
  (and (symbol? v)
       (= 'Cassandra$AsyncClient
          (-> (ns-resolve 'casyn.api v)
              meta :arglists
              ffirst meta :tag))))

;; Async helper
(defmacro wrap-result-channel
  "Wraps a form in a Lamina result-channel, and make the last arg of the form an
   AsyncMethodCallback with error/complete callback bound to a result-channel"
  [form & post-realize-fns]
  (let [thrift-cmd-call (gensym)
        [method client & args] form
        result-hint (format "org.apache.cassandra.thrift.Cassandra$AsyncClient$%s_call"
                            (-> form first str (subs 1)))]
    `(let [result-ch# (lc/result-channel)]
       (~method ^"org.apache.cassandra.thrift.Cassandra$AsyncClient" (.thrift-client ~client)
                ~@args ^"org.apache.thrift.async.AsyncMethodCallback"
                (reify AsyncMethodCallback
                  (onComplete [_ ~thrift-cmd-call]
                    (let [result# (.getResult ~(with-meta thrift-cmd-call
                                                           {:tag result-hint}))]
                      (x/execute (.executor ~client)
                                 #(lc/success result-ch# result#))))
                  (onError [_ error#]
                    (x/execute (.executor ~client)
                               #(lc/error result-ch# error#)))))
       (lc/run-pipeline
        result-ch#
        {:error-handler (fn [_#])}
        t/thrift->casyn
        ~@(filter identity post-realize-fns)))))





(defmacro wrap-result-channel+schema [form schema output]
  `(wrap-result-channel
    ~form
    #(if ~schema
       (casyn.schema/decode-result % ~schema ~output)
       %)))

;; Objects

(defn column
  "Returns a Thrift Column instance.
Optional kw args:
  - :type (keyword): Represents the column type, defaults :column can also be :counter
  - :ttl (integer): Allows to specify the Time to live value for the column
  - :timestamp (long): Allows to specify the Timestamp for the column
                       (in nanosecs), defaults to the value for the current time"
  [name value & {:keys [type ttl timestamp]
                 :or {type :column}}]
  (case type
    :column (let [col (Column. ^ByteBuffer (codecs/clojure->byte-buffer name))]
      (.setValue col ^ByteBuffer (codecs/clojure->byte-buffer value))
      (.setTimestamp col (or timestamp (utils/ts)))
      (when ttl (.setTtl col (int ttl)))
      col)
    :counter (CounterColumn. (codecs/clojure->byte-buffer name)
                             (long value))
    :super (SuperColumn. (codecs/clojure->byte-buffer name)
                         (map #(apply column %) value))
    :counter-super-column (CounterSuperColumn. (codecs/clojure->byte-buffer name)
                                               (map #(apply column :counter %) value))))

(defn column-parent
  "Returns a Thrift ColumnParent instance, works for common columns or
  super columns depending on arity used. The \"super\" argument will be
  used as super column name (can be of any supported type)"
  ^ColumnParent [^String cf & [super]]
  (let [cp (ColumnParent. cf)]
    (when super
      (.setSuper_column cp ^ByteBuffer (codecs/clojure->byte-buffer super)))
    cp))

(defn column-path
  "Returns a Thrift ColumnPath instance, works for common columns or
  super columns depending on arity used.
Optional kw args:
  - :type (keyword): Represents the column type, defaults :column can also be :counter
  - :super : super column name (can be of any supported type), must be present
             if type is :super"
  ([^String cf & {:keys [super column]}]
     (let [cp ^ColumnPath (column-path cf)]
       (when super
         (.setSuper_column cp ^ByteBuffer (codecs/clojure->byte-buffer super)))
       (when column
         (.setColumn cp ^ByteBuffer (codecs/clojure->byte-buffer column)))
       cp))
  ([^String cf]
     (ColumnPath. cf)))

(def index-operators
  {:eq?  IndexOperator/EQ
   :lt?  IndexOperator/LT
   :gt?  IndexOperator/GT
   :lte? IndexOperator/LTE
   :gte? IndexOperator/GTE})

(defn index-expressions
  "Returns and IndexExpression instance for a sequence of clauses.
The first value in the expression vectors must be a valid index-operator: :eq?, :lt?, :lte?:, :gt?, :gte?

Example: [[:eq? :foo \"bar\"]
          [:gt? \"baz\" 1]]"
  [expressions]
  (map (fn [[op k v]]
         (IndexExpression. (codecs/clojure->byte-buffer k)
                           (index-operators op)
                           (codecs/clojure->byte-buffer v)))
       expressions))

(defn index-clause
  "Defines one or more IndexExpressions for get_indexed_slices. An
IndexExpression containing an EQ IndexOperator must be present.
Optional kw args:
  - :start-key : The first key in the inclusive KeyRange
  - :count (long): The total number of keys to permit in the KeyRange. defaults to 100"
  [expressions & {:keys [start-key count]
                  :or {count 100}}]
  (IndexClause. (index-expressions expressions)
                (codecs/clojure->byte-buffer start-key)
                (int count)))

(defn slice-predicate
  "Returns a SlicePredicate instance, takes a map, it can be either for named keys
using the :columns key, or a range defined from :start :finish :reversed :count
Ex: (slice-predicate {:columns [\"foo\" \"bar\"]})
    (slice-predicate :start 100 :finish 200 :reversed true :count 10)

Optional kw args:
  - :start : The column name to start the slice with.
  - :finish : The column name to stop the slice at
  - :reversed (bool): Whether the results should be ordered in reversed order.
  - :count : How many columns to return, defaults to 100
  - :columns: A list of column names to retrieve"
  [{:keys [columns start finish reversed count]}]
  (let [sp (SlicePredicate.)]
    (if columns
      (.setColumn_names sp (map codecs/clojure->byte-buffer columns))
      (.setSlice_range sp (SliceRange. (codecs/clojure->byte-buffer start)
                                       (codecs/clojure->byte-buffer finish)
                                       (boolean reversed)
                                       (int (or count 100)))))))

(defn key-range
  "Returns a Thrift KeyRange instance for a range of keys, row-filter
  accepts a sequence of index expressions, see index-expression

Optional kw args:
  - :start-token : The first token in the exclusive KeyRange.
  - :end-token : The last token in the exclusive KeyRange.
  - :start-key: The first key in the inclusive KeyRange.
  - :end-key : The last key in the inclusive KeyRange.
  - :count : The total number of keys to permit in the KeyRange.
  - :row-filter: The list of index expressions vectors"
  [{:keys [start-token start-key end-token end-key count row-filter]}]
  (let [kr (KeyRange.)]
    (when start-token (.setStart_token kr ^String start-token))
    (when end-token (.setEnd_token kr ^String end-token))
    (when start-key (.setStart_key kr ^ByteBuffer (codecs/clojure->byte-buffer start-key)))
    (when end-key (.setEnd_key kr ^ByteBuffer (codecs/clojure->byte-buffer end-key)))
    (when count (.setCount kr (int count)))
    (when row-filter (.setRow_filter kr (index-expressions row-filter)))
    kr))

(defn mutation
  "Takes column name, and value + optional :type that can have the
  following  values :column (default) :super :counter :counter-super. :ttl
  and :timestamp options are also available when dealing with super or
  regular columns, otherwise ignored.

Optional kw args:
  - :type (keyword): Represents the column type
                     defaults :column can also be :counter :super :counter-super
  - :ttl (integer): Allows to specify the Time to live value for the column
  - :timestamp (long): Allows to specify the Timestamp for the column
                       (in nanosecs), defaults to the value for the current time"
  [name value & {:keys [type ttl timestamp]}]
  (doto (Mutation.)
    (.setColumn_or_supercolumn
     (let [c (ColumnOrSuperColumn.)]
       (case type
         :super
         (.setSuper_column c (column name value
                                     :ttl ttl
                                     :timestamp timestamp
                                     :type :super))

         :counter
         (.setCounter_column c ^CounterColumn (column name value :type :counter))

         :counter-super
         (.setCounter_super_column c ^CounterSuperColumn (column name value :type :counter-super))

         ;; else
         (.setColumn c ^Column (column name value
                               :ttl ttl
                               :timestamp timestamp)))
       c))))

(defn delete-mutation
  "Accepts optional slice-predicate arguments :columns, :start, :finish, :count,
:reversed, if you specify :columns the other slice args will be ignored (as
defined by thrift)

Optional kw args:
  - :super : optional super column name
  - :start : The column name to start the slice with.
  - :finish : The column name to stop the slice at
  - :reversed (bool): Whether the results should be ordered in reversed order.
  - :count : How many columns to return, defaults to 100
  - :columns: A list of column names to retrieve"
  [& {:keys [super]
      :as opts}] ;; expects a pred and opt sc
  (doto (Mutation.)
    (.setDeletion
     (let [d (Deletion.)]
       (.setTimestamp d (utils/ts))
       (.setPredicate d (slice-predicate opts))
       (when super
         (.setSuper_column d ^ByteBuffer (codecs/clojure->byte-buffer super)))
       d))))

;; API

(defn login
  "Expect an AuthenticationRequest instance as argument"
  [^Client client ^AuthenticationRequest auth-req]
  (wrap-result-channel (.login client auth-req) ))

(defn set-keyspace
  ""
  [^Client client  ^String ks]
  (wrap-result-channel (.set_keyspace client ks)))

(defn get-column
  "Returns a single column.
Optional kw args:
  - :consistency : optional consistency-level, defaults to :one
  - :super : optional super column name
  - :schema : schema used for result decoding
  - :output : output format (if nil it will return casyn types,
              if :as-map it will try to turn collections to maps"
  [^Client client cf row-key col
   & {:keys [super consistency schema output]}]
  (wrap-result-channel+schema
   (.get client
         ^ByteBuffer (codecs/clojure->byte-buffer row-key)
         (column-path cf :super super :column col)
         (consistency-level consistency))
   schema output))

(defn get-slice
  "Returns a slice of columns. Accepts optional slice-predicate arguments :columns, :start, :finish, :count,
:reversed, if you specify :columns the other slice args will be ignored (as defined by the cassandra api).

Optional kw args:
  - :super : optional super column name
  - :start : The column name to start the slice with.
  - :finish : The column name to stop the slice at
  - :reversed (bool): Whether the results should be ordered in reversed order.
  - :count : How many columns to return, defaults to 100
  - :columns: A list of column names to retrieve
  - :consistency : optional consistency-level, defaults to :one
  - :schema : schema used for result decoding
  - :output : output format (if nil it will return casyn types,
              if :as-map it will try to turn collections to maps"
  [^Client client cf row-key
   & {:keys [super consistency schema output]
      :as opts}]
  (wrap-result-channel+schema
   (.get_slice client
               (codecs/clojure->byte-buffer row-key)
               (column-parent cf super)
               (slice-predicate opts)
               (consistency-level consistency))
    schema output))

(defn mget-slice
  "Returns a collection of slices of columns.
   Accepts optional slice-predicate
   arguments :columns, :start, :finish, :count, :reversed, if you
   specify :columns the other slice args will be ignored (as defined by the cassandra api)

Optional kw args:
  - :super : optional super column name
  - :start : The column name to start the slice with.
  - :finish : The column name to stop the slice at
  - :reversed (bool): Whether the results should be ordered in reversed order.
  - :count : How many columns to return, defaults to 100
  - :columns: A list of column names to retrieve
  - :consistency : optional consistency-level, defaults to :one
  - :schema : schema used for result decoding
  - :output : output format (if nil it will return casyn types,
              if :as-map it will try to turn collections to maps"
  [^Client client cf row-keys
   & {:keys [super consistency schema output]
      :as opts}]
  (wrap-result-channel+schema
   (.multiget_slice client
                    (map codecs/clojure->byte-buffer row-keys)
                    (column-parent cf super)
                    (slice-predicate opts)
                    (consistency-level consistency))
    schema output))

(defn get-count
  "Accepts optional slice-predicate arguments :columns, :start, :finish, :count,
:reversed, if you specify :columns the other slice args will be ignored (as
defined by the cassandra api).

Optional kw args:
  - :super : optional super column name
  - :start : The column name to start the slice with.
  - :finish : The column name to stop the slice at
  - :reversed (bool): Whether the results should be ordered in reversed order.
  - :count : How many columns to return, defaults to 100
  - :columns: A list of column names to retrieve
  - :consistency : optional consistency-level, defaults to :one
  - :schema : schema used for result decoding
  - :output : output format (if nil it will return casyn types,
              if :as-map it will try to turn collections to maps"
  [^Client client cf row-key
   & {:keys [super consistency schema output]
      :as opts}]
  (wrap-result-channel+schema
   (.get_count client
               (codecs/clojure->byte-buffer row-key)
               (column-parent cf super)
               (slice-predicate opts)
               (consistency-level consistency))
    schema output))

(defn mget-count
  "Accepts optional slice-predicate arguments :columns, :start, :finish, :count,
:reversed, if you specify :columns the other slice args will be ignored (as
defined by the cassandra api).

Optional kw args:
  - :super : optional super column name
  - :start : The column name to start the slice with.
  - :finish : The column name to stop the slice at
  - :reversed (bool): Whether the results should be ordered in reversed order.
  - :count : How many columns to return, defaults to 100
  - :columns: A list of column names to retrieve
  - :consistency : optional consistency-level, defaults to :one
  - :schema : schema used for result decoding
  - :output : output format (if nil it will return casyn types,
              if :as-map it will try to turn collections to maps"
  [^Client client cf row-keys
   & {:keys [super consistency schema output]
      :as opts}]
  (wrap-result-channel+schema
   (.multiget_count client
                    (map codecs/clojure->byte-buffer row-keys)
                    (column-parent cf super)
                    (slice-predicate opts)
                    (consistency-level consistency))
    schema output))

(defn insert-column
  "Inserts a single column.

Optional kw args:
  - :type (keyword): Represents the column type, defaults :column can also be :counter, :super
  - :super : if type if :super this argument will be used as the super column name
  - :ttl (integer): Allows to specify the Time to live value for the column
  - :timestamp (long): Allows to specify the Timestamp for the column
                       (in nanosecs), defaults to the value for the current time
  - :consistency : optional consistency-level, defaults to :one"
  [^Client client cf row-key name value
   & {:keys [super type consistency ttl timestamp]
      :or {type :column}}]
  (wrap-result-channel
   (.insert client
            (codecs/clojure->byte-buffer row-key)
            (column-parent cf super)
            (case type
              :column (column name value :ttl ttl :timestamp timestamp)
              :counter (column name value :type :counter)
               ;; values is a collection of columns for super-cols
              :super (column super value :type :super))
            (consistency-level consistency))))

(defn increment
  "Increment the specified counter column value.

Optional kw args:
  - :super : this argument will be used as the super column name if specified
  - :consistency : optional consistency-level, defaults to :one"
  [^Client client cf row-key column-name value
   & {:keys [super consistency]}]
  (wrap-result-channel
   (.add client
         (codecs/clojure->byte-buffer row-key)
         (column-parent cf super)
         (column column-name value :type :counter)
         (consistency-level consistency))))

(defn delete
  "Delete column(s), works on regular columns or counters.

Optional kw args:
  - :type (keyword): Represents the column type, defaults :column can also be :counter, :super
  - :super :  used as the super column name if specified
  - :ttl (integer): Allows to specify the Time to live value for the column
  - :timestamp (long): Allows to specify the Timestamp for the column
                       (in nanosecs), defaults to the value for the current time
  - :consistency : optional consistency-level, defaults to :one"
  [^Client client cf row-key
   & {:keys [column super timestamp consistency type]
      :or {timestamp (utils/ts)}}]
  (if (= :counter type)
    (wrap-result-channel
     (.remove_counter client
                      (codecs/clojure->byte-buffer row-key)
                      (column-path cf :super super :column column)
                      (consistency-level consistency)))
    (wrap-result-channel
     (.remove client
              (codecs/clojure->byte-buffer row-key)
              (column-path cf :super super :column column)
              timestamp
              (consistency-level consistency)))))

(defn batch-mutate
  "Executes the specified mutations on the keyspace.
Expects a map of maps
The outer map key is a row key, the inner map key is the column family name, its values are the mutations instances (see mutation & delete-mutation fns):

Example:
 {\"row-0\" {\"cf\" [(mutation \"n0\" \"un0\")
                     (mutation \"n00\" \"un00\")]}
  \"row-1\" {cf [(mutation \"n1\" \"n10\")]}}

Optional kw args:
  - :consistency : optional consistency-level, defaults to :one"
  [^Client client mutations
   & {:keys [consistency]}]
  (wrap-result-channel
   (.batch_mutate client
                  (reduce-kv (fn [m k v]
                               (assoc m (codecs/clojure->byte-buffer k) v))
                             {}
                             mutations)
                  (consistency-level consistency))))

(defn get-range-slice
  "Accepts optional slice-predicate arguments :columns, :start, :finish, :count,
:reversed, if you specify :columns the other slice args will be ignored (as
defined by the cassandra api). Accepts optional key-range arguments :start-token
:start-key :end-token :end-key :count-key :row-filter (vector of index-expressions).

Optional kw args:
  - :super :  used as the super column name if specified
  - :start : The column name to start the slice with.
  - :finish : The column name to stop the slice at
  - :reversed (bool): Whether the results should be ordered in reversed order.
  - :count : How many columns to return, defaults to 100
  - :columns: A list of column names to retrieve

  - :start-token : The first token in the exclusive KeyRange.
  - :end-token : The last token in the exclusive KeyRange.
  - :start-key: The first key in the inclusive KeyRange.
  - :end-key : The last key in the inclusive KeyRange.
  - :count : The total number of keys to permit in the KeyRange.
  - :row-filter: The list of index expressions vectors

  - :consistency : optional consistency-level, defaults to :one
  - :schema : schema used for result decoding
  - :output : output format (if nil it will return casyn types,
              if :as-map it will try to turn collections to maps"
  [^Client client cf
   & {:keys [super consistency schema output]
      :as opts}]
  (wrap-result-channel+schema
   (.get_range_slices client
                      (column-parent cf super)
                      (slice-predicate opts)
                      (key-range opts)
                      (consistency-level consistency))
    schema output))

(defn get-indexed-slice
  "Accepts optional slice-predicate arguments :columns, :start, :finish, :count,
:reversed, if you specify :columns the other slice args will be ignored (as
defined by the cassandra api).

Optional kw args:
  - :super :  used as the super column name if specified
  - :start : The column name to start the slice with.
  - :finish : The column name to stop the slice at
  - :reversed (bool): Whether the results should be ordered in reversed order.
  - :count : How many columns to return, defaults to 100
  - :columns: A list of column names to retrieve

  - :consistency : optional consistency-level, defaults to :one
  - :schema : schema used for result decoding
  - :output : output format (if nil it will return casyn types,
              if :as-map it will try to turn collections to maps"
  [^Client client cf index-clause-args
   & {:keys [super consistency schema output]
      :as opts}]
  (wrap-result-channel+schema
   (.get_indexed_slices client
                        (column-parent cf super)
                        (index-clause index-clause-args)
                        (slice-predicate opts)
                        (consistency-level consistency))
    schema output))

(defn get-paged-slice
  "DEPRECATED: use get-range-slice instead"
  [^Client client cf
   & {:keys [super consistency schema output]
      :as opts}]
  (wrap-result-channel+schema
   (.get_paged_slice client
                     cf
                     (key-range opts)
                     (-> opts :start-column codecs/clojure->byte-buffer)
                     (consistency-level consistency))
    schema output))

(defn truncate
  "Removes all the rows from the given column family."
  [^Client client cf]
  (wrap-result-channel (.truncate client cf)))

(defn describe-cluster-name
  "Gets the name of the cluster."
  [^Client client]
  (wrap-result-channel (.describe_cluster_name client)))

(defn describe-keyspace
  "Gets information about the specified keyspace."
  [^Client client ks]
  (wrap-result-channel (.describe_keyspace client ks) ))

(defn describe-keyspaces
  "Gets a list of all the keyspaces configured for the cluster."
  [^Client client]
  (wrap-result-channel (.describe_keyspaces client)))

(defn describe-partitioner
  "Gets the name of the partitioner for the cluster."
  [^Client client]
  (wrap-result-channel (.describe_partitioner client)))

(defn describe-ring
  "Gets the token ring; a map of ranges to host addresses. Represented
  as a set of TokenRange instead of a map from range to list of
  endpoints, because you can't use Thrift structs as map keys:
  https://issues.apache.org/jira/browse/THRIFT-162 for the same
  reason, we can't return a set here, even though order is neither
  important nor predictable."
  [^Client client ks]
  (wrap-result-channel (.describe_ring client ks)))

(defn describe-schema-versions
  "For each schema version present in the cluster, returns a list of
  nodes at that version. Hosts that do not respond will be under the
  key DatabaseDescriptor.INITIAL_VERSION. The cluster is all on the
  same version if the size of the map is 1"
  [^Client client]
  (wrap-result-channel (.describe_schema_versions client)))

(defn describe-snitch
  "Gets the name of the snitch used for the cluster."
  [^Client client]
  (wrap-result-channel (.describe_snitch client)))

(defn describe-splits
  ""
  [^Client client cf start-token end-token keys-per-split]
  (wrap-result-channel (.describe_splits client
                                         cf
                                         start-token end-token
                                         keys-per-split)))

(defn describe-token-map
  ""
  [^Client client]
  (wrap-result-channel (.describe_token_map client)))

(defn describe-version
  "Gets the Thrift API version."
  [^Client client]
  (wrap-result-channel (.describe_version client)))

(defn set-cql-version
  ""
  [^Client client version]
  (wrap-result-channel (.set_cql_version client version)))

(defn prepare-cql-query
  "Prepare a CQL (Cassandra Query Language) statement by compiling and returning
a casyn.types.CqlPreparedResult instance"
  [^Client client query]
  (wrap-result-channel
   (.prepare_cql_query client
                       (codecs/clojure->byte-buffer query)
                       Compression/NONE)))

(defn execute-cql-query
  "Executes a CQL (Cassandra Query Language) statement.
Optional kw args:
  - :schema : schema used for result decoding
  - :output : output format (if nil it will return casyn types,
              if :as-map it will try to turn collections to maps"
  [^Client client  query
   & {:keys [schema output]}]
  (wrap-result-channel+schema
   (.execute_cql_query client
                       (codecs/clojure->byte-buffer query)
                       Compression/NONE)
    schema output))

(defn execute-prepared-cql-query
  "Executes a prepared CQL (Cassandra Query Language) statement by
  passing an id token and a list of variables to bind.

Optional kw args:
  - :schema : schema used for result decoding
  - :output : output format (if nil it will return casyn types,
              if :as-map it will try to turn collections to maps"
  [^Client client item-id values
   & {:keys [schema output]}]
  (wrap-result-channel+schema
   (.execute_prepared_cql_query client
                                (int item-id)
                                (map codecs/clojure->byte-buffer values))
    schema output))

;; Sugar

(defn put
  "Accepts cols as vectors or maps to be applied to cols
  constructors (use maps for simple key vals, use vectors if you need
  to set mutations options:

Optional kw args for mutations when passed as vectors:
  - :type (keyword): Represents the column type
                     defaults :column can also be :counter :super :counter-super
  - :ttl (integer): Allows to specify the Time to live value for the column
  - :timestamp (long): Allows to specify the Timestamp for the column
                       (in nanosecs), defaults to the value for the current time"
  [^Client client cf row-key columns
   & {:keys [consistency type]}]
  (batch-mutate
   client

   {row-key
    {cf (map #(apply mutation %) columns)}}
   :consistency consistency))

;; aliases
(def ^{:doc "Alias to mget-slice"} get-rows mget-slice)
(def ^{:doc "Alias to get-slice"} get-row get-slice)