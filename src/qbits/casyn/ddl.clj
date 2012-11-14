(ns qbits.casyn.ddl
  (:require
   [qbits.casyn.api :as api]
   [qbits.casyn.codecs :as codecs])
  (:import
   [org.apache.cassandra.thrift CfDef KsDef ColumnDef
    Cassandra$AsyncClient IndexType]))


;; AbstractCommutativeType, AbstractCompositeType, AbstractUUIDType,
;; AsciiType, BooleanType, BytesType, DateType, DoubleType, FloatType,
;; IntegerType, LocalByPartionerType, LongType, ReversedType, UTF8Type

;; default_validation_class Specifies a validator class to use for
;; validating all the column values in the column family. Valid values
;; are AsciiType, BytesType, IntegerType, LexicalUUIDType, LongType,
;; TimeUUIDTYpe, and UTF8Type. It is possible to implement additional
;; validators by creating custom validation classes.

;; compare_with This attribute defines the sort algorithm which will
;; be used to compare columns. Users may customize this behavior by
;; extending org.apache.cassandra.db.marshal.AbstractType. The
;; different values available for CompareWith are detailed below:

;; Type	Description
;; BytesType	Simple non-validating byte comparison (Default)
;; AsciiType	Similar to BytesType, but validates that input is US-ASCII
;; UTF8Type	UTF-8 encoded string comparison
;; LongType	Compares values as 64 bit longs
;; LexicalUUIDType	128 bit UUID compared by byte value
;; TimeUUIDType	Timestamp compared 128 bit version 1 UUID

(def cassandra-types
  {:ascii             "AsciiType"
   :bytes             "BytesType"
   :composite         "CompositeType"
   :counter           "CounterColumnType"
   :double            "DoubleType"
   :dynamic-composite "DynamicCompositeTYpe"
   :integer           "IntegerType"
   :lexical-uuid      "LeixcalUUIDType"
   :local-partitioner "LocalByPartionerType"
   :long              "LongType"
   :time-uuid         "TimeUUIDType"
   :utf-8             "UTF8Type"
   :uuid              "UUIDType"})

(def join-comma (partial clojure.string/join ","))

(defprotocol PCassandraType
  (clj->cassandra-type [td] "Converts a clj friendly type def to a proper cassandra type"))

(extend-protocol PCassandraType

  clojure.lang.IPersistentMap
  (clj->cassandra-type [m]
    (clojure.string/join "" (map clj->cassandra-type m)))

  clojure.lang.MapEntry
  (clj->cassandra-type [[k v]]
    (format "%s(%s)" (clj->cassandra-type k) (clj->cassandra-type v)))

  clojure.lang.Sequential
  (clj->cassandra-type [s]
    (join-comma (map clj->cassandra-type s)))

  clojure.lang.Keyword
  (clj->cassandra-type [k] (get cassandra-types k))

  String
  (clj->cassandra-type [s] s))


(def column-type
  {:super "Super"
   :standard "Standard"})

;; TODO: docstrings

(defn column-definition
  [col-name validation-class & [index-name]]
  (let [cdef (ColumnDef. (codecs/clojure->byte-buffer col-name)
                         (clj->cassandra-type validation-class))]
    (when index-name
      (.setIndex_name cdef (name index-name))
      (.setIndex_type cdef IndexType/KEYS))
    cdef))

(defn column-family-definition
  ""
  [ks-name cf-name
   & {:keys [cf-type
             comparator-type
             default-validation-class
             key-validation-class
             replicate-on-write
             column-metadata]}]
  (let [cfd (CfDef. ks-name cf-name)]
    (when cf-type
      (.setColumn_type cfd (column-type cf-type)))
    (when comparator-type
      (.setComparator_type cfd (clj->cassandra-type comparator-type)))
    (when default-validation-class
      (.setDefault_validation_class
       cfd
       (clj->cassandra-type default-validation-class)))
    (when key-validation-class
      (.setKey_validation_class
       cfd
       (clj->cassandra-type key-validation-class)))
    (when replicate-on-write
      (.setReplicate_on_write cfd replicate-on-write))
    (when column-metadata
      (.setColumn_metadata cfd (map #(apply column-definition %) column-metadata)))
    cfd))

(defn keyspace-definition
  ""
  [ks-name strategy-class column-family-definitions
   & {:keys [durable-writes strategy-options]}]
  (let [ksd (KsDef. (name ks-name)
                    strategy-class
                    (map #(apply column-family-definition %)
                         column-family-definitions))]
    (when strategy-options
      (.setStrategy_options ksd strategy-options))
    (when durable-writes (.setDurable_writes ksd durable-writes))
    ksd))

(defn add-keyspace
  [client ks-name strategy-class column-family-defs & more]
  (api/wrap-result-channel
   (.system_add_keyspace
    client
    (apply keyspace-definition
           ks-name strategy-class
           (map #(cons ks-name %)
                column-family-defs)
           more))))

(defn update-keyspace
  ""
  [client ks-name strategy-class column-family-defs & more]
  (api/wrap-result-channel
   (.system_update_keyspace
    client
    (apply keyspace-definition
           ks-name strategy-class
           (map #(cons ks-name %)
                column-family-defs)
           more))))

(defn drop-keyspace
  ""
  [client ks-name]
  (api/wrap-result-channel
   (.system_drop_keyspace client ks-name)))

(defn add-column-family
  ""
  [client & cf-args]
  (api/wrap-result-channel
   (.system_add_column_family
    client
    (apply column-family-definition cf-args))))

(defn update-column-family
  ""
  [client & cf-args]
  (api/wrap-result-channel
   (.system_update_column_family
    client
    (apply column-family-definition cf-args))))

(defn drop-column-family
  ""
  [client cf-name]
  (api/wrap-result-channel
   (.system_drop_column_family client cf-name)))