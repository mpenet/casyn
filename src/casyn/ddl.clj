(ns casyn.ddl
  (:require [casyn.core :as core]
            [casyn.codecs :as codecs])
  (:import [org.apache.cassandra.thrift CfDef KsDef ColumnDef
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

(def column-type
  {:super "Super"
   :standard "Standard"})

;; :: TODO

(defn column-definition
  [name validation-class & [index-name]]
  (let [cdef (ColumnDef. (codecs/clojure->byte-buffer name)
                         (cassandra-types validation-class))]
    (when index-name
      (.setIndex_name cdef (name index-name))
      (.setIndex_type cdef IndexType/KEYS))
    cdef))

(defn column-family-definition
  ""
  [ks-name cf-name
   & {:keys [cf-type comparator-type
             default-validation-class
             replicate-on-write
             column-metadata]}]
  (let [cfd (CfDef. (name ks-name) (name cf-name))]
    (when cf-type
      (.setColumn_type cfd (column-type cf-type)))
    (when comparator-type
      (.setComparator_type cfd (comparator-type cassandra-types)))
    (when default-validation-class
      (.setDefault_validation_class
       cfd
       (default-validation-class cassandra-types)))
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
  ""
  [^Cassandra$AsyncClient client ks-name strategy-class column-family-defs & more]
  (core/wrap-result-channel
   (.system_add_keyspace
    client
    (apply keyspace-definition
           ks-name strategy-class
           (map #(cons ks-name %)
                column-family-defs)
           more))))

(defn update-keyspace
  ""
  [^Cassandra$AsyncClient client ks-name strategy-class column-family-defs & more]
  (core/wrap-result-channel
   (.system_update_keyspace
    client
    (apply keyspace-definition
           ks-name strategy-class
           (map #(cons ks-name %)
                column-family-defs)
           more))))

(defn drop-keyspace
  ""
  [^Cassandra$AsyncClient client ks-name]
  (core/wrap-result-channel
   (.system_drop_keyspace client ks-name)))

(defn add-column-family
  ""
  [^Cassandra$AsyncClient client & cf-args]
  (core/wrap-result-channel
   (.system_add_column_family
    client
    (apply column-family-definition cf-args))))

(defn update-column-family
  ""
  [^Cassandra$AsyncClient client & cf-args]
  (core/wrap-result-channel
   (.system_update_column_family
    client
    (apply column-family-definition cf-args))))

(defn drop-column-family
  ""
  [^Cassandra$AsyncClient client cf-name]
  (core/wrap-result-channel
   (.system_drop_column_family client cf-name)))