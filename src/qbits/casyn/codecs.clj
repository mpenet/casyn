(ns qbits.casyn.codecs
  (:require
   qbits.casyn.types
   [taoensso.nippy :as nippy]
   [qbits.tardis :as uuid])
  (:import
   [org.apache.cassandra.db.marshal
    UTF8Type Int32Type IntegerType AsciiType
    BytesType DoubleType LongType FloatType UUIDType LexicalUUIDType DateType
    BooleanType CompositeType ListType MapType SetType EmptyType]
   [java.nio ByteBuffer]))

(declare meta-encodable)

(defprotocol ByteBufferEncodable
  (clojure->byte-buffer [v] "Converts from clojure types to byte-buffers"))

(extend-protocol ByteBufferEncodable

  (Class/forName "[B")
  (clojure->byte-buffer [b]
    (.decompose BytesType/instance b))

  ByteBuffer
  (clojure->byte-buffer [b] b)

  String
  (clojure->byte-buffer [s] (.decompose UTF8Type/instance s))

  Boolean
  (clojure->byte-buffer [b]
    (.decompose BooleanType/instance b))

  Long
  (clojure->byte-buffer [l]
    (.decompose LongType/instance l))

  Integer
  (clojure->byte-buffer [i]
    (.decompose Int32Type/instance i))

  BigInteger
  (clojure->byte-buffer [i]
    (.decompose IntegerType/instance i))

  Double
  (clojure->byte-buffer [d]
    (.decompose DoubleType/instance d))

  Float
  (clojure->byte-buffer [f]
    (.decompose FloatType/instance f))

  java.util.Date
  (clojure->byte-buffer [d]
    (.decompose DateType/instance d))

  java.util.UUID
  (clojure->byte-buffer [u]
    (.decompose UUIDType/instance u))

  org.apache.cassandra.cql.jdbc.JdbcLexicalUUID
  (clojure->byte-buffer [u]
    (.decompose LexicalUUIDType/instance u))

  com.eaio.uuid.UUID
  (clojure->byte-buffer [u]
    (clojure->byte-buffer (str u)))

  clojure.lang.Keyword
  (clojure->byte-buffer [k]
    (clojure->byte-buffer (name k)))

  Object
  (clojure->byte-buffer [o]
    ;; try to find out if it a custom type else just serialize as :clj
    (meta-encodable o))

  nil
  (clojure->byte-buffer [b]
    (.decompose EmptyType/instance b)))


(defmulti bytes->clojure
  "Decode byte arrays from schema type definitions"
  (fn [val-type v]
    (if (keyword? val-type)
      val-type
      :composite)))

(defmethod bytes->clojure :utf-8 [_  b]
  (.compose UTF8Type/instance (ByteBuffer/wrap b)))

(defmethod bytes->clojure :ascii [_  b]
  (.compose AsciiType/instance (ByteBuffer/wrap b)))

(defmethod bytes->clojure :keyword [_  b]
  (keyword (bytes->clojure :utf-8 b)))

(defmethod bytes->clojure :long [_  b]
  (.compose LongType/instance (ByteBuffer/wrap b)))

(defmethod bytes->clojure :float  [_ b]
  (.compose FloatType/instance (ByteBuffer/wrap b)))

(defmethod bytes->clojure :double [_ b]
  (.compose DoubleType/instance (ByteBuffer/wrap b)))

(defmethod bytes->clojure :int [_ b]
  (.compose Int32Type/instance (ByteBuffer/wrap b)))

(defmethod bytes->clojure :big-int [_ b]
  (.compose IntegerType/instance (ByteBuffer/wrap b)))

(defmethod bytes->clojure :boolean [_ b]
  (.compose BooleanType/instance (ByteBuffer/wrap b)))

(defmethod bytes->clojure :date [_ b]
  (.compose DateType/instance (ByteBuffer/wrap b)))

(defmethod bytes->clojure :lexical-uuid [_ b]
  (.compose LexicalUUIDType/instance (ByteBuffer/wrap b)))

(defmethod bytes->clojure :uuid [_ b]
  (.compose UUIDType/instance (ByteBuffer/wrap b)))

(defmethod bytes->clojure :lexical-uuid [_ b]
  (.compose LexicalUUIDType/instance (ByteBuffer/wrap b)))

(defmethod bytes->clojure :time-uuid [_ b]
  (uuid/to-uuid (bytes->clojure :utf-8 b)))

(defmethod bytes->clojure :clj [_ b]
  (nippy/thaw-from-bytes b))

(defmethod bytes->clojure :default [_ b] b)



;; encoder for types with meta+:casyn info
(defmulti meta-encodable (fn [x] (-> x meta :casyn :type)))

(defmethod meta-encodable :default [x] ;; will work for clj too
  (-> x nippy/freeze-to-bytes ByteBuffer/wrap))

;; special data types markers

(defn mark-as
  ""
  [x type-key]
  (vary-meta x assoc-in [:casyn :type] type-key))

;; 1.1 types

(defn clj
  "Mark a column value|name|key value as serializable"
  [x]
  (mark-as x :clj))

;; Prepare for cassandra 1.2 new collections types

(defn clist
  [x]
  (mark-as x :list))

(defn cset
  [x]
  (mark-as x :set))

(defn cset
  [x]
  (mark-as x :map))
