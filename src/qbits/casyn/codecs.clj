(ns qbits.casyn.codecs
  (:require
   qbits.casyn.types
   [taoensso.nippy :as nippy]
   [qbits.tardis :as uuid])
  (:import
   [org.apache.cassandra.db.marshal
    UTF8Type Int32Type IntegerType AsciiType AbstractType
    BytesType DoubleType LongType FloatType UUIDType LexicalUUIDType DateType
    BooleanType CompositeType ListType MapType SetType EmptyType]
   [java.nio ByteBuffer]))

(declare meta-encode)

(defprotocol ByteBufferEncodable
  (clojure->byte-buffer [v] "Converts from clojure types to byte-buffers"))

(extend-protocol ByteBufferEncodable

  (Class/forName "[B")
  (clojure->byte-buffer [b]
    (.decompose BytesType/instance b))

  ByteBuffer
  (clojure->byte-buffer [b] b)

  String
  (clojure->byte-buffer [s]
    (.decompose UTF8Type/instance s))

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
    (meta-encode o))

  nil
  (clojure->byte-buffer [b]
    (.decompose EmptyType/instance b)))

(defn compose
  [type-instance b]
  (.compose ^AbstractType type-instance
            (ByteBuffer/wrap b)))

(defmulti bytes->clojure
  "Decode byte arrays from schema type definitions"
  (fn [val-type v]
    (if (keyword? val-type)
      val-type
      :composite)))

(defmethod bytes->clojure :utf-8 [_  b] (compose UTF8Type/instance b))
(defmethod bytes->clojure :ascii [_  b] (compose AsciiType/instance b))
(defmethod bytes->clojure :long [_  b] (compose LongType/instance b))
(defmethod bytes->clojure :float  [_ b] (compose FloatType/instance b))
(defmethod bytes->clojure :double [_ b] (compose DoubleType/instance b))
(defmethod bytes->clojure :int [_ b] (compose Int32Type/instance b))
(defmethod bytes->clojure :big-int [_ b] (compose IntegerType/instance b))
(defmethod bytes->clojure :boolean [_ b] (compose BooleanType/instance b))
(defmethod bytes->clojure :date [_ b] (compose DateType/instance b))
(defmethod bytes->clojure :lexical-uuid [_ b] (compose LexicalUUIDType/instance b))
(defmethod bytes->clojure :uuid [_ b] (compose UUIDType/instance b))
(defmethod bytes->clojure :keyword [_  b] (keyword (bytes->clojure :utf-8 b)))
(defmethod bytes->clojure :time-uuid [_ b] (uuid/to-uuid (bytes->clojure :utf-8 b)))
(defmethod bytes->clojure :clj [_ b] (nippy/thaw-from-bytes b))
(defmethod bytes->clojure :default [_ b] b)

;; Encoder for types with meta+:casyn info
(defmulti meta-encode (fn [x] (-> x meta :casyn :type)))

(defmethod meta-encode :default [x]
  (-> x nippy/freeze-to-bytes ByteBuffer/wrap))

;; Special data types markers

(defn mark-as
  "Marks a value as a Cassandra native (composite, collection and
  possibly others in the future)"
  [x type-key]
  (vary-meta x assoc-in [:casyn :type] type-key))
