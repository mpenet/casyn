(ns casyn.codecs
  (:require
   casyn.types
   [taoensso.nippy :as nippy]
   [tardis.core :as uuid])
  (:import
   [org.apache.cassandra.utils ByteBufferUtil]
   [java.nio ByteBuffer]))

(declare meta-encodable)

(defprotocol ByteBufferEncodable
  (clojure->byte-buffer [v] "Converts from clojure types to byte-buffers"))

(extend-protocol ByteBufferEncodable

  (Class/forName "[B")
  (clojure->byte-buffer [b]
    (ByteBuffer/wrap b))

  ByteBuffer
  (clojure->byte-buffer [b] b)

  String
  (clojure->byte-buffer [s]
    (ByteBuffer/wrap (.getBytes s "utf-8")))

  clojure.lang.Keyword
  (clojure->byte-buffer [k]
    (clojure->byte-buffer (name k)))

  Boolean
  (clojure->byte-buffer [b]
    (ByteBufferUtil/bytes
     (int (if b 1 0))))

  Long
  (clojure->byte-buffer [l]
    (ByteBufferUtil/bytes ^long l))

  Integer
  (clojure->byte-buffer [i]
    (ByteBufferUtil/bytes ^int i))

  Double
  (clojure->byte-buffer [d]
    (ByteBufferUtil/bytes ^double d))

  Float
  (clojure->byte-buffer [f]
    (ByteBufferUtil/bytes ^float f))

  java.util.Date
  (clojure->byte-buffer [d]
    (clojure->byte-buffer (.getTime d)))

  java.util.UUID
  (clojure->byte-buffer [u]
    (clojure->byte-buffer (.toString u)))

  com.eaio.uuid.UUID
  (clojure->byte-buffer [u]
    (clojure->byte-buffer (str u)))

  Object
  (clojure->byte-buffer [o]
    ;; try to find out if it a custom type else just serialize as :clj
    (meta-encodable o))

  nil
  (clojure->byte-buffer [b]
    (java.nio.ByteBuffer/allocate 0)))


(defmulti bytes->clojure
  "Decode byte arrays from schema type definitions"
  (fn [val-type v]
    (if (keyword? val-type)
      val-type
      :composite)))

(defmethod bytes->clojure :utf-8 [_  b]
  (ByteBufferUtil/string (ByteBuffer/wrap b)))

(defmethod bytes->clojure :ascii [_  b]
  (ByteBufferUtil/string (ByteBuffer/wrap b)
                         sun.nio.cs.US_ASCII))

(defmethod bytes->clojure :keyword [_  b]
  (keyword (bytes->clojure :utf-8 b)))

(defmethod bytes->clojure :long [_  b]
  (ByteBufferUtil/toLong (ByteBuffer/wrap b)))

(defmethod bytes->clojure :float  [_ b]
  (ByteBufferUtil/toFloat (ByteBuffer/wrap b)))

(defmethod bytes->clojure :double [_ b]
  (ByteBufferUtil/toDouble (ByteBuffer/wrap b)))

(defmethod bytes->clojure :int [_ b]
  (ByteBufferUtil/toInt (ByteBuffer/wrap b)))

(defmethod bytes->clojure :boolean [_ b]
  (= 1 (bytes->clojure :int b)))

(defmethod bytes->clojure :date [_ b]
  (java.util.Date. ^long (bytes->clojure :long b)))

(defmethod bytes->clojure :uuid [_ b]
  (java.util.UUID/fromString (bytes->clojure :utf-8 b)))

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
