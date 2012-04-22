(ns casyn.codecs
  (:require casyn.types)
  (:import
   [org.apache.cassandra.utils ByteBufferUtil]
   [org.apache.cassandra.thrift
    ColumnOrSuperColumn Column CounterColumn CounterSuperColumn
    SuperColumn KeySlice]
   [java.nio ByteBuffer]))

(defprotocol ThriftDecodable
  (thrift->clojure [v] "Transforms a thrift type to clojure friendly type"))

(defprotocol ByteBufferEncodable
  (clojure->byte-buffer [v] "Converts from clojure types to byte-buffers"))

(extend-protocol ThriftDecodable

  ByteBuffer
  (thrift->clojure [b]
    (ByteBufferUtil/getArray b))

  java.util.ArrayList
  (thrift->clojure [a]
    (map thrift->clojure a))

  java.util.Map
  (thrift->clojure [a]
    (reduce
     (fn [m [k v]]
       (assoc m (thrift->clojure k) (thrift->clojure v)))
     (array-map)
     a))

  Column
  (thrift->clojure [c]
    (casyn.types.Column. (.getName c)
                         (.getValue c)
                         (.getTtl c)
                         (.getTimestamp c)))

  CounterColumn
  (thrift->clojure [c]
    (casyn.types.CounterColumn. (.getName c)
                                (.getValue c)))

  ColumnOrSuperColumn
  (thrift->clojure [c]
    (thrift->clojure (or (.column c)
                         (.counter_column c)
                         (.super_column c)
                         (.counter_super_column c))))

  SuperColumn
  (thrift->clojure [sc]
    (casyn.types.SuperColumn.
     (.getName sc)
     (map thrift->clojure (.getColumns sc))))


  CounterSuperColumn
  (thrift->clojure [sc]
    (casyn.types/CounterSuperColumn.
     (.getName sc)
     (map thrift->clojure (.getColumns sc))))

  KeySlice
  (thrift->clojure [ks]
    (casyn.types.KeySlice. (.getKey ks)
                           (thrift->clojure (.getColumns ks))))

  Object
  (thrift->clojure [v] v)

  nil
  (thrift->clojure [v] v))


(extend-protocol ByteBufferEncodable

  (Class/forName "[B")
  (clojure->byte-buffer [b]
    (ByteBuffer/wrap b))

  String
  (clojure->byte-buffer [s]
    (ByteBuffer/wrap (.getBytes s "utf-8")))

  clojure.lang.Keyword
  (clojure->byte-buffer [s]
    (clojure->byte-buffer (name s)))

  clojure.lang.Symbol
  (clojure->byte-buffer [s]
    (clojure->byte-buffer (str s)))

  Boolean
  (clojure->byte-buffer [b]
    (ByteBufferUtil/bytes
     (int (if b 1 0))))

  Long
  (clojure->byte-buffer [b]
    (ByteBufferUtil/bytes ^long b))

  Integer
  (clojure->byte-buffer [b]
    (ByteBufferUtil/bytes ^int b))

  Double
  (clojure->byte-buffer [b]
    (ByteBufferUtil/bytes ^double b))

  Float
  (clojure->byte-buffer [b]
    (ByteBufferUtil/bytes ^float b))

  clojure.lang.Sequential
  (clojure->byte-buffer [v]
    (map clojure->byte-buffer v))

  Object
  (clojure->byte-buffer [b]
    ;; we asume it s a clojure data structure, if you want to escape
    ;; this for other types extend this protocol to do so
    (ByteBufferUtil/bytes (prn-str b)))

  nil
  (clojure->byte-buffer [b]
    (java.nio.ByteBuffer/allocate 0)))


(defmulti bytes->clojure (fn [type-key v] type-key))

(defmethod bytes->clojure :string [_  v]
  (ByteBufferUtil/string (ByteBuffer/wrap v)))

(defmethod bytes->clojure :keyword [_  v]
  (keyword (bytes->clojure :string v)))

(defmethod bytes->clojure :long [_  v]
  (ByteBufferUtil/toLong (ByteBuffer/wrap v)))

(defmethod bytes->clojure :float  [_ v]
  (ByteBufferUtil/toFloat (ByteBuffer/wrap v)))

(defmethod bytes->clojure :double [_ v]
  (ByteBufferUtil/toDouble (ByteBuffer/wrap v)))

(defmethod bytes->clojure :int [_ v]
  (ByteBufferUtil/toInt (ByteBuffer/wrap v)))

(defmethod bytes->clojure :boolean [_ v]
  (= 1 (bytes->clojure :int v)))

(defmethod bytes->clojure :symbol [_ v]
  (symbol (bytes->clojure :string v)))

(defmethod bytes->clojure :clojure [_ v]
  (read-string (bytes->clojure :string v)))

(defmethod bytes->clojure :default [_ v] v)

(defn cols->map
  [cols]
  (apply array-map (mapcat (juxt :name :value) cols)))