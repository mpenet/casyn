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

  Object
  (clojure->byte-buffer [b]
    (ByteBufferUtil/bytes b))

  clojure.lang.Sequential
  (clojure->byte-buffer [v]
    (map clojure->byte-buffer v))

  nil
  (clojure->byte-buffer [b]
    (java.nio.ByteBuffer/allocate 0)))


(defmulti bytes->clojure (fn [type-key v] type-key))

(defmethod bytes->clojure :string [_  v]
  (ByteBufferUtil/string (ByteBuffer/wrap v)))

(defmethod bytes->clojure :long [_  v]
  (ByteBufferUtil/toLong (ByteBuffer/wrap v)))

(defmethod bytes->clojure :float  [_ v]
  (ByteBufferUtil/toFloat (ByteBuffer/wrap v)))

(defmethod bytes->clojure :double [_ v]
  (ByteBufferUtil/toDouble (ByteBuffer/wrap v)))

(defmethod bytes->clojure :int [_ v]
  (ByteBufferUtil/toDouble (ByteBuffer/wrap v)))

(defmethod bytes->clojure :clojure [_ v]
  (read-string (bytes->clojure :string v)))

(defmethod bytes->clojure :default [_ v] v)