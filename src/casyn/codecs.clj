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

  ByteBuffer
  (clojure->byte-buffer [b] b)

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
  (clojure->byte-buffer [b]
    ;; it it s a vector and it has meta:composite present use this value
    ;; else forward to next possible encoder
    (let [m (meta b)]
      (if-let [composite-value (:composite m)]
        composite-value
        (clojure->byte-buffer b))))

  Object
  (clojure->byte-buffer [b]
    ;; we asume it s a clojure data structure, if you want to escape
    ;; this for other types extend this protocol to do so
    (ByteBufferUtil/bytes (prn-str b)))

  nil
  (clojure->byte-buffer [b]
    (java.nio.ByteBuffer/allocate 0)))


(defmulti bytes->clojure
  ""
  (fn [val-type v]
    (if (keyword? val-type)
      val-type
      :composite)))

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
  "Turns a collection of columns into an array-map with column name mapped to key"
  [cols]
  (apply array-map (mapcat (juxt :name :value) cols)))

;; Composite

;; /*
;;  * The encoding of a CompositeType column name should be:
;;  *   <component><component><component> ...
;;  * where <component> is:
;;  *   <length of value><value><'end-of-component' byte>
;;  * where <length of value> is a 2 bytes unsigned short the and the
;;  * 'end-of-component' byte should always be 0 for actual column name.

;;  * However, it can set to 1 for query bounds. This allows to query for the
;;  * equivalent of 'give me the full super-column'. That is, if during a slice
;;  * query uses:
;;  *   start = <3><"foo".getBytes()><0>
;;  *   end   = <3><"foo".getBytes()><1>
;;  * then he will be sure to get *all* the columns whose first component is "foo".

;;  * If for a component, the 'end-of-component' is != 0, there should not be any
;;  * following component. The end-of-component can also be -1 to allow
;;  * non-inclusive query. For instance:
;;  *   start = <3><"foo".getBytes()><-1>
;;  * allows to query everything that is greater than <3><"foo".getBytes()>, but
;;  * not <3><"foo".getBytes()> itself.
;;  */

(def eoc   (byte 0))
(def eoc-> (byte 1))
(def eoc-< (byte -1))

(defn encode-composite-value
  [raw-value suffix]
  (let [value-bb ^ByteBuffer (clojure->byte-buffer raw-value)
        ;; we need a new bb of capacity 2+(size value)+1
        bb (ByteBuffer/allocate (+ 3 (.capacity value-bb)))]
    (.putShort bb (short (.capacity value-bb))) ;; prefix
    (.put bb value-bb) ;; actual value
    (.put bb ^byte suffix) ;; eoc suffix
    (.rewind bb)))

(defn decode-composite-column
  "Takes a composite value as byte-array and transforms it to a collection of
byte-arrays individual values"
  [ba]
  (let [bb (ByteBuffer/wrap ba)]
    (loop [bb bb
           values []]
      (if (> (.remaining bb) 0)
        (let [dest (byte-array (.getShort bb))] ;; create the output buffer for the value
          (.get bb dest) ;; fill it
          (.position bb (inc (.position bb))) ;; skip the last eoc byte
          (recur bb (conj values dest)))
        values))))

(defmethod bytes->clojure :composite [composite-types vs]
  (map #(bytes->clojure %1 %2)
       composite-types
       (decode-composite-column vs)))

(defn composite-encoder-generator
  "Takes a collection of values and adds the composite bytebuffer value as
metadata.
This allows to have similar values between encoded/decoded, minus the
composite metadata"
  [eoc]
  (fn [& values]
    (let [bbv (map #(encode-composite-value % eoc) values)
          bb (ByteBuffer/allocate (reduce (fn [s bb]
                                            (+ s (.capacity ^ByteBuffer bb)))
                                          0
                                          bbv))]
      (doseq [v bbv]
        (.put bb ^ByteBuffer v))

      (.rewind bb)

      (vary-meta values assoc :composite bb))))

(def composite (composite-encoder-generator eoc))
(def composite-< (composite-encoder-generator eoc-<))
(def composite-> (composite-encoder-generator eoc->))
