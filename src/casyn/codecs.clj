(ns casyn.codecs
  (:require casyn.types)
  (:import
   [org.apache.cassandra.utils ByteBufferUtil]
   [org.apache.cassandra.thrift
    ColumnOrSuperColumn Column CounterColumn CounterSuperColumn
    SuperColumn KeySlice CqlResult CqlRow CqlResultType CqlPreparedResult]
   [java.nio ByteBuffer]))

(declare composite-expression)

(defprotocol ThriftDecodable
  (thrift->clojure [v] "Transforms a thrift type to clojure friendly type"))

(defprotocol ByteBufferEncodable
  (clojure->byte-buffer [v] "Converts from clojure types to byte-buffers"))

;; extend-protocol messes things up during expansion
(extend-type (Class/forName "[Lorg.apache.cassandra.thrift.ColumnOrSuperColumn;")
  ThriftDecodable
  (thrift->clojure [cs]
    (map thrift->clojure cs)))

(extend-type (Class/forName "[Lorg.apache.cassandra.thrift.KeySlice;")
  ThriftDecodable
  (thrift->clojure [kss]
    (mapv thrift->clojure kss)))

(extend-protocol ThriftDecodable

  ByteBuffer
  (thrift->clojure [b]
    (ByteBufferUtil/getArray b))

  java.util.ArrayList
  (thrift->clojure [a]
    (thrift->clojure (into-array a)))

  java.util.Map
  (thrift->clojure [a]
    (reduce
     (fn [m [k v]]
       (assoc m
         (thrift->clojure k)
         (thrift->clojure v)))
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
     (thrift->clojure (.getColumns sc))))


  CounterSuperColumn
  (thrift->clojure [sc]
    (casyn.types/CounterSuperColumn.
     (.getName sc)
     (thrift->clojure (.getColumns sc))))


  KeySlice
  (thrift->clojure [ks]
    {(.getKey ks)
     (thrift->clojure (.getColumns ks))})

  CqlRow
  (thrift->clojure [r]
    {(.getKey r)
     (mapv thrift->clojure (.getColumns r))})

  CqlResult
  (thrift->clojure [r]
    (condp = (.getType r)
      CqlResultType/INT (.getNum r)
      CqlResultType/ROWS (mapv thrift->clojure (.getRows r))))

  CqlPreparedResult
  (thrift->clojure [c]
    {:item-id (.getItemId c)
     :count (.getCount c)})

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
  (clojure->byte-buffer [k]
    (clojure->byte-buffer (name k)))

  clojure.lang.Symbol
  (clojure->byte-buffer [s]
    (clojure->byte-buffer (str s)))

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

  Object
  (clojure->byte-buffer [b]
    ;; check for a composite
    (if (and (sequential? b)
             (:composite (meta b)))
      (apply composite-expression (map #(vector :eq? %) b))
      (ByteBufferUtil/bytes (prn-str b))))

  nil
  (clojure->byte-buffer [b]
    (java.nio.ByteBuffer/allocate 0)))


(defmulti bytes->clojure
  "Decode byte arrays
   TODO: use isa? protocol maybe"
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

(defmethod bytes->clojure :date [_ v]
  (java.util.Date. ^long (bytes->clojure :long v)))

(defmethod bytes->clojure :uuid [_ u]
  (java.util.UUID/fromString (bytes->clojure :string u)))

(defmethod bytes->clojure :symbol [_ v]
  (symbol (bytes->clojure :string v)))

(defmethod bytes->clojure :clojure [_ v]
  (read-string (bytes->clojure :string v)))

(defmethod bytes->clojure :default [_ v] v)


;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; Composite
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
;; The encoding of a CompositeType column name should be:
;; <component><component><component> ...
;; where <component> is:
;; <length of value><value><'end-of-component' byte>
;; where <length of value> is a 2 bytes unsigned short the and the
;; 'end-of-component' byte should always be 0 for actual column name.

;; However, it can set to 1 for query bounds. This allows to query for the
;; equivalent of 'give me the full super-column'. That is, if during a slice
;; query uses:
;; start = <3><"foo".getBytes()><0>
;; end   = <3><"foo".getBytes()><1>
;; then he will be sure to get *all* the columns whose first component is "foo".

;; If for a component, the 'end-of-component' is != 0, there should not be any
;; following component. The end-of-component can also be -1 to allow
;; non-inclusive query. For instance:
;; start = <3><"foo".getBytes()><-1>
;; allows to query everything that is greater than <3><"foo".getBytes()>, but
;; not <3><"foo".getBytes()> itself.

(def composite-operators
  {:eq? (byte 0)
   :lt? (byte -1)
   :gt? (byte 1)})

(defn composite-value
  "Create a composite value as a ByteBuffer from a raw clojure value
  and a composite operator"
  [raw-value suffix]
  (let [value-bb ^ByteBuffer (clojure->byte-buffer raw-value)
        ;; we need a new bb of capacity 2+(size value)+1
        bb (ByteBuffer/allocate (+ 3 (.capacity value-bb)))]
    (.putShort bb (short (.capacity value-bb))) ;; prefix
    (.put bb value-bb) ;; actual value
    (.put bb ^byte suffix) ;; eoc suffix
    (.rewind bb)))

(defn composite-expression
  "Takes a sequence of values pairs holding operator and actual clojure value to
be encoded as a composite type. Returns a ByteBuffer
ex: (composite-expression [:eq? 12] [:gt? \"meh\"] [:lt? 12])"
  [& values]
  (let [bbv (map (fn [[op v]]
                   (composite-value v (get composite-operators op)))
                 values)
        bb (ByteBuffer/allocate (reduce (fn [s bb]
                                          (+ s (.capacity ^ByteBuffer bb)))
                                        0
                                        bbv))]
    (doseq [v bbv]
      (.put bb ^ByteBuffer v))

    (.rewind bb)))

(defn composite->bytes-values
  "Takes a composite byte-array returned by thrift and transforms it
  to a collection of byte-arrays holding actual values for decoding"
  [ba]
  (let [bb (ByteBuffer/wrap ba)]
    (loop [values []]
      (if (> (.remaining bb) 0)
        (let [dest (byte-array (.getShort bb))] ;; create the output buffer for the value
          (.get bb dest)                        ;; fill it
          (.position bb (inc (.position bb))) ;; skip the last eoc byte
          (recur (conj values dest)))
        values))))

(defmethod bytes->clojure :composite [composite-types vs]
  (map #(bytes->clojure %1 %2)
       composite-types
       (composite->bytes-values vs)))

(defn composite
  "Mark a column value|name|key value as composite"
  [& values]
  (vary-meta values assoc :composite true))


;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; UUIDs
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
