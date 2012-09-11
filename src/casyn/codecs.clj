(ns casyn.codecs
  (:require casyn.types
            [taoensso.nippy :as nippy]
            [tardis.core :as uuid]
            [clj-time.coerce :as ct-c])
  (:import [org.apache.cassandra.utils ByteBufferUtil]
           [org.apache.cassandra.thrift
            ColumnOrSuperColumn Column CounterColumn CounterSuperColumn
            SuperColumn KeySlice CqlResult CqlRow CqlResultType CqlPreparedResult]
           [java.nio ByteBuffer]))

(declare composite-expression meta-encodable)

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
    (into-array (map thrift->clojure kss))))

(extend-protocol ThriftDecodable

  ByteBuffer
  (thrift->clojure [b]
    (ByteBufferUtil/getArray b))

  java.util.ArrayList
  (thrift->clojure [a]
    (when (> (count a) 0)
      (thrift->clojure (into-array a))))

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
    (casyn.types.CounterSuperColumn.
     (.getName sc)
     (thrift->clojure (.getColumns sc))))

  KeySlice
  (thrift->clojure [ks]
    (casyn.types.KeySlice. (.getKey ks)
                           (map thrift->clojure (.getColumns ks))))

  CqlRow
  (thrift->clojure [r]
    (casyn.types.CqlRow. (.getKey r)
                         (map thrift->clojure (.getColumns r))))

  CqlResult
  (thrift->clojure [r]
    (-> (casyn.types.CqlResult. (.getNum r)
                                (.getType r)
                                (map thrift->clojure (.getRows r)))
        (with-meta {:schema (.getSchema r)})))

  CqlPreparedResult
  (thrift->clojure [c]
    (casyn.types.CqlPreparedResult. (.getItemId c)
                                    (.getCount c)
                                    (.getVariable_names c)
                                    (.getVariable_types c)))

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

  org.joda.time.DateTime
  (clojure->byte-buffer [dt]
    (clojure->byte-buffer (ct-c/to-long dt)))

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

(defmethod bytes->clojure :string [_  b]
  (ByteBufferUtil/string (ByteBuffer/wrap b)))

(defmethod bytes->clojure :keyword [_  b]
  (keyword (bytes->clojure :string b)))

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

(defmethod bytes->clojure :date-time [_ b]
  (ct-c/from-long (bytes->clojure :long b)))

(defmethod bytes->clojure :uuid [_ b]
  (java.util.UUID/fromString (bytes->clojure :string b)))

(defmethod bytes->clojure :time-uuid [_ b]
  (uuid/to-uuid (bytes->clojure :string b)))

(defmethod bytes->clojure :clj [_ b]
  (nippy/thaw-from-bytes b))

(defmethod bytes->clojure :default [_ b] b)


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

(defmethod bytes->clojure :composite [composite-types b]
  (->> (map #(bytes->clojure %1 %2)
            composite-types
            (composite->bytes-values b))
       ;; mark as composite again: consistent read/modify behavior, it
       ;; stays a composite
       composite))

;; encoder for types with meta+:casyn info

(defmulti meta-encodable (fn [x] (-> x meta :casyn :type)))

(defmethod meta-encodable :composite [x]
  (apply composite-expression (map #(vector :eq? %) x)))

(defmethod meta-encodable :default [x] ;; will work for clj too
  (-> x nippy/freeze-to-bytes ByteBuffer/wrap))

;; special data types markers

(defn mark-as
  ""
  [x type-key]
  (vary-meta x assoc-in [:casyn :type] type-key))

;; 1.1 types

(defn composite
  "Mark a column value|name|key value as composite"
  [x]
  (mark-as x :composite))

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
