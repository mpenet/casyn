(ns casyn.codecs.composite
     (:require [casyn.codecs :as codecs])
     (:import [java.nio ByteBuffer]))

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
  (let [value-bb ^ByteBuffer (codecs/clojure->byte-buffer raw-value)
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

(defn composite
  "Mark a column value|name|key value as composite"
  [x]
  (codecs/mark-as x :composite))

(defmethod codecs/bytes->clojure :composite [composite-types b]
  (->> (map #(codecs/bytes->clojure %1 %2)
            composite-types
            (composite->bytes-values b))
       ;; mark as composite again: consistent read/modify behavior, it
       ;; stays a composite
       composite))

(defmethod codecs/meta-encodable :composite [x]
  (apply composite-expression (map #(vector :eq? %) x)))
