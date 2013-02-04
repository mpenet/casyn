(ns qbits.casyn.codecs.collection
  "Encoding/decoding of native Cassandra collection types."
  (:require
   [qbits.casyn.codecs :as codecs])
  (:import
   [java.nio ByteBuffer]))

;; Encoding markers

(defn c*collection
  "Marks a clojure value as cassandra native collection for encoding"
  [x]
  (codecs/mark-as x :collection))

(defprotocol PCollection
  (encode [this] "Encodes a clojure coll to Cassandra bytes value")
  (decode [this bytes spec] "Decodes Cassandra collection from bytes to clojure value"))

(defn pack
  "Packs a collection of buffers into a single value"
  [bbs elements size]
  (let [result (ByteBuffer/allocate (+ 2 size))]
    (.putShort result (short elements))
    (doseq [^ByteBuffer bb bbs]
      (.putShort result (short (.remaining bb)))
      (.put result (.duplicate bb)))
    (.flip result)))

(extend-protocol PCollection
  ;; Layout is: {@code <n><sk_1><k_1><sv_1><v_1>...<sk_n><k_n><sv_n><v_n> }
  ;; where:
  ;;  n is the number of elements
  ;;  sk_i is the number of bytes composing the ith key k_i
  ;;  k_i is the sk_i bytes composing the ith key
  ;;  sv_i is the number of bytes composing the ith value v_i
  ;;  v_i is the sv_i bytes composing the ith value
  clojure.lang.IPersistentMap
  (encode [m]
    (loop [m m
           bbs []
           elements 0
           size 0]
      (if-let [e (first m)]
        (let [^ByteBuffer bbk (-> e key codecs/clojure->byte-buffer)
              ^ByteBuffer bbv (-> e val codecs/clojure->byte-buffer)]
          (recur (rest m)
                 (conj bbs bbk bbv)
                 (inc elements)
                 (+ size 4 (.remaining bbk) (.remaining bbv))))
        (pack bbs elements size))))

  (decode [coll bytes spec]
    (let [bb (ByteBuffer/wrap bytes)
          elements (.getShort bb)
          [key-type val-type] (-> spec first val)
          m (transient coll)]
      (loop [m m]
        (if (= (.remaining bb) 0)
          (-> m persistent! c*collection)
          (let [bak (byte-array (.getShort bb))]
            (.get bb bak) ;; fill key data
            (let [bav (byte-array (.getShort bb))]
              (.get bb bav) ;; fill value data
              (recur (assoc! m
                             (codecs/bytes->clojure key-type bak)
                             (codecs/bytes->clojure val-type bav)))))))))

  ;; Layout is: {@code <n><s_1><b_1>...<s_n><b_n> }
  ;; where:
  ;;  n is the number of elements
  ;;  s_i is the number of bytes composing the ith element
  ;;  b_i is the s_i bytes composing the ith element
  clojure.lang.IPersistentCollection
  (encode [xs]
    (loop [xs xs
           bbs []
           elements 0
           size 0]
      (if-let [x (first xs)]
        (let [bb (codecs/clojure->byte-buffer x)]
          (recur
           (rest xs)
           (conj bbs bb)
           (inc elements)
           (+ size (.remaining ^ByteBuffer bb) 2)))
        (pack bbs elements size))))

  (decode [m bytes spec]
    (let [coll-type (val (first spec))
          ^ByteBuffer bb (ByteBuffer/wrap bytes)
          coll (transient m)
          elements (.getShort bb)] ;; skip length
      (loop [coll coll]
        (if (= (.remaining bb) 0)
          (-> coll persistent! c*collection)
          (let [ba (byte-array (.getShort bb))]
            (.get bb ba)
            (recur (conj! coll (codecs/bytes->clojure coll-type ba)))))))))

(defmethod codecs/meta-encode :collection [xs]
  (encode xs))

(defmethod codecs/bytes->clojure :list [spec bytes]
  (decode [] bytes spec))

(defmethod codecs/bytes->clojure :set [spec bytes]
  (decode #{} bytes spec))

(defmethod codecs/bytes->clojure :map [spec bytes]
  (decode {} bytes spec))