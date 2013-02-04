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
  (encode [this] "Encodes a clojure coll to Cassandra representation"))

(defmethod codecs/meta-encode :collection [xs]
  (encode xs))

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
        (pack bbs elements size)))))


(defn coll->clojure
  [ba-coll coll-spec]
  (let [coll-type (val (first coll-spec))
        ^ByteBuffer bb (ByteBuffer/wrap ba-coll)
        coll (transient [])
        elements (.getShort bb)] ;; skip length
    (loop [coll coll]
      (if (= (.remaining bb) 0)
        (persistent! coll)
        (let [ba (byte-array (.getShort bb))]
          (.get bb ba)
          (recur (conj! coll (codecs/bytes->clojure coll-type ba))))))))

(defmethod codecs/bytes->clojure :list [coll-spec bytes]
  (c*collection (coll->clojure bytes coll-spec)))

(defmethod codecs/bytes->clojure :set [coll-spec bytes]
  (->> (coll->clojure bytes coll-spec)
       (into #{})
       c*collection))

(defmethod codecs/bytes->clojure :map [collection-spec b]
  (let [bb (ByteBuffer/wrap b)
        elements (.getShort bb)
        [key-type val-type] (-> collection-spec first val)
        m (transient {})]
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

(comment

;; (prn (codecs/meta-encode (c*list  ["a" "b"])))
;; (prn (codecs/meta-encode (c*set  #{"a" "b"})))
;; (prn (codecs/meta-encode (c*map  {"a" "b"
;;                                   "c" "d"})))
;; (prn (codecs/bytes->clojure {:map2 [:utf-8 :utf-8]}
;;                             (codecs/meta-encode (c*map  {"a" "b"
;;                                                          "c" "d"}))))

;; (prn (codecs/bytes->clojure {:list3 :utf-8}
;;                             (codecs/meta-encode (c*list ["a" "b"]))))
;; (prn (codecs/bytes->clojure :utf-8 (second (list->bytes-values2 (codecs/meta-encode (c*list  ["a" "b"]))))))
;; (prn (codecs/meta-encode (c*set  ["a" "b"])))
;; (prn (set->bytes-values (codecs/meta-encode (c*set  ["a" "b"]))))
;; (prn (codecs/bytes->clojure :utf-8 (first (list->bytes-values2 (codecs/meta-encode (c*set  ["a" "b"]))))))
;; (prn (codecs/bytes->clojure :utf-8 (first (list->bytes-values2 (codecs/meta-encode (c*set  ["a" "b"]))))))

)