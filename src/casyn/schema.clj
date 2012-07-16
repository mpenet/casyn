(ns casyn.schema
  (:require [casyn.codecs :as codecs])
  (:import [casyn.types
            Column
            CounterColumn
            SuperColumn
            CounterSuperColumn]))

(defn cols->map
  "Turns a collection of columns into an array-map with column name mapped to key"
  [cols]
  (apply array-map (mapcat (juxt :name :value) cols)))

;; probably overkill, but we could imagine having different schema later
(defrecord Schema [name row super columns])
(defmacro defschema
  "Create a var holding a Schema instance to be used as an argument to
  decode-result. TODO: add schema sync such as ks/sc/c definitions"
  [name & {:keys [row super columns]}]
  `(def ~name (Schema. ~(keyword name) ~row ~super ~columns)))

(defprotocol SchemaDecodable
  (decode-result [result schema] [result schema as-map]
    "Decodes a result according to supplied schema"))

(extend-protocol SchemaDecodable

  clojure.lang.Sequential
  (decode-result
    ([r s]
       (map #(decode-result % s) r))
    ([r s m]
       (if m
         (cols->map (decode-result r s))
         (decode-result r s))))

  clojure.lang.IPersistentVector
  (decode-result
    ([r s]
       (map #(decode-result % s) r))
    ([r s m]
       (map #(decode-result % s m) r)))

  clojure.lang.IPersistentMap
  (decode-result
    ([r s]
       (reduce-kv
        (fn [m k v]
          (assoc m
            (codecs/bytes->clojure (:row s) k)
            (decode-result v s)))
        (array-map)
        r))
    ([r s m]
       (reduce-kv (fn [m k v]
                    (assoc m k (cols->map v)))
                   (array-map)
                   (decode-result r s))))

  Column
  (decode-result [r s]
    (let [[name-type value-type] (-> s :columns :default)
          col-name (codecs/bytes->clojure name-type (:name r))
          col-value-bytes (:value r)]
      (assoc r
        :name col-name
        :value  (when-not (empty? col-value-bytes)
                  (codecs/bytes->clojure
                   (get-in s [:columns :exceptions col-name] value-type)
                   col-value-bytes)))))

  CounterColumn
  (decode-result [r s]
    (update-in r
               [:name]
               (partial codecs/bytes->clojure (-> s :columns :default first))))

  CounterSuperColumn
  (decode-result [r s]
    (assoc r
      :row (codecs/bytes->clojure (:name s) (:name r))
      :columns (decode-result (:columns r) s)))

  SuperColumn
  (decode-result [r s]
    (assoc r
      :row (codecs/bytes->clojure (:name s) (:name r))
      :columns (decode-result (:columns r) s)))


  nil
  (decode-result
    ([r s] nil)
    ([r s m] nil))

  Object
  (decode-result
    ([r s] r)
    ([r s m] r)))