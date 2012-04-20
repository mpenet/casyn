(ns casyn.schema
  (:require [casyn.codecs :as codecs])
  (:import [casyn.types
            Column
            CounterColumn
            SuperColumn
            CounterSuperColumn
            KeySlice]))

;; probably overkill, but we could imagine having different schema later
(defrecord Schema [name row super columns])
(defmacro defschema
  "Create a var holding a Schema instance to be used as an argument to
  decode-result. TODO: add schema sync such as ks/sc/c definitions"
  [name & {:keys [row super columns]}]
  `(def ~name (Schema. ~(keyword name) ~row ~super ~columns)))

(defprotocol SchemaDecodable
  (decode-result [r s] "Decodes a result according to supplied schema"))

(extend-protocol SchemaDecodable

  clojure.lang.Sequential
  (decode-result [r s]
    (map #(decode-result % s) r))

  clojure.lang.IPersistentMap
  (decode-result [r s]
    (reduce-kv
     (fn [m k v]
       (assoc m
         (codecs/bytes->clojure (:row s) k)
         (decode-result v s)))
     (array-map)
     r))

  Column
  (decode-result [r s]
    (let [[name-type value-type] (-> s :columns :default)
          col-name (codecs/bytes->clojure name-type (:name r))]
      (assoc r
        :name col-name
        :value (codecs/bytes->clojure
                (get-in s [:columns :exceptions col-name] value-type)
                (:value r)))))

  CounterColumn
  (decode-result [r s]
    (update-in r
               [:name]
               (partial (codecs/bytes->clojure (-> s :columns :default first)))))

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


  KeySlice
  (decode-result [r s]
    (assoc r
      :row (codecs/bytes->clojure (:row s) (:row r))
      :columns (decode-result (:columns r) s)))


  nil
  Object
  (decode-result [r s]
    r))