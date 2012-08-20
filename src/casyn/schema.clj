(ns casyn.schema
  (:require [casyn.codecs :as codecs])
  (:import [casyn.types
            Column
            CounterColumn
            SuperColumn
            CounterSuperColumn
            KeySlice
            CqlRow
            ]))

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

(extend-type (Class/forName "[Lcasyn.types.KeySlice;")
  SchemaDecodable
  (decode-result
    ([r s m]
       (map #(decode-result % s m) r))
    ([r s]
       (decode-result r s false))))

(extend-type (Class/forName "[Lcasyn.types.CqlRow;")
  SchemaDecodable
  (decode-result
    ([r s m]
       (map #(decode-result % s m) r))
    ([r s]
       (decode-result r s false))))

(extend-protocol SchemaDecodable

  clojure.lang.Sequential
  (decode-result
    ([r s m]
       (if m
         (cols->map (map #(decode-result % s) r))
         (map #(decode-result % s) r)))
    ([r s]
       (decode-result r s false)))

  clojure.lang.IPersistentMap
  (decode-result
    ([r s m]
       (if m
         (reduce-kv (fn [m k v]
                      (assoc m k (cols->map v)))
                    (array-map)
                    (decode-result r s))
         (reduce-kv
          (fn [m k v]
            (assoc m
              (codecs/bytes->clojure (:row s) k)
              (decode-result v s)))
          (array-map)
          r)))
    ([r s]
       (decode-result r s false)))

  KeySlice
  (decode-result
    ([r s m]
       (if m
         {(codecs/bytes->clojure (:row s) (:row r))
          (decode-result (:columns r) s m)}
         (assoc r
           :row (codecs/bytes->clojure (:row s) (:row r))
           :columns (decode-result (:columns r) s m))))
    ([r s]
       (decode-result r s false)))

  CounterSuperColumn
  (decode-result
    ([r s m]
       (assoc r
         :name (codecs/bytes->clojure (:name s) (:name r))
         :columns (decode-result (:columns r) s)))
    ([r s]
       (decode-result r s false)))

  SuperColumn
  (decode-result
    ([r s m]
       (assoc r
         :name (codecs/bytes->clojure (:name s) (:row r))
         :columns (decode-result (:columns r) s)))
    ([r s]
       (decode-result r s false)))

  CqlRow
  (decode-result
    ([r s m]
       (if m
         (decode-result (:columns r) s m)
         (assoc r
           :row (codecs/bytes->clojure (:row s) (:row r))
           :columns (decode-result (:columns r) s m))))
    ([r s]
       (decode-result r s false)))

  Column
  (decode-result
    ([r s m]
       (let [[name-type value-type] (-> s :columns :default)
             col-name (codecs/bytes->clojure name-type (:name r))
             col-value-bytes (:value r)
             col-value (when-not (empty? col-value-bytes)
                         (codecs/bytes->clojure
                          (get-in s [:columns :exceptions col-name] value-type)
                          col-value-bytes))]
         (if m
           {col-name col-value}
           (assoc r
             :name col-name
             :value col-value))))
    ([r s]
       (decode-result r s false)))

  CounterColumn
  (decode-result
    ([r s m]
       (let [col-name (->> r :name (codecs/bytes->clojure (-> s :columns :default first)))]
         (if m
           {col-name (:value r)}
           (assoc r :name col-name))))
    ([r s]
       (decode-result r s false)))

  nil
  (decode-result
    ([r s] nil)
    ([r s m] nil))

  Object
  (decode-result
    ([r s] r)
    ([r s m] r)))