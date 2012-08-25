(ns casyn.test.core
  (:use
   casyn.core
   clojure.test)
  (:require
   [casyn.utils :as utils]
   [lamina.core :as lc]))


(def c nil)
(def ks "casyn_test_ks")
(def cf "test_cf")
(def ccf "counter_cf")
(def cocf "composite_cf")

(defn print-line []
  (print "\n--------------------------------------------------------------------------------\n"))

(defschema test-schema
  :row :string
  :super :string
  :columns
  {:default [:string :string]
   :exceptions {"age" :long
                "c0" :long
                "n2-nil" :long}})

(defschema test-codec-schema
  :row :string
  :super :string
  :columns
  {:default [:keyword :string]
   :exceptions {:long :long
                :int :int
                :double :double
                :float :float
                :str :string
                :date :date
                :kw :keyword
                :boo :boolean
                :clj :clj
                :clj2 :clj
                :uuid :uuid
                :comp [:string :long :double]}})

(def test-coerce-data
  {:long 1
   :int (int 1)
   :float (float 0.2)
   :double 0.3
   :date (java.util.Date.)
   :str "meh"
   :kw :keyword
   :boo true
   :clj #clj{:foo "bar"}
   :clj2 #clj[1 2 3]
   :comp #composite["dwa" (long 216) (double 3.14)]
   :uuid (java.util.UUID/randomUUID)})

(defschema composite-cf-schema
  :row [:long :long :long]
  :columns {:default [[:long :long :long] :string]})


(defn setup-test []
  @(c insert-column cf "0" "n0" "value0")
  @(c insert-column cf "0" "n00" "value00")
  @(c insert-column cf "1" "n1" "value1")
  @(c insert-column cf "1" "n2-nil" nil)
  @(c put cf "2" test-coerce-data)
  @(c increment ccf "5" "c0" 2)

  @(c put cocf 0
      {#composite[2 3 4] "0"
       #composite[5 6 7] "1"
       #composite[6 8 9] "2"})

  ;; @(c insert-column cocf
  ;;     0
  ;;     #composite[3 4 5]
  ;;     "1")

  ;; @(c insert-column cocf
  ;;     0
  ;;     #composite[4 5 6]
  ;;     "2")
  )

(defn teardown-test []
  @(c truncate cf))

(use-fixtures
 :once
 (fn [test-runner]

   (try
     (println "trying to drop" ks)
     @(drop-keyspace (make-client "127.0.0.1" 9160) ks)
     (println "droped" ks)
     (catch Exception e
       (print-line)
       (println "You can ignore this")))
   (try
     @(add-keyspace (make-client "127.0.0.1" 9160)
                    ks
                    "SimpleStrategy"
                    [[cf
                      :column-metadata [[:n0 :utf-8]
                                        [:n1 :utf-8 :n1_index :utf-8]]]
                     [ccf
                      :default-validation-class :counter
                      :replicate-on-write true]
                     [cocf
                      :default-validation-class :utf-8
                      :key-validation-class :long
                      :comparator-type "CompositeType(LongType, LongType, LongType)"
                      :replicate-on-write true]]
                    :strategy-options {"replication_factor" "1"})
     (println  "Keyspace created, waiting for the change to propagate to other nodes")
     (print-line)

     (let [cl (make-cluster "127.0.0.1" 9160 ks)
           cx (client-fn cl)]
       (alter-var-root #'c
                       (constantly cx)
                       (when (thread-bound? #'c)
                         (set! c cx))))
     (test-runner)
     (catch Exception e
       (.printStackTrace e))
     (finally
       @(drop-keyspace (make-client) ks)))))

(use-fixtures
 :each
 (fn [test-runner]
     (setup-test)
     (test-runner)

     (teardown-test)))

(deftest test-set-keyspace
  (is (nil? @(c set-keyspace ks))))

(deftest test-insert-and-read

  (is (= casyn.types.Column
         @(lc/run-pipeline
           (c insert-column cf "4" "col-name" "col-value")
           (fn [_] (c get-column cf "4" "col-name"))
           type))))

(deftest test-get-slice
  (is (= 2 (count @(c get-slice cf "0" :columns ["n0" "n00"]))))
  (is (= 2 (count @(c get-slice cf "0" :start "n0" :finish "n00"))))
  (is (= 2 (count @(c get-row cf "0" :schema test-schema)))))

(deftest test-mget-slice
  (is (= 2 (count @(c mget-slice cf ["0" "1"]
                      :columns ["n0" "n1" "n00"]))))
  (is (= 2 (count @(c mget-slice cf ["0" "1"] :schema test-schema))))
  (is (= 2 (count @(c get-rows cf ["0" "1"] :schema test-schema)))))

(deftest test-get-count
  (is (= 2 @(c get-count cf "0" :columns ["n0" "n00"] :schema test-schema)))
  (is (= 2 @(c get-count cf "0" :start "n0" :finish "n00" :schema test-schema))))

(deftest test-mget-count
  (is (= {"1" 1 "0" 2}
         @(c mget-count cf ["0" "1"] :columns ["n0" "n1" "n00"] :schema test-schema)))
  (is (= {"1" 2 "0" 2}
         @(c mget-count cf ["0" "1"] :start "n0" :finish "zzzzz" :schema test-schema))))

(deftest counters
  (is (nil? @(c increment ccf "5" "c0" 10)))
  (is (= 12 (:value @(c get-column ccf "5" "c0"))))
  ;; keys must be decodable
  (is (= "c0" (:name @(c get-column ccf "5" "c0" :schema test-schema))))
  (is (nil? @(c remove-counter ccf "5" "c0"))))

(deftest test-mutation
  (is (nil?
       @(c batch-mutate
         {"0" {cf
               [(column-mutation "n0" "un0")
                (column-mutation "n00" "un00")]}
          "1" {cf
               [(column-mutation "n1" "n10")]}})))

  (is (nil?
       @(c batch-mutate
         {"0" {cf
               [(delete-mutation :columns ["n0"] )
                (delete-mutation :columns ["n00"])]}
          "1" {cf
               [(delete-mutation :columns ["n1"])]}})))

  (is (nil?
       @(c put cf "11"
           [["test-dwa1" "dwa1"]
            ["test-dwa2" "dwa2"]])))

  (is (nil?
       @(c put cf "12"
           {:test-dwa1 "dwa1"
            :test-dwa2 "dwa12"}))))

(deftest deletes
  (is (= nil (seq @(c remove-column cf "0" "n0")))))

(deftest test-ranges
  (is (= 1 (count @(c get-range-slice cf
                      :start-key "0"
                      :end-key "1"
                      :columns ["n0" "n00"]
                      :row-filter [[:eq? "n0" "value0"]]
                      :schema test-schema))))

  (is (= 1 (count @(c get-paged-slice cf
                      :start-key "0"
                      :end-key "0"
                      :start-column "n0"
                      :schema test-schema)))))

(deftest codecs
  (is (= test-coerce-data
         @(c get-row cf "2" :schema test-codec-schema :output :as-map)))
  (is (= {"n0" "value0" "n00" "value00"}
         @(c get-row cf "0" :schema test-schema :output :as-map)))
  (is (= {"0" {"n0" "value0", "n00" "value00"}, "1" {"n1" "value1", "n2-nil" nil}}
         @(c get-rows cf ["0" "1"] :schema test-schema :output :as-map))))

(deftest test-index
  (is (= '({"1" {"n1" "value1"}})
         @(c get-indexed-slice cf
            [[:eq? :n1 "value1"]]
            :columns ["n1"]
            :schema test-schema
            :output :as-map)))

  (is (= 1 (count @(c get-indexed-slice
                      cf
                      [[:eq? :n1 "value1"]]
                      :columns ["n1"]
                      :schema test-schema
                      :output :as-map)))))

(deftest error-handlers
  (is (= nil @(lc/run-pipeline
               (c get-row cf "mehhhhh" :super "meh" :schema test-schema)
               {:error-handler
                (fn [e]
                  (if-not (instance? org.apache.cassandra.thrift.InvalidRequestException e)
                    (throw (Exception. "meh"))
                    (lc/complete nil))
                  )}
              count)))
  ;; not found returns nil
  (is (= nil @(lc/run-pipeline
               (c get-column cf "1" "meh")
               :foo
               :bar))))

(deftest test-cql
  (is @(lc/run-pipeline
        (c execute-cql-query "SELECT * FROM test_cf;"
           :schema test-codec-schema :output :as-map)
        #(= "value0" (-> % :rows first :n0))))
  (let [prepared-statement @(c prepare-cql-query "SELECT * FROM test_cf WHERE KEY=?;")]
      (is (not-empty prepared-statement))
      (is @(lc/run-pipeline
            (c execute-prepared-cql-query (:item-id prepared-statement) ["0"]
               :schema test-codec-schema :output :as-map)
            #(= "value0" (-> % :rows first :n0))))))


(deftest test-with*
  (is @(with-client c
         (lc/run-pipeline
          (execute-cql-query "SELECT * FROM test_cf;"
                             :schema test-codec-schema :output :as-map)
          #(= "value0" (-> % :rows first :n0)))))
    (is @(with-client2 c
         (lc/run-pipeline
          (execute-cql-query "SELECT * FROM test_cf;"
                             :schema test-codec-schema :output :as-map)
          #(= "value0" (-> % :rows first :n0))))))


(deftest test-composites

  ;; (println @(c get-row cocf 0))

  ;; (println @(c get-range-slice cocf
  ;;              :start-key 0
  ;;              :end-key 0
  ;;              :columns [(composite-expression [:eq? 2] [:eq? 3] [:eq? 4])]
  ;;              ;; :row-filter [[:eq? "n0" "value0"]]
  ;;              ;; :columns [(composite-expression [:eq? 2])]
  ;;              ))

  ;; (is (= [2 3 4]  (:name (get @(c get-row cocf 0
  ;;                                 :schema composite-cf-schema
  ;;                                 :output :as-map) 0))))

  ;; (println )

  )