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
                :symbol :symbol
                :date :date
                :kw :keyword
                :boo :boolean
                :clj :clojure
                :clj2 :clojure
                :uuid :uuid
                :comp [:string :long :double]
                :crazy-nested-comp [:string :long [:string [:string :long :double] :double]]
                }})

(def test-coerce-data
  {:long 1
   :int (int 1)
   :float (float 0.2)
   :double 0.3
   :date (java.util.Date.)
   :str "meh"
   :symbol 'sym
   :kw :keyword
   :boo true
   :clj {:foo "bar"}
   :clj2 [1 2 3]
   :comp (composite "dwa" (long 216) (double 3.14))
   :uuid (java.util.UUID/randomUUID)
   :crazy-nested-comp (composite "dwa1"
                                 (long 216)
                                 (composite "dwa2"
                                            (composite "dwa3"
                                                       (long 217)
                                                       (double 3.141))
                                            (double 3.1415)))
   })


(defn setup-test []
  @(c insert-column cf "0" "n0" "value0")
  @(c insert-column cf "0" "n00" "value00")
  @(c insert-column cf "1" "n1" "value1")
  @(c insert-column cf "1" "n2-nil" nil)
  @(c put cf "2" test-coerce-data)
  @(c increment ccf "5" "c0" 2))

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

  (is (= 2
         @(lc/run-pipeline
           (c get-slice cf "0" :columns ["n0" "n00"])
           count)))

  (is (= 2
         @(lc/run-pipeline
           (c get-slice cf "0" :start "n0" :finish "n00")
           count)))

  (is (= 2
         @(lc/run-pipeline
           (c get-row cf "0")
           #(decode-result % test-schema)
           count)
         )))

(deftest test-mget-slice

  (is (= 2
         @(lc/run-pipeline
           (c mget-slice cf ["0" "1"] :columns ["n0" "n1" "n00"])
           count)))

  (is (= 2
         @(lc/run-pipeline
           (c mget-slice cf ["0" "1"])
           #(decode-result % test-schema)
           count)))

    (is (= 2
         @(lc/run-pipeline
           (c get-rows cf ["0" "1"])
           #(decode-result % test-schema)
           count))))

(deftest test-get-count
  (is (= 2
         @(lc/run-pipeline
           (c get-count cf "0" :columns ["n0" "n00"])
           #(decode-result % test-schema))))

  (is (= 2
         @(lc/run-pipeline
           (c get-count cf "0" :start "n0" :finish "n00")
           #(decode-result % test-schema)))))

(deftest test-mget-count
  (is (= {"1" 1 "0" 2}
         @(lc/run-pipeline
           (c mget-count cf ["0" "1"] :columns ["n0" "n1" "n00"])
           #(decode-result % test-schema))))

  (is (= {"1" 2 "0" 2}
         @(lc/run-pipeline
           (c mget-count
              cf
              ["0" "1"]
              :start "n0" :finish "zzzzz")
           #(decode-result % test-schema)))))

(deftest counters
  (is (nil? @(c increment ccf "5" "c0" 10)))

  (is (= 12 @(lc/run-pipeline
              (c get-column ccf "5" "c0")
              #(:value %))))

  ;; keys must be decodable
  (is (= "c0" @(lc/run-pipeline
                (c get-column ccf "5" "c0")
                #(decode-result % test-schema)
                #(:name %))))

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
  (is (= 3
         @(lc/run-pipeline
           (c get-range-slice cf
              :start-key "0"
              :end-key "1"
              :columns ["n0" "n00"])
           #(decode-result % test-schema)
           count)))

  (is (= 1
         @(lc/run-pipeline
           (c get-paged-slice cf
              :start-key "0"
              :end-key "0"
              :start-column "n0")
           #(decode-result % test-schema)
           count))))

(deftest codecs
  (is (= test-coerce-data
         @(lc/run-pipeline
          (c get-row cf "2")
          #(decode-result % test-codec-schema)
          cols->map)))

  (is (= {"n0" "value0"
          "n00" "value00"}
         @(lc/run-pipeline
           (c get-row cf "0")
           #(decode-result % test-schema true))))


(is (= {"0" {"n0" "value0", "n00" "value00"}, "1" {"n1" "value1", "n2-nil" nil}}
         @(lc/run-pipeline
           (c get-rows cf ["0" "1"])
           #(decode-result % test-schema true)))))

(deftest test-index
  (is (= '({"1" {"n1" "value1"}})
       @(lc/run-pipeline
           (c get-indexed-slice
              cf
              [[:eq? :n1 "value1"]]
              :columns ["n1"])
           #(decode-result % test-schema true))))

  (is (= 1
         @(lc/run-pipeline
           (c get-indexed-slice
              cf
              [[:eq? :n1 "value1"]]
              :columns ["n1"])
           #(decode-result % test-schema)
           count))))

(deftest error-handlers
  (is (= nil @(lc/run-pipeline
               (c get-row cf "mehhhhh" :super "meh")
               {:error-handler
                (fn [e]
                  (if-not (instance? org.apache.cassandra.thrift.InvalidRequestException e)
                    (throw (Exception. "meh"))
                    (lc/complete nil))
                  )}
              #(decode-result % test-schema)
              count)))
  ;; not found returns nil
  (is (= nil @(lc/run-pipeline
               (c get-column cf "1" "meh")
               :foo
               :bar))))

(deftest test-cql
  (is @(lc/run-pipeline
        (c execute-cql-query "SELECT * FROM test_cf;")
        #(decode-result % test-codec-schema true)
        #(= "value0" (-> % ffirst val :n0))))
  (let [prepared-statement @(c prepare-cql-query "SELECT * FROM test_cf WHERE KEY=?;")]
      (is (not-empty prepared-statement))
      (is @(lc/run-pipeline
            (c execute-prepared-cql-query (:item-id prepared-statement) ["0"])
            #(decode-result % test-codec-schema true)
            #(= "value0" (-> % ffirst val :n0))))))