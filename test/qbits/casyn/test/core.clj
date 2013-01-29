(ns qbits.casyn.test.core
  (:use
   qbits.casyn
   clojure.test)
  (:require
   [qbits.casyn.utils :as utils]
   [qbits.casyn.codecs.joda-time]
   [lamina.core :as lc]
   [qbits.tardis :as uuid]
   [clj-time.core :as ctc]))


(def c nil)
(def ks "casyn_test_ks")
(def cf "test_cf")
(def ccf "counter_cf")
(def cocf "composite_cf")

(defn print-line []
  (print "\n--------------------------------------------------------------------------------\n"))

(defschema test-schema
  :row :utf-8
  :super :utf-8
  :columns
  {:default [:utf-8 :utf-8]
   :exceptions {"age" :long
                "c0" :long
                "n2-nil" :long}})

(defschema test-codec-schema
  :row :utf-8
  :super :utf-8
  :columns
  {:default [:keyword :utf-8]
   :exceptions {:long :long
                :int :int
                :double :double
                :float :float
                :str :utf-8
                :str2 :ascii
                :date :date
                :dt :date-time
                :kw :keyword
                :boo :boolean
                :clj :clj
                :clj2 :clj
                :clj3 :clj
                :uuid :uuid
                :tuuid :time-uuid
                :comp [:utf-8 :long :double]}})

(def test-coerce-data
  {:long 1
   :int (int 1)
   :float (float 0.2)
   :double 0.3
   :date (java.util.Date.)
   :dt (ctc/now)
   :str "meh"
   ;; :str2 (.getBytes "meh" "US-ASCII")
   :kw :keyword
   :boo true
   :clj #qbits.casyn/clj{:foo "bar"}
   :clj2 #qbits.casyn/clj[1 2 3]
   :clj3 #{:a :b {:c {:d "e"}}} ;; #clj implicit
   :nil-value nil
   :comp #qbits.casyn/composite["dwa" (long 216) (double 3.14)]
   :uuid (java.util.UUID/randomUUID)
   :tuuid (uuid/unique-time-uuid)})

(defschema composite-cf-schema
  :row :long
  :columns {:default [[:long :long :long] :utf-8]})

(defn setup-test []
  @(c insert-column cf "0" "n0" "value0")
  @(c insert-column cf "0" "n00" "value00")
  @(c insert-column cf "1" "n1" "value1")
  @(c insert-column cf "1" "n2-nil" nil)
  @(c put cf "2" test-coerce-data)
  @(c increment ccf "5" "c0" 2)

  @(c put cocf 0
      {(composite [2 3 4]) "0"
       (composite [2 6 7]) "1"
       (composite [2 9 10]) "2"
       (composite [3 11 10]) "3"
       (composite [3 12 10]) "4"
       (composite [3 13 10]) "5"
       (composite [4 12 10]) "6"
       #qbits.casyn/composite[4 13 10] "7"}))

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
                      :comparator-type {:composite [:long :long :long]}
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
     ;; (println "start tests")
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
  (is (= qbits.casyn.types.Column
         @(lc/run-pipeline
           (c insert-column cf "4" "col-name" "col-value")
           (fn [_] (c get-column cf "4" "col-name"))
           type))))

(deftest test-get-slice
  (is (= 2 (count @(c get-slice cf "0" :columns ["n0" "n00"]))))
  (is (= 2 (count @(c get-slice cf "0" :start "n0" :finish "n00"))))
  (is (= 2 (count @(c get-row cf "0" :schema test-schema))))
  (is (= nil @(c get-slice cf "1000" :columns ["n0" "n00"]))))

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
  (is (nil? @(c delete ccf "5" :column "c0" :type :counter))))

(deftest test-mutation
  (is (nil?
       @(c batch-mutate
           [[cf "0" [(mutation "n0" "un0")
                     (mutation "n00" "un00")]]
            [cf "1" [(mutation "n1" "n10")]]])))

  (is (nil?
       @(c batch-mutate
           [[cf "0" [(delete-mutation :columns ["n0"] )
                     (delete-mutation :columns ["n00"])]]
            [cf "1" [(delete-mutation :columns ["n1"])]]])))

  (is (nil?
       @(c put cf "11"
           [["test-dwa1" "dwa1"]
            ["test-dwa2" "dwa2"]])))

  (is (nil?
       @(c put cf "12"
           {:test-dwa1 "dwa1"
            :test-dwa2 "dwa12"}))))

(deftest deletes
  (is (= nil (seq @(c delete cf "0" :column "n0"))))
  (is (= 1 (count @(c get-row cf "0"))))
  (is (= nil (seq @(c delete cf "0"))))
  (is (= 0 (count @(c get-row cf "0")))))

(deftest test-ranges
  (is (= 1 (count @(c get-range-slice cf
                      :start-key "0"
                      :end-key "1"
                      :columns ["n0" "n00"]
                      :row-filter [[:eq? "n0" "value0"]]
                      :schema test-schema)))))

(deftest codecs
  (is (= test-coerce-data
         @(c get-row cf "2" :schema test-codec-schema :as :map)))
  (is (= {"n0" "value0" "n00" "value00"}
         @(c get-row cf "0" :schema test-schema :as :map)))
  (is (= {"0" {"n0" "value0", "n00" "value00"}, "1" {"n1" "value1", "n2-nil" nil}}
         @(c get-rows cf ["0" "1"] :schema test-schema :as :map)))
  (let [test-nils (reduce-kv (fn [m k v] (assoc m k nil)) (array-map) test-coerce-data)]
    @(c put cf "66" test-nils)
    (is (= test-nils @(c get-row cf "66" :schema test-codec-schema
                         :as :map)))))

(deftest test-index
  (is (= '({"1" {"n1" "value1"}})
         @(c get-indexed-slice cf
            [[:eq? :n1 "value1"]]
            :columns ["n1"]
            :schema test-schema
            :as :map)))

  (is (= 1 (count @(c get-indexed-slice
                      cf
                      [[:eq? :n1 "value1"]]
                      :columns ["n1"]
                      :schema test-schema
                      :as :map)))))

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
           :schema test-codec-schema :as :map)
        #(= "value0" (-> % :rows first :n0))))
  (let [prepared-statement @(c prepare-cql-query "SELECT * FROM test_cf WHERE KEY=?;")]
      (is (not-empty prepared-statement))
      (is @(lc/run-pipeline
            (c execute-prepared-cql-query (:item-id prepared-statement) ["0"]
               :schema test-codec-schema :as :map)
            #(= "value0" (-> % :rows first :n0))))))


;; (deftest test-with*
;;   (is @(with-client c
;;          (lc/run-pipeline
;;           (execute-cql-query "SELECT * FROM test_cf;"
;;                              :schema test-codec-schema :as :map)
;;           #(= "value0" (-> % :rows first :n0)))))
;;     (is @(with-client2 c
;;          (lc/run-pipeline
;;           (execute-cql-query "SELECT * FROM test_cf;"
;;                              :schema test-codec-schema :as :map)
;;           #(= "value0" (-> % :rows first :n0))))))


(deftest test-composites-expressions
  (is  (= {[2 9 10] "2", [3 11 10] "3" [3 12 10] "4", [3 13 10] "5"}
          @(c get-slice cocf 0
              :start (composite-expression [:eq? 2] [:gt? 6])
              :finish (composite-expression [:lt? 4])
              :schema composite-cf-schema :as :map))))

(deftest type-converter
  (is (= "UTF8Type" (clj->cassandra-type :utf-8)))
  (is (= "UTF8Type" (clj->cassandra-type "UTF8Type")))
  (is (= "UTF8Type,LongType" (clj->cassandra-type [:utf-8 :long])))
  (is (= "CompositeType(UTF8Type,LongType)"
         (clj->cassandra-type {:composite [:utf-8 :long]}))))

(deftest test-shutdown
  (let [cl (make-cluster "127.0.0.1" 9160 ks) ]
    (is (= nil (shutdown cl)))))