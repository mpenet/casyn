(ns casyn.test.core
  (:require [casyn
             [core :as core]
             [client :as client]
             [schema :as schema]
             [ddl :as ddl]
             [codecs :as codecs]
             [utils :as utils]]
            ;; [casyn.pool.fifo :as fifo-pool]
            [casyn.cluster.core :as cluster]
            [lamina.core :as lc])
  (:use [clojure.test]))


(def client-x nil)

(def ks "casyn_test_ks")
(def cf "test_cf")
(def ccf "counter_cf")

(defn print-line []
  (print "\n--------------------------------------------------------------------------------\n"))

(schema/defschema test-schema
  :row :string
  :super :string
  :columns
  {:default [:string :string]
   :exceptions {"age" :long}})

(defn setup-test []
  @(client-x core/insert-column "0" cf (core/column "n0" "value0"))
  @(client-x core/insert-column "0" cf (core/column "n00" "value00"))
  @(client-x core/insert-column "1" cf (core/column "n1" "value1"))
  @(client-x core/add "5" ccf "c0" 2))

(defn teardown-test []
  @(client-x core/truncate cf))

(use-fixtures
 :once
 (fn [test-runner]
   (try
     (try
       (println "trying to drop" ks)

       @(ddl/drop-keyspace (client/make-client "127.0.0.1" 9160) ks)
       (println "droped" ks)
       (catch Exception e
         (print-line)
         (println "You can ignore this")))

     @(ddl/add-keyspace (client/make-client "127.0.0.1" 9160)
                        ks
                        "SimpleStrategy"
                        [[cf]
                         [ccf
                          :default-validation-class :counter
                          :replicate-on-write true]]
                        :strategy-options {"replication_factor" "1"})


     (println  "Keyspace created, waiting for the change to propagate to other nodes")
     (print-line)

     (let [cl (cluster/make-cluster "127.0.0.1" 9160 ks)
           cx (client/client-executor cl)]
       ;; (Thread/sleep 000)
       (alter-var-root #'client-x
                       (constantly cx)
                       (when (thread-bound? #'client-x)
                         (set! client-x cx))))
     (test-runner)
     (catch Exception e
       (.printStackTrace e))
     (finally
       @(ddl/drop-keyspace (client/make-client) ks)))))

(use-fixtures
 :each
 (fn [test-runner]
     (setup-test)
     (test-runner)
     (teardown-test)))

(deftest test-set-keyspace
  (is (nil? @(client-x core/set-keyspace ks))))

(deftest test-insert-and-read
  (is (= casyn.types.Column
         @(lc/run-pipeline
           (client-x core/insert-column "4" cf
                     (core/column "col-name" "col-value"))
           (fn [_] (client-x core/get-column "4" [cf "col-name"]))
           type))))

(deftest test-get-slice
  (is (= 2
         @(lc/run-pipeline
           (client-x core/get-slice "0" cf
                     (core/columns-by-names "n0" "n00"))
           count)))

  (is (= 2
         @(lc/run-pipeline
           (client-x core/get-slice "0" [cf]
                     (core/columns-by-range :start "n0" :finish "n00"))
           count)))

  (is (= 2
         @(lc/run-pipeline
           (client-x core/get-row "0" cf)
           #(schema/decode-result % test-schema)
           count)
         )))

(deftest test-mget-slice
  (is (= 2
         @(lc/run-pipeline
           (client-x core/mget-slice ["0" "1"] cf
                     (core/columns-by-names "n0" "n1" "n00"))
           count)))

  (is (= 2
         @(lc/run-pipeline
           (client-x core/mget-slice ["0" "1"] cf
                     (core/columns-by-range))
           #(schema/decode-result % test-schema)
           count)))

    (is (= 2
         @(lc/run-pipeline
           (client-x core/get-rows ["0" "1"] cf)
           #(schema/decode-result % test-schema)
           count))))

(deftest test-get-count
  (is (= 2
         @(lc/run-pipeline
           (client-x core/get-count "0" [cf]
                     (core/columns-by-names "n0" "n00"))
           #(schema/decode-result % test-schema))))

  (is (= 2
         @(lc/run-pipeline
           (client-x core/get-count "0" cf
                     (core/columns-by-range :start "n0" :finish "n00"))
           #(schema/decode-result % test-schema)))))

(deftest test-mget-count
  (is (= {"1" 1 "0" 2}
         @(lc/run-pipeline
           (client-x core/mget-count ["0" "1"] cf
                     (core/columns-by-names "n0" "n1" "n00"))
           #(schema/decode-result % test-schema))))

  (is (= {"1" 1 "0" 2}
         @(lc/run-pipeline
           (client-x core/mget-count
                     ["0" "1"]
                     cf
                     (core/columns-by-range :start "n0" :finish "zzzzz"))
           #(schema/decode-result % test-schema)))))

(deftest counters
  (is (nil? @(client-x core/add "5" ccf "c0" 10)))

  (is (= 12 @(lc/run-pipeline
              (client-x core/get-column "5" [ccf "c0"])
              #(:value %))))

  (is (nil? @(client-x core/remove-counter "5" [ccf "c0"]))))

(deftest test-mutation
  (is (nil?
       @(client-x core/batch-mutate
         {"0" {cf
               [(core/column-mutation "n0" "un0")
                (core/column-mutation "n00" "un00")]}
          "1" {cf
               [(core/column-mutation "n1" "n10")]}})))

  (is (nil?
       @(client-x core/batch-mutate
         {"0" {cf
               [(core/delete-mutation (core/columns-by-names "n0" ))
                (core/delete-mutation (core/columns-by-names "n00"))]}
          "1" {cf
               [(core/delete-mutation (core/columns-by-names "n1"))]}})))

  (is (nil?
       @(client-x core/put "11" cf
                  [["test-dwa1" "dwa1"]
                   ["test-dwa2" "dwa2"]])))

  (is (nil?
       @(client-x core/put "12" cf
                  {:test-dwa1 "dwa1"
                   :test-dwa2 "dwa12"}))))

(deftest deletes
  (is (= nil (seq @(client-x core/remove-column "0" cf)))))

(deftest test-ranges
  (is (= 2
         @(lc/run-pipeline
           (client-x core/get-range-slice cf
                     (core/columns-by-names "n0" "n00")
                     [:start-key "0" :end-key "1"])
           #(schema/decode-result % test-schema)
           count))))

;; (deftest test-index
;;   (is (= 2
;;          @(lc/run-pipeline
;;            (core/get-index-slice *c*
;;                                  [cf]
;;                                  (core/columns-by-names "n0" "n00")
;;                                  [:start-key "0" :end-key "1"])
;;            #(schema/decode-result % test-schema)
;;            count))))


(deftest test-cql
  (is true))