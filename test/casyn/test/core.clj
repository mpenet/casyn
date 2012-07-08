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
                :kw :keyword
                :boo :boolean
                :clj :clojure
                :comp [:string :long :double]}})

(def test-coerce-data-in
  {:long 1
   :int (int 1)
   :float (float 0.2)
   :double 0.3
   :str "meh"
   :symbol 'sym
   :kw :keyword
   :boo true
   :clj {:foo "bar"}
   :comp (composite "dwa" (long 216) (double 3.14))
   })

(def test-coerce-data-out
  (assoc test-coerce-data-in
    :comp ["dwa" (long 216) (double 3.14)]))



(defn setup-test []
  @(c insert-column "0" cf (column "n0" "value0"))
  @(c insert-column "0" cf (column "n00" "value00"))
  @(c insert-column "1" cf (column "n1" "value1"))
  @(c insert-column "1" cf (column "n2-nil" nil))
  @(c put "2" cf test-coerce-data-in)
  @(c add "5" ccf "c0" 2))

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
           (c insert-column "4" cf
                     (column "col-name" "col-value"))
           (fn [_] (c get-column "4" [cf "col-name"]))
           type))))

(deftest test-get-slice
  (is (= 2
         @(lc/run-pipeline
           (c get-slice "0" cf
                     (columns-by-names "n0" "n00"))
           count)))

  (is (= 2
         @(lc/run-pipeline
           (c get-slice "0" [cf]
                     (columns-by-range :start "n0" :finish "n00"))
           count)))

  (is (= 2
         @(lc/run-pipeline
           (c get-row "0" cf)
           #(decode-result % test-schema)
           count)
         )))

(deftest test-mget-slice
  (is (= 2
         @(lc/run-pipeline
           (c mget-slice ["0" "1"] cf
                     (columns-by-names "n0" "n1" "n00"))
           count)))

  (is (= 2
         @(lc/run-pipeline
           (c mget-slice ["0" "1"] cf
                     (columns-by-range))
           #(decode-result % test-schema)
           count)))

    (is (= 2
         @(lc/run-pipeline
           (c get-rows ["0" "1"] cf)
           #(decode-result % test-schema)
           count))))

(deftest test-get-count
  (is (= 2
         @(lc/run-pipeline
           (c get-count "0" [cf]
                     (columns-by-names "n0" "n00"))
           #(decode-result % test-schema))))

  (is (= 2
         @(lc/run-pipeline
           (c get-count "0" cf
                     (columns-by-range :start "n0" :finish "n00"))
           #(decode-result % test-schema)))))

(deftest test-mget-count
  (is (= {"1" 1 "0" 2}
         @(lc/run-pipeline
           (c mget-count ["0" "1"] cf
                     (columns-by-names "n0" "n1" "n00"))
           #(decode-result % test-schema))))

  (is (= {"1" 2 "0" 2}
         @(lc/run-pipeline
           (c mget-count
                     ["0" "1"]
                     cf
                     (columns-by-range :start "n0" :finish "zzzzz"))
           #(decode-result % test-schema)))))

(deftest counters
  (is (nil? @(c add "5" ccf "c0" 10)))

  (is (= 12 @(lc/run-pipeline
              (c get-column "5" [ccf "c0"])
              #(:value %))))

  ;; keys must be decodable
  (is (= "c0" @(lc/run-pipeline
              (c get-column "5" [ccf "c0"])
              #(decode-result % test-schema)
              #(:name %))))

  (is (nil? @(c remove-counter "5" [ccf "c0"]))))

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
               [(delete-mutation (columns-by-names "n0" ))
                (delete-mutation (columns-by-names "n00"))]}
          "1" {cf
               [(delete-mutation (columns-by-names "n1"))]}})))

  (is (nil?
       @(c put "11" cf
                  [["test-dwa1" "dwa1"]
                   ["test-dwa2" "dwa2"]])))

  (is (nil?
       @(c put "12" cf
                  {:test-dwa1 "dwa1"
                   :test-dwa2 "dwa12"}))))

(deftest deletes
  (is (= nil (seq @(c remove-column "0" cf)))))

(deftest test-ranges
  (is (= 3
         @(lc/run-pipeline
           (c get-range-slice cf
                     (columns-by-names "n0" "n00")
                     [:start-key "0" :end-key "1"])
           #(decode-result % test-schema)
           count))))


(deftest codecs
  ;; FIXME : -in and -out should be identical
  ;; probably wrapping composite in a type that looks like a list would do.
  ;; or using meta
  (is (= test-coerce-data-out
         @(lc/run-pipeline
          (c get-row "2" cf)
          #(decode-result % test-codec-schema)
          cols->map))))

(deftest test-index
  (is (= 1
         @(lc/run-pipeline
           (c get-indexed-slice
                     cf
                     [[:eq? :n1 "value1"]]
                     (columns-by-names "n1"))
           #(decode-result % test-schema)
           count))))


(deftest error-handlers
  (is (= nil @(lc/run-pipeline
               (c get-row "1" [cf "meh"])
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
               (c get-column "1" [cf "meh"])
               :foo
               :bar))))

(deftest test-cql
  (is true))