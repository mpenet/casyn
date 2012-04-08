(ns playground
  (:require [casyn
             [core :as core]
             [client :as client]
             [schema :as schema]
             [ddl :as ddl]
             [codecs :as codecs]
             [utils :as utils]]
            [casyn.pool.commons :as p]
            [casyn.cluster :as cp]
            [casyn.cluster.core :as cluster]
            [lamina.core :as lac]))



(def ks "casyn_test_ks")
(def cf "test_cf")
(def ccf "counter_cf")



(try
 @(ddl/add-keyspace (client/make-client)
                    ks
                    "SimpleStrategy"
                    [[cf]
                     [ccf
                      :default-validation-class :counter
                      :replicate-on-write true]]
                    :strategy-options {"replication_factor" "1"})

 (catch Exception e nil)  )



(schema/defschema test-schema
  :row :string
  :super :string
  :columns
  {:default [:string :string]
   :exceptions {"age" :long}})

(def cl (cluster/make-cluster "localhost" 9160 ks))

(def client-x (client/client-executor cl))


;; (prn cl)

;; (prn (.nodes (.balancer cl)))

;; (use 'casyn.auto-discovery)

;; (prn (cp/refresh cl (prn (discover cl))))
;; ;; (connection/close  client-pool)

(prn @(client-x core/insert-column
            "1"
            cf
            (core/column "col-name" "col-value")))



(prn @(client-x core/get-column
            "1"
           [cf "col-name"]))


;; @(lac/run-pipeline
;;  (client-x core/insert-column
;;            "1"
;;            cf
;;            (core/column "col-name" "col-val0ue111"))

;;  (fn [_] (client-x core/get-column0
;;                    "1"
;;                    [cf "col-name"]))
;;  )
(time
 (dotimes [d 1000]
   (client-x core/insert-column
             "1"
             cf
             (core/column "col-name" "col-value"))))
