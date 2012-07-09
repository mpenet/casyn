(ns playground
  (:require [casyn.core :as c]
            [lamina.core :as lac]))

(def ks "casyn_test_ks")
(def cf "test_cf")
(def ccf "counter_cf")



(try
 @(c/add-keyspace (c/make-client)
                    ks
                    "SimpleStrategy"
                    [[cf]
                     [ccf
                      :default-validation-class :counter
                      :replicate-on-write true]]
                    :strategy-options {"replication_factor" "1"})

 (catch Exception e nil)  )



(c/defschema test-schema
  :row :string
  :super :string
  :columns
  {:default [:string :string]
   :exceptions {"age" :long}})

(def cl (c/make-cluster "localhost" 9160 ks
                        ;; :pool {:max-active 12 :max-idle 12 :max-total 12}
                        ))

(def client-x (c/client-fn cl))


;; (prn cl)

;; (prn (.nodes (.balancer cl)))

;; (use 'casyn.auto-discovery)

;; (prn (cp/refresh cl (prn (discover cl))))
;; ;; (connection/close  client-pool)

(prn @(client-x c/insert-column
                cf
                "1"
                (c/column "col-name" "col-value")))



(prn
 @(client-x c/get-column
            cf
            "1"
            "col-name"))


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
   (client-x c/insert-column
             cf
             "1"
             (c/column "col-name" "col-value"))))

;; "Elapsed time: 1365.107026 msecs"
;; "Elapsed time: 1255.803791 msecs"
;; "Elapsed time: 1231.108563 msecs"
;; "Elapsed time: 1229.586674 msecs"
;; "Elapsed time: 1233.15066 msecs"
;; "Elapsed time: 1945.389424 msecs"
;; "Elapsed time: 1893.17525 msecs"
;; "Elapsed time: 1218.882871 msecs"
;; "Elapsed time: 1222.487246 msecs"
;; "Elapsed time: 1208.44207 msecs"