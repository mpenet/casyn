(ns qbits.casyn.auto-discovery
  ""
  (:require
   [qbits.casyn.client :as c]
   [qbits.casyn.api :as api]
   [qbits.casyn.cluster :as clu]
   [qbits.knit.core :as knit]
   [lamina.core :as lc]
   [useful.exception :as uex]
   [clojure.tools.logging :as log])
  (:import
   [org.apache.cassandra.thrift KsDef TokenRange EndpointDetails]))

(defn discover
  [cluster]
  (try
    (let [cx (c/client-fn cluster :failover :try-all)
          keyspaces @(cx api/describe-keyspaces)]
      (reduce
       (fn [nodes ^KsDef ks]
         (let [ks-name (.getName ks)]
           (if (= ks-name  "system")
             nodes ;; exclude system keyspace
             @(lc/run-pipeline
               (cx api/describe-ring ks-name)
               {:error-handler (fn [_] (lc/complete nodes))} ;; next ks
               (fn [token-ranges]
                 (apply conj nodes
                        (for [^TokenRange range token-ranges
                              ^EndpointDetails endpoint (.getEndpoint_details range)]
                          (.getHost endpoint))))))))
       #{}
       keyspaces))
    (catch Exception e
      (log/error (uex/exception-map e)))))

(defn start-worker
  ([cluster interval]
     (knit/schedule :with-fixed-delay interval
      #(when-let [nodes (discover cluster)]
         (clu/refresh cluster nodes))))
  ([cluster]
     (start-worker cluster 100)))