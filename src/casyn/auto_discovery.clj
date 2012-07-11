(ns casyn.auto-discovery
  (:require
   [casyn.api :as api]
   [casyn.cluster :as clu]
   [casyn.client :as c]
   [lamina.core :as lc]
   tron)
  (:import [org.apache.cassandra.thrift KsDef TokenRange EndpointDetails]))

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
      (.printStackTrace e))))

(defn start-worker
  ([cluster interval]
     (tron/periodically
      :casyn.auto-discovery.worker
      #(when-let [nodes (discover cluster)]
         (clu/refresh cluster nodes))
      interval))
  ([cluster]
     (start-worker cluster 100)))