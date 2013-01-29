(ns qbits.casyn.auto-discovery
  "Discovery of server nodes as they are added/removed"
  (:require
   [qbits.casyn.client :as c]
   [qbits.casyn.api :as api]
   [qbits.casyn.cluster :as clu]
   [qbits.knit :as knit]
   [lamina.core :as lc]
   [useful.exception :as uex]
   [clojure.tools.logging :as log])
  (:import
   [org.apache.cassandra.thrift KsDef TokenRange EndpointDetails]))

(defprotocol PAutodiscovery
  (start [worker cluster interval])
  (discover [worker cluster]  "Tries to obtain a list of current valid/active nodes in the cluster")
  (shutdown [worker]))

(defn make-worker
  "Scheduled worker launched from a qbits.casyn.PCluster instance to trigger
discovery continuously"
  []
  (let [executor (knit/executor :scheduled)]
    (reify
      PAutodiscovery
      (discover [worker cluster]
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

      (start [worker cluster interval]
        (knit/schedule :with-fixed-delay interval :executor executor
                       #(when-let [nodes (discover worker cluster)]
                          (clu/refresh cluster nodes))))
      (shutdown [worker]
        (log/info "Shuting down discovery worker")
        (.shutdown ^java.util.concurrent.ExecutorService executor)))))
