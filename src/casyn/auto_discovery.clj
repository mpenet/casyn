(ns casyn.auto-discovery
  (:require
   [clojure.core.incubator :refer [-?>>]]
   [casyn.api :as api]
   [casyn.client :as cl]
   [casyn.cluster :as clu]
   tron))

(defn discover
  [cluster]
  (try
    (let [cx (cl/client-fn cluster :failover :try-all)]
      (-?>> @(cx api/describe-keyspaces)
            (map #(.getName %))
            (remove #{"system"})
            (map (fn [ks] (try
                            @(cx api/describe-ring ks)
                            (catch Exception e
                              (println e) nil))))
            (filter identity)
            ((fn [r]
               (for [ranges r
                     range ranges
                     endpoint (.getEndpoint_details range)]
                 (.getHost endpoint))))
            set))
    (catch Exception e
      (.printStackTrace e)
      nil)))

(defn start-worker
  ([cluster interval]
     (tron/periodically
      :casyn.auto-discover.worker
      #(when-let [nodes (discover cluster)]
         (clu/refresh cluster nodes))
      interval))

  ([cluster]
     (start-worker cluster 100)))