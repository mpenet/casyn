(ns casyn.cluster.core
  (:use
   [clojure.set]
   [casyn.cluster]
   [casyn.pool])

  (:require
   [clojure.tools.logging :as log]
   [lamina.core :as lac]
   [casyn.utils :as u]
   [casyn.balancer :as b]
   ;; [casyn.balancer.least-loaded :as bll]
   [casyn.balancer.round-robin :as brr]
   [casyn.auto-discovery :as d]
   [casyn.core :as core]
   [casyn.pool.commons :as p])

  (:import [org.apache.commons.pool.impl GenericKeyedObjectPool]))

(deftype Cluster [host port keyspace balancer pool options]
  PCluster

  (get-pool [cluster] pool)
  (get-balancer [cluster] balancer)

  (select-node [cluster avoid-node-set]
    (b/select-node balancer pool avoid-node-set))

  (add-node [cluster node-host]
    (log/info :cluster.node.add node-host)
    (add pool node-host)
    (b/register-node balancer node-host))

  (remove-node [cluster node-host]
    (log/info :cluster.node.remove node-host)
    (drain pool node-host)
    (b/deregister-node balancer node-host))

  PDiscoverable
  (refresh [cluster active-nodes]
    (let [current-nodes-hosts (into #{} (b/get-nodes balancer))]
      (doseq [node-host (difference active-nodes current-nodes-hosts)]
        (add-node cluster node-host))
      (doseq [node-host (difference current-nodes-hosts active-nodes)]
        (remove-node cluster node-host)))))

(defn make-cluster
  [host port keyspace & {:keys [auto-discovery load-balancer-strategy]
                         :or {auto-discovery true
                              load-balancer-strategy :round-robin}
                         :as options}]

  (let [host (u/host->ip host)
        cluster (Cluster. host port keyspace
                          (b/balancer load-balancer-strategy host)
                          (apply p/create-pool host port keyspace
                                 (mapcat (juxt key val) (:pool options)))
                          options)]

    ;; (Thread/sleep 4000)
    (when auto-discovery
      (d/start-worker cluster 2000))

    cluster))
