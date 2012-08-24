(ns casyn.cluster.core
  (:require
   [casyn.cluster :refer [add-node remove-node select-node
                          get-balancer get-pool refresh
                          PCluster PDiscoverable]]
   [clojure.set :refer [difference]]
   [clojure.tools.logging :as log]
   [casyn.utils :as u]
   [casyn.balancer :as b]
   [casyn.pool :as p]
   [casyn.client :as c]
   [casyn.balancer.least-loaded :as bll]
   [casyn.balancer.round-robin :as brr]
   [casyn.auto-discovery :as discovery]
   [casyn.pool.commons :as commons-pool])

  (:import [org.apache.commons.pool.impl GenericKeyedObjectPool]))

(deftype Cluster [balancer pool cf-pool options]
  PCluster

  (get-pool [cluster] pool)
  (get-balancer [cluster] balancer)

  (select-node [cluster avoid-node-set]
    (b/select-node balancer pool avoid-node-set))

  (add-node [cluster node-host]
    (log/info :cluster.node.add node-host)
    (p/add pool node-host)
    (b/register-node balancer node-host))

  (remove-node [cluster node-host]
    (log/info :cluster.node.remove node-host)
    (p/drain pool node-host)
    (b/unregister-node balancer node-host))

  PDiscoverable
  (refresh [cluster active-nodes]
    (let [current-nodes-hosts (into #{} (b/get-nodes balancer))]
      (doseq [node-host (difference active-nodes current-nodes-hosts)]
        (add-node cluster node-host))
      (doseq [node-host (difference current-nodes-hosts active-nodes)]
        (remove-node cluster node-host)))))

(defn make-cluster
  "hosts can be a sequence or a single value, port will be the same
  for all the hosts, you can manage a fixed sized cluster with
  predetermined ips by turning auto-discovery off"
  [hosts port
  keyspace & {:keys [auto-discovery load-balancer-strategy selector-threads-num]
                          :or {auto-discovery true
                               load-balancer-strategy :round-robin
                               selector-threads-num 3}
                          :as options}]

  (let[cf-pool (c/client-factory-pool selector-threads-num)
       cluster (Cluster. (b/balancer load-balancer-strategy)
                         (apply commons-pool/create-pool port keyspace cf-pool
                                (mapcat (juxt key val) (:pool options)))
                         cf-pool
                         options)]

    (if (sequential? hosts)
      (doseq [host hosts]
        (add-node cluster (u/host->ip host)))
      (add-node cluster (u/host->ip hosts)))

    (when auto-discovery
      (discovery/start-worker cluster 2000))

    cluster))
