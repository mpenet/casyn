(ns qbits.casyn.cluster
  "Different cluster implementation can provide different
   node pools selection policies and different client selection also")

(defprotocol PCluster
  (get-pool [cluster] "")
  (get-balancer [cluster] "")
  (get-options [cluster] "")
  (add-node [cluster node] "")
  (remove-node [cluster node] "")
  (select-node [cluster avoid-node-set] "")
  (shutdown [cluster] "Release cluster ressources (pools, executors etc)"))

(defprotocol PDiscoverable
  (refresh [cluster nodes] ""))