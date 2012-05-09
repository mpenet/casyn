(ns casyn.cluster
  "Different cluster implementation can provide different
   node pools selection policies and different client selection also")

(defprotocol PCluster
  (get-pool [cluster] "")
  (get-balancer [cluster] "")
  (add-node [cluster node] "")
  (remove-node [cluster node] "")
  (select-node [cluster avoid-node-set] ""))

(defprotocol PDiscoverable
  (refresh [cluster nodes] ""))