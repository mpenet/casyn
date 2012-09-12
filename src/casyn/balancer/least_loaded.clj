(ns casyn.balancer.least-loaded
  (:require
   [casyn.balancer :refer [PBalancer balancer]]
   [casyn.pool :as p])
  (:import
   [java.util.concurrent LinkedBlockingQueue]))

(deftype LeastLoadedBalancer [^LinkedBlockingQueue nodes]

  PBalancer

  (get-nodes [b]
    (to-array nodes))

  (select-node [b pool avoid-node-set]
    (->> nodes
         (remove avoid-node-set)
         (sort-by (partial p/active-clients pool))
         first))

  (register-node [b node]
    (.offer nodes node))

  (unregister-node [b node]
    (.remove nodes node)))

(defmethod balancer :least-loaded [_ & opts]
  (LeastLoadedBalancer. (LinkedBlockingQueue.)))
