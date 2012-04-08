(ns casyn.balancer.least-loaded
  (:require
   [casyn.balancer :as b]
   [casyn.pool :as p]
   [lamina.core :as lac])
  (:import [java.util.concurrent LinkedBlockingQueue ]))

(deftype LeastLoadedBalancer [^LinkedBlockingQueue nodes]

  b/PBalancer

  (get-nodes [b]
    (to-array nodes))

  (select-node [b pool avoid-node-set]
    (->> nodes
         (remove avoid-node-set)
         (sort-by (partial p/active-clients pool))
         first))

  (register-node [b node]
    (.offer nodes node))

  (deregister-node [b node]
    (.remove nodes node)))

(defmethod b/balancer :least-loaded [_ & initial-nodes]
  (LeastLoadedBalancer. (LinkedBlockingQueue. initial-nodes)))
