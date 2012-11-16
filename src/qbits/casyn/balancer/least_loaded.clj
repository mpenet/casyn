(ns qbits.casyn.balancer.least-loaded
  "Implementation of a failover strategy where the Node pool the with the least
active client is selected first"
  (:require
   [qbits.casyn.balancer :refer [PBalancer balancer]]
   [qbits.casyn.pool :as p])
  (:import
   [java.util.concurrent LinkedBlockingQueue]))

(defmethod balancer :least-loaded [_ & opts]
  (let [nodes (LinkedBlockingQueue.)]
    (reify PBalancer

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
        (.remove nodes node)))))
