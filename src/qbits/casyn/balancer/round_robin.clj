(ns qbits.casyn.balancer.round-robin
    "Implementation of a balancing strategy where nodes are selected in the order
they where registered, evenly distributing charge between them"
  (:require
   [qbits.casyn.balancer :refer [PBalancer balancer]])
  (:import
   [java.util.concurrent LinkedBlockingQueue]
   [java.util Collection]))

(defmethod balancer :round-robin [_ & opts]
  (let [nodes (LinkedBlockingQueue.)]
    (reify PBalancer

      (get-nodes [b]
        (to-array nodes))

      (select-node [b _ avoid-node-set]
        (let [node (.take nodes)]
          (.offer nodes node) ;; complete rotation
          (if (contains? avoid-node-set node)
            (.select-node b _ avoid-node-set)
            node)))

      (register-node [b node]
        (.offer nodes node))

      (unregister-node [b node]
        (.remove nodes node)))))