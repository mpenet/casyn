(ns casyn.balancer.round-robin
  (:require [casyn.balancer :refer [PBalancer balancer]])
  (:import [java.util.concurrent LinkedBlockingQueue]
           [java.util Collection]))

(deftype RoundRobinBalancer [^LinkedBlockingQueue nodes]

  PBalancer

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
    (.remove nodes node)))


(defmethod balancer :round-robin [_]
  (RoundRobinBalancer. (LinkedBlockingQueue.)))