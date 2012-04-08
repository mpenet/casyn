(ns casyn.balancer.round-robin
  (:require
   [casyn.balancer :as b]
   [lamina.core :as lac])

  (:import [java.util.concurrent LinkedBlockingQueue]))

(deftype LinkedBlockingQueueRoundRobinBalancer [^LinkedBlockingQueue nodes]

  casyn.balancer/PBalancer

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

  (deregister-node [b node]
    (.remove nodes node)))


(defmethod b/balancer :round-robin [_ & initial-nodes]
  (LinkedBlockingQueueRoundRobinBalancer.
   (LinkedBlockingQueue. initial-nodes)))