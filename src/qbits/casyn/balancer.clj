(ns qbits.casyn.balancer
  "Protocol defining what a Balancer should support in order to be
  compatible with cluster instances.")

(defprotocol PBalancer
  (get-nodes [b] "Retuns a collection of the current nodes registered")
  (select-node [b pool avoid-node-set] "Returns an active node, using the current strategy")
  (register-node [b node] "Notify balancer of new node")
  (unregister-node [b node] "Removes node from balancer"))

(defmulti balancer (fn [k & opts] k))