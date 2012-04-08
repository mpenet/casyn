(ns casyn.balancer)

(defprotocol PBalancer
  (get-nodes [b])
  (select-node [b pool avoid-node-set])
  (register-node [b node])
  (deregister-node [b node]))

(defmulti balancer (fn [k & initial-nodes] k))