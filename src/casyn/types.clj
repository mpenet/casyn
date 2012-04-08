(ns casyn.types
  "Internal data types for cassandra results")

(defrecord Column [name value ttl timestamp])
(defrecord CounterColumn [name value])
(defrecord SuperColumn [name columns])
(defrecord CounterSuperColumn [name columns])
(defrecord KeySlice [row columns])
