(ns casyn.pool.commons
  (:require
   [casyn.pool :refer [PPool returnable? return invalidate]]
   [casyn.client :as c]
   [casyn.api :as api]
   [casyn.ddl :as ddl]
   [lamina.core :as lac])

  (:import
   [org.apache.commons.pool KeyedPoolableObjectFactory]
   [org.apache.commons.pool.impl GenericKeyedObjectPool]))

(extend-type GenericKeyedObjectPool
  PPool
  (add [pool node-host]
    (.addObject pool node-host))

  (borrow [pool node-host]
    (.borrowObject pool node-host))

  (return [pool node-host client]
    (.returnObject pool node-host client))

  (invalidate [pool node-host client]
    (.invalidateObject pool node-host client))

  (return-or-invalidate [pool node-host client]
    (if (returnable? client)
      (return pool node-host client)
      (invalidate pool node-host client)))

  (drain [pool node-host]
    (.clear pool node-host))

  (drain [pool]
    (.clear pool))

  (close [pool]
    (.close pool))

  (active-clients [pool node-host]
    (.getNumActive pool node-host))

  (active-clients [pool]
    (.getNumActive pool))

  (idle-clients [pool node-host]
    (.getNumIdle pool node-host))

  (idle-clients [pool]
    (.getNumIdle pool)))

(defn make-factory [port keyspace]
  (reify KeyedPoolableObjectFactory
    (makeObject [this node-host]
      (when-let [client (c/make-client node-host port)]
        @(api/set-keyspace client keyspace)
        client))
    (destroyObject [this node-host client]
      (c/kill client))
    (activateObject [this node-host client])
    (validateObject [this node-host client])
    (passivateObject [this node-host client])))

;; borrowed from ptaoussanis/accession
(defn set-pool-option [^GenericKeyedObjectPool pool [opt v]]
  (case opt
    :max-active (.setMaxActive pool v)
    :max-total  (.setMaxTotal pool v)
    :min-idle   (.setMinIdle pool v)
    :max-idle   (.setMaxIdle pool v)
    :max-wait   (.setMaxWait pool v)
    :lifo       (.setLifo pool v)
    :test-on-borrow  (.setTestOnBorrow pool v)
    :test-on-return  (.setTestOnReturn pool v)
    :test-while-idle (.setTestWhileIdle pool v)
    :when-exhausted-action         (.setWhenExhaustedAction pool v)
    :num-tests-per-eviction-run    (.setNumTestsPerEvictionRun pool v)
    :time-between-eviction-runs-ms (.setTimeBetweenEvictionRunsMillis pool v)
    :min-evictable-idle-time-ms    (.setMinEvictableIdleTimeMillis pool v))
  pool)

(def pool-options-defaults
  {:when-exhausted-action GenericKeyedObjectPool/WHEN_EXHAUSTED_GROW})

(defn create-pool
  "Using the constructor with the most options here,
   it is not very pretty but at least everything is accessible from the
   clojure side without having to do java interop and I can control
   defaults if needed"
  ^GenericKeyedObjectPool
  [host port keyspace & pool-options]
  (reduce set-pool-option
          (GenericKeyedObjectPool. (make-factory port keyspace))
          (merge pool-options-defaults (apply hash-map pool-options))))