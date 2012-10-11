(ns casyn.pool.commons
  (:require
   [casyn.pool :refer [PPool returnable? return invalidate]]
   [casyn.client :as c]
   [casyn.api :as api]
   [casyn.executor :as x])

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

(defn make-factory
  [port keyspace cf-pool callback-executor]
  (reify KeyedPoolableObjectFactory
    (makeObject [this node-host]
      (when-let [client (c/make-client node-host port cf-pool callback-executor)]
        @(api/set-keyspace client keyspace)
        client))
    (destroyObject [this node-host client]
      (c/kill client))
    (activateObject [this node-host client])
    (validateObject [this node-host client])
    (passivateObject [this node-host client])))

;; borrowed from ptaoussanis/accession
(defn set-pool-option
  [^GenericKeyedObjectPool pool [opt v]]
  (case opt
    :max-active                    (.setMaxActive pool v)
    :max-total                     (.setMaxTotal pool v)
    :min-idle                      (.setMinIdle pool v)
    :max-idle                      (.setMaxIdle pool v)
    :max-wait                      (.setMaxWait pool v)
    :lifo                          (.setLifo pool v)
    :test-on-borrow                (.setTestOnBorrow pool v)
    :test-on-return                (.setTestOnReturn pool v)
    :test-while-idle               (.setTestWhileIdle pool v)
    :when-exhausted-action         (.setWhenExhaustedAction pool v)
    :num-tests-per-eviction-run    (.setNumTestsPerEvictionRun pool v)
    :time-between-eviction-runs-ms (.setTimeBetweenEvictionRunsMillis pool v)
    :min-evictable-idle-time-ms    (.setMinEvictableIdleTimeMillis pool v))
  pool)

(def defaults
  {:when-exhausted-action GenericKeyedObjectPool/WHEN_EXHAUSTED_BLOCK})

(defn make-pool
  "Create a connection pool.

List of supported options are:

+ :max-active
+ :max-total
+ :min-idle
+ :max-idle
+ :max-wait
+ :lifo
+ :test-on-borrow
+ :test-on-return
+ :test-while-idle
+ :when-exhausted-action
+ :num-tests-per-eviction-run
+ :time-between-eviction-runs-ms
+ :min-evictable-idle-time-ms

They map to the equivalents you can find on the apache commons pool documentation:
http://commons.apache.org/pool/apidocs/org/apache/commons/pool/impl/GenericKeyedObjectPool.html"
  ^GenericKeyedObjectPool
  [port keyspace cf-pool callback-executor & options]
  (reduce set-pool-option
          (GenericKeyedObjectPool. (make-factory port
                                                 keyspace
                                                 cf-pool
                                                 callback-executor))
          (merge defaults (apply hash-map options))))