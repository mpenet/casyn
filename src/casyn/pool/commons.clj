(ns casyn.pool.commons
  (:use [casyn.pool])

  (:require
   [casyn.client :as c]
   [casyn.core :as core]
   [casyn.ddl :as ddl]
   [lamina.core :as lac])

  (:import
   [org.apache.commons.pool KeyedPoolableObjectFactory]
   [org.apache.commons.pool.impl GenericKeyedObjectPool]))

(def exhausted-actions
  {:fail GenericKeyedObjectPool/WHEN_EXHAUSTED_FAIL
   :block GenericKeyedObjectPool/WHEN_EXHAUSTED_BLOCK
   :grow GenericKeyedObjectPool/WHEN_EXHAUSTED_GROW})

(defn create-pool [factory max-active exhausted-action max-wait max-idle]
  (GenericKeyedObjectPool. factory
                           max-active
                           (exhausted-actions exhausted-actions)
                           max-wait
                           max-idle))

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
        @(core/set-keyspace client keyspace)
        client))

    (destroyObject [this node-host client]
      (c/kill client))

    (activateObject [this node-host client])
    (validateObject [this node-host client])
    (passivateObject [this node-host client])))

(defn create-pool
  "Using the constructor with the most options here,
   it is not very pretty but at least everything is accessible from the
   clojure side without having to do java interop and I can control
   defaults if needed"
  ^GenericKeyedObjectPool
  [host port keyspace
   & {:keys [max-active exhausted-action max-wait max-idle max-total min-idle
             test-on-borrow test-on-return time-between-eviction-runs-millis
             num-tests-per-eviction-run min-evictable-idle-time-millis
             test-while-idle lifo]
      :or {max-active GenericKeyedObjectPool/DEFAULT_MAX_ACTIVE
           exhausted-action :block
           max-wait GenericKeyedObjectPool/DEFAULT_MAX_WAIT
           max-idle GenericKeyedObjectPool/DEFAULT_MAX_IDLE
           max-total GenericKeyedObjectPool/DEFAULT_MAX_TOTAL
           min-idle GenericKeyedObjectPool/DEFAULT_MIN_IDLE
           test-on-borrow GenericKeyedObjectPool/DEFAULT_TEST_ON_BORROW
           test-on-return GenericKeyedObjectPool/DEFAULT_TEST_ON_RETURN
           time-between-eviction-runs-millis GenericKeyedObjectPool/DEFAULT_TIME_BETWEEN_EVICTION_RUNS_MILLIS
           num-tests-per-eviction-run GenericKeyedObjectPool/DEFAULT_NUM_TESTS_PER_EVICTION_RUN
           min-evictable-idle-time-millis GenericKeyedObjectPool/DEFAULT_MIN_EVICTABLE_IDLE_TIME_MILLIS
           test-while-idle GenericKeyedObjectPool/DEFAULT_TEST_WHILE_IDLE
           lifo GenericKeyedObjectPool/DEFAULT_LIFO}
      :as options}]

  (GenericKeyedObjectPool. (make-factory port keyspace)
                           max-active
                           (exhausted-actions exhausted-action)
                           max-wait
                           max-idle
                           max-total
                           min-idle
                           test-on-borrow
                           test-on-return
                           time-between-eviction-runs-millis
                           num-tests-per-eviction-run
                           min-evictable-idle-time-millis
                           test-while-idle
                           lifo))