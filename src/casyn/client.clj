(ns casyn.client
  (:require
   [lamina.core :as lac]
   [casyn.cluster :as c]
   [casyn.pool :as p]
   [casyn.balancer :as b])

  (:import
   [org.apache.cassandra.thrift Cassandra$AsyncClient Cassandra$AsyncClient$Factory]
   [org.apache.thrift.transport TNonblockingSocket]
   [org.apache.thrift.protocol TBinaryProtocol$Factory]
   [org.apache.thrift.async TAsyncClient TAsyncClientManager]
   [org.apache.commons.pool.impl GenericKeyedObjectPool]))

(def client-factory (Cassandra$AsyncClient$Factory.
                     (TAsyncClientManager.)
                     (TBinaryProtocol$Factory.)))

(def defaults
  {:timeout 5000
   :host "127.0.0.1"
   :port 9160})

(defn make-client
  "Create client with its own socket"
  ([host port timeout]
     (try
       (doto (.getAsyncClient ^Cassandra$AsyncClient$Factory client-factory
                              (TNonblockingSocket. host port))
         (.setTimeout timeout))
       (catch Exception _ nil)))

  ([host port]
     (make-client host
                  port
                  (:timeout defaults)))
  ([]
     (make-client (:host defaults)
                  (:port defaults)
                  (:timeout defaults))))

(defprotocol PClient
  (set-timeout [client timeout])
  (has-errors? [client])
  (kill [client] "kill socket etc?"))

(extend-type TAsyncClient

  PClient
  (set-timeout [client]
    (.setTimeout client)
    client)

  (has-errors? [client]
    (try (.hasError client)
         (catch IllegalStateException e true)))

  p/PPoolableClient
  (borrowable? [client]
    "Health check client before borrow"
    (not (has-errors? client)))

  (returnable? [client]
    "Health check client before it is returned"
    (not (has-errors? client)))

  (kill [client]))

(declare failover)

(def error-pipeline
  (lac/pipeline failover))

(def dispose-pipeline
  (lac/pipeline
   (fn [state]
     (let [{:keys [node-host client pool]} state]
       (p/return-or-invalidate pool node-host client)))))

(defn assoc-stage
  [stage-key stage & [error-handler]]
  (fn [state]
    (lac/run-pipeline
     state
     {:error-handler
      (when error-handler
        (fn [e]
          (error-handler state e)))}
     stage
     #(assoc state stage-key %))))

(def client-pipeline
  (lac/pipeline
   (assoc-stage :node-host #(c/select-node (:cluster %) (:avoid-node-set %)))
   (assoc-stage :pool #(c/get-pool (:cluster %)) error-pipeline)
   (assoc-stage :client #(p/borrow (:pool %) (:node-host %)) error-pipeline)
   (assoc-stage :result (fn [state]
                          (let [{:keys [f client args]} state]
                            (when-let [timeout (:client-timeout state)]
                              (set-timeout client timeout))
                            (apply f client args)))
                (fn [state e]
                  (dispose-pipeline state)
                  (error-pipeline state)))
   (fn [state]
     (dispose-pipeline state)
     (:result state))))

(defmulti failover :failover)

(defmethod failover :try-all [state]
  (let [nodes (-> state :cluster c/get-balancer b/get-nodes)]
    (when (< (count nodes) (count (:avoid-node-set state)))
      (lac/redirect client-pipeline
                    (update-in state [:avoid-node-set] conj (:node-host state))))))

(defmethod failover :try-next [state]
  (when (empty? (:avoid-node-set state))
    (lac/redirect client-pipeline
                  (assoc state :avoid-node-set #{(:node-host state)}))))

(defmethod failover :default identity)

(defn client-executor
  "Returns a fn that will execute its first arg against the
   rest of args handling the client borrow/return/sanity/timeouts checks"
  [cluster & {:keys [timeout failover]}]
  (fn [f & more]
    (client-pipeline
     {:cluster cluster
      :client-timeout nil
      :failover failover
      :f f
      :args more})))
