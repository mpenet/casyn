(ns casyn.client
  (:require
   [lamina.core :as lac]
   [casyn.cluster :as c]
   [casyn.pool :as p]
   [casyn.balancer :as b])

  (:import
   [org.apache.cassandra.thrift Cassandra$AsyncClient Cassandra$AsyncClient$Factory
    NotFoundException InvalidRequestException AuthenticationException
    AuthorizationException SchemaDisagreementException
    TimedOutException UnavailableException]
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
  (set-timeout [client timeout]
    (.setTimeout client timeout)
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

(def dispose-pipeline
   (lac/pipeline
    (fn [state]
      (let [{:keys [node-host client pool]} state]
        (p/return-or-invalidate pool node-host client)))))

(declare failover)

(defmulti stage (fn [state value] state))

(defmethod stage ::start [_ value]
  (lac/run-pipeline
   (c/select-node (:cluster value) (:avoid-node-set value))
   {:error-handler (fn [e] (lac/complete [::failover (assoc value :error e)]))}
   #(vector ::node-ready (assoc value :node-host %))))

(defmethod stage ::node-ready [_ value]
  (lac/run-pipeline
   (c/get-pool (:cluster value))
   {:error-handler (fn [e] (lac/complete [::failover (assoc value :error e)]))}
   #(vector ::pool-ready (assoc value :pool %))))

(defmethod stage ::pool-ready [_ value]
  (lac/run-pipeline
   (p/borrow (:pool value) (:node-host value))
   {:error-handler (fn [e] (lac/complete [::failover (assoc value :error e)]))}
   #(vector ::client-ready (assoc value :client %))))

(defmethod stage ::client-ready [_ value]
  (lac/run-pipeline
   nil
   {:error-handler (fn [e]
                     (dispose-pipeline value)
                     (lac/complete
                        ;; some errors type bypass failover
                      (case (type e)
                        (TimedOutException
                         UnavailableException
                         TApplicationException)
                        [::failover (assoc value :error e)]
                        [::error (assoc value :error e)])))}
   (fn [_]
     (let [{:keys [f client args]} value]
       (when-let [timeout (:client-timeout value)]
         (set-timeout client timeout))
       (apply f client args)))
   #(do
      (dispose-pipeline value)
      [::success %])))

(defmethod stage ::failover [_ value]
  (failover value))

(defmethod stage ::error [_ value]
  (throw (:error value)))

(defn run-command [initial-state exit-states initial-value]
  (lac/run-pipeline
   [initial-state initial-value]
   (fn [[state value]]
     (if (contains? exit-states state)
       (lac/complete value)
       (lac/restart (stage state value))))))

(defn client-fn
  "Returns a fn that will execute its first arg against the
   rest of args handling the client borrow/return/sanity/timeouts checks"
  [cluster & {:keys [timeout failover]}]
  (fn [f & more]
    (run-command
     ::start
     #{::success ::error}
     {:cluster cluster
      :client-timeout nil
      :failover failover
      :f f
      :args more})))


(defmulti failover :failover)

(defmethod failover :try-all [value]
  (let [nodes (-> value :cluster c/get-balancer b/get-nodes)]
    (if (< (count nodes) (count (:avoid-node-set value)))
      [::start (update-in value [:avoid-node-set] conj (:node-host value))]
      [::error value])))

(defmethod failover :try-next [value]
  (if (empty? (:avoid-node-set value))
    [::start (assoc value :avoid-node-set #{(:node-host value)})]
    [::error value]))

(defmethod failover :default [v]
  [::error v])

;; TODO: more failover strategies, retry on same host, count errors per
;; node/interval, unregister/ban bad nodes from the
;; balancer+cluster for amount of time