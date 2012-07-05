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
    TimedOutException UnavailableException ]
   [org.apache.thrift TApplicationException]
   [org.apache.thrift.transport TNonblockingSocket]
   [org.apache.thrift.protocol TBinaryProtocol$Factory]
   [org.apache.thrift.async TAsyncClient TAsyncClientManager]))

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
  (kill [client]))

(extend-type TAsyncClient

  PClient
  (set-timeout [client timeout]
    (.setTimeout client timeout)
    client)

  (has-errors? [client]
    (try (.hasError client)
         (catch IllegalStateException e true)))

  (kill [client])

  p/PPoolableClient
  (borrowable? [client]
    "Health check client before borrow"
    (not (has-errors? client)))

  (returnable? [client]
    "Health check client before it is returned"
    (not (has-errors? client))))

(declare select-node-stage)

(defn error-stage
  [state]
  (throw (:error state)))

(defmulti failover-stage :failover)

(defmethod failover-stage :try-all [value]
  (let [nodes (-> value :cluster c/get-balancer b/get-nodes)]
    (if (< (count nodes) (count (:avoid-node-set value)))
      [select-node-stage (update-in value [:avoid-node-set] conj (:node-host value))]
      [error-stage value])))

(defmethod failover-stage :try-next [value]
  (if (empty? (:avoid-node-set value))
    [select-node-stage (assoc value :avoid-node-set #{(:node-host value)})]
    [error-stage value]))

(defmethod failover-stage :default [v]
  [error-stage v])

(def dispose-pipeline
  (lac/pipeline
   (fn [state]
     (let [{:keys [node-host client pool]} state]
       (p/return-or-invalidate pool node-host client)))))

(defn run-command-stage
  [state]
  (lac/run-pipeline
   nil
   {:error-handler
    (fn [e]
      (dispose-pipeline state)
      (let [etype (type e)]
        (lac/complete
         (cond
           (= NotFoundException etype)
           [nil nil]
           (contains? #{TimedOutException
                        UnavailableException
                        TApplicationException}
                      etype)
           [failover-stage (assoc state :error e)]

           :else
           [error-stage (assoc state :error e)]))))}
   (fn [_]
     (let [{:keys [f client args]} state]
       (when-let [timeout (:client-timeout state)]
         (set-timeout client timeout))
       (apply f client args)))
   #(do
      (dispose-pipeline state)
      [nil %])))

(defn select-client-stage
  [state]
  (lac/run-pipeline
   (p/borrow (:pool state) (:node-host state))
   {:error-handler (fn [e] (lac/complete [failover-stage (assoc state :error e)]))}
   #(vector run-command-stage (assoc state :client %))))

(defn select-pool-stage
  [state]
  (lac/run-pipeline
   (c/get-pool (:cluster state))
   {:error-handler (fn [e] (lac/complete [failover-stage (assoc state :error e)]))}
   #(vector select-client-stage (assoc state :pool %))))

(defn select-node-stage ;; start
  [state]
  (lac/run-pipeline
   (c/select-node (:cluster state) (:avoid-node-set state))
   {:error-handler (fn [e] (lac/complete [failover-stage (assoc state :error e)]))}
   #(vector select-pool-stage (assoc state :node-host %))))

(defn client-fn
  "Returns a fn that will execute its first arg against the
   rest of args handling the client borrow/return/sanity/timeouts checks"
  [cluster & {:keys [timeout failover]}]
  (fn [f & more]
    (lac/run-pipeline
     (select-node-stage
      {:cluster cluster
       :client-timeout nil
       :failover failover
       :f f
       :args more})
     (fn [[next-stage state]]
       (if next-stage
         (lac/restart (next-stage state))
         (lac/complete state))))))