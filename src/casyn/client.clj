(ns casyn.client
  (:require
   [lamina.core :as lc]
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
   [org.apache.thrift.async TAsyncClient TAsyncClientManager]
   [java.util.concurrent LinkedBlockingQueue]))

(defn client-factory []
  (Cassandra$AsyncClient$Factory.
   (TAsyncClientManager.)
   (TBinaryProtocol$Factory.)))

(defn resize-client-factory-pool
  [^LinkedBlockingQueue pool num]
  (let [diff (- num (.size pool))]
    (dotimes [i (java.lang.Math/abs ^Integer diff)]
      (if (pos? diff)
        (.put pool (client-factory))
        (.poll pool))))
  pool)

(defn client-factory-pool [initial-size]
  (resize-client-factory-pool (LinkedBlockingQueue.) initial-size))

(defn select
  [^LinkedBlockingQueue pool]
  (let [cf (.poll pool)]
    (.put pool cf)
    cf))

(def defaults
  {:timeout 5000
   :host "127.0.0.1"
   :port 9160
   :pool (client-factory-pool 3)})

(defn make-client
  "Create client with its own socket"
  ([host port pool timeout]
     (doto (.getAsyncClient ^Cassandra$AsyncClient$Factory (select pool)
                            (TNonblockingSocket. host port))
       (.setTimeout timeout)))
  ([host port pool]
     (make-client host
                  port
                  pool
                  (:timeout defaults)))
  ([host port]
     (make-client host
                  port
                  (:pool defaults)
                  (:timeout defaults)))
  ([host]
     (make-client host
                  (:port defaults)
                  (:pool defaults)
                  (:timeout defaults)))
  ([]
     (make-client (:host defaults)
                  (:port defaults)
                  (:pool defaults)
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
  (lc/pipeline
   (fn [state]
     (let [{:keys [node-host client pool]} state]
       (p/return-or-invalidate pool node-host client)))))

(defn run-command-stage
  [state]
  (lc/run-pipeline
   nil
   {:error-handler
    (fn [e]
      (dispose-pipeline state)
      (let [etype (type e)]
        (lc/complete
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
       (apply f client args)))
   #(do
      (dispose-pipeline state)
      [nil %])))

(defn select-client-stage
  [state]
  (lc/run-pipeline
   (p/borrow (:pool state) (:node-host state))
   {:error-handler (fn [e] (lc/complete [failover-stage (assoc state :error e)]))}
   #(vector run-command-stage
            (assoc state :client (set-timeout % (:client-timeout state))))))

(defn select-pool-stage
  [state]
  (lc/run-pipeline
   (c/get-pool (:cluster state))
   {:error-handler (fn [e] (lc/complete [failover-stage (assoc state :error e)]))}
   #(vector select-client-stage (assoc state :pool %))))

(defn select-node-stage ;; start
  [state]
  (lc/run-pipeline
   (c/select-node (:cluster state) (:avoid-node-set state))
   {:error-handler (fn [e] (lc/complete [failover-stage (assoc state :error e)]))}
   #(vector select-pool-stage (assoc state :node-host %))))

(defn client-fn
  "Returns a fn that will execute its first arg against the rest of
   args. handles the client borrow/return/sanity/timeouts checks,
   returns a result-channel.
   :client-timeout is in ms
   :failover can be :try-all, or :try-next, disabled by default,
  inherits cluster settings"
  [cluster & {:keys [client-timeout failover]
              :or {client-timeout (:client-timeout (c/get-options cluster))
                   failover (:failover (c/get-options cluster))}}]
  (fn [f & more]
    (lc/run-pipeline
     (select-node-stage
      {:cluster cluster
       :client-timeout client-timeout
       :failover failover
       :f f
       :args more})
     {:error-handler (fn [_])}
     (fn [[next-stage state]]
       (if next-stage
         (lc/restart (next-stage state))
         (lc/complete state))))))