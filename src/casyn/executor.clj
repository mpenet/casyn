(ns casyn.executor
  (:import [java.util.concurrent Callable Future ExecutorService Executors ThreadFactory
            CancellationException ExecutionException TimeoutException]))

(defn thread-factory [& {:keys [daemon]
                         :or {daemon true}}]
  (reify ThreadFactory
    (newThread [_ f]
      (doto (Thread. f)
        (.setDaemon daemon)))))

(defn ^Future execute
  [^ExecutorService executor ^Callable f]
  (.submit executor f))

(def default-executor (Executors/newCachedThreadPool (thread-factory)))
