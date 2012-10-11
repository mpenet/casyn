(ns casyn.executor
  (:import [java.util.concurrent Future ThreadFactory Executors ExecutorService
            ScheduledThreadPoolExecutor TimeUnit]))

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

(defn periodically
  "Executes fn at specified interval, fn execution offsets the delay"
  [f delay & {:keys [init pool-size]
              :or {init 0 pool-size 1}}]
  (.scheduleWithFixedDelay (ScheduledThreadPoolExecutor. pool-size)
                           ^Runnable f
                           ^long init
                           ^long delay
                           TimeUnit/MILLISECONDS))