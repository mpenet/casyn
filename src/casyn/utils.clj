(ns casyn.utils)

(defn ts
  "Simple/naive microsec clock"
  []
  (* (System/currentTimeMillis) 1001))

(defonce ts-sync-counter (atom 0))

(defn sync-ts
  "Microsec clock somewhat safe at least locally, but it seems that is
  what most cients also provide"
  []
  (+ (swap! ts-sync-counter inc) (ts)))

(defn host->ip
  "Convert string input to valid ip resolves hostname if necessary"
  [s]
  (.getHostAddress (java.net.InetAddress/getByName s)))
