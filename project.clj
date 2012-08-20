(defproject cc.qbits/casyn "0.1.4-SNAPSHOT"
  :description "Clojure client for Cassandra using Thrift AsyncClient + Lamina/perf"
  :url "https://github.com/mpenet/casyn"
  :license {:name "Eclipse Public License"}
  :dependencies [[org.clojure/clojure "1.4.0"]
                 [org.clojure/core.incubator "0.1.0"]
                 [lamina "0.5.0-SNAPSHOT"]
                 [org.apache.cassandra/cassandra-all "1.1.3"]
                 [tron "0.5.3"]
                 [useful "0.8.0"]
                 [commons-pool "1.6"]
                 [com.taoensso/nippy "0.10.1"]]
  :profiles {:1.4  {:dependencies [[org.clojure/clojure "1.4.0"]]}
             :1.5  {:dependencies [[org.clojure/clojure "1.5.0-master-SNAPSHOT"]]}
             :dev  {:dependencies [[codox "0.6.1"]]}
             :test {:dependencies []}}
  :min-lein-version "2.0.0"
  :warn-on-reflection true)
