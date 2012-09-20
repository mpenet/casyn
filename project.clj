(defproject cc.qbits/casyn "0.1.7-SNAPSHOT"
  :description "Clojure client for Cassandra using Thrift AsyncClient + Lamina/perf"
  :url "https://github.com/mpenet/casyn"
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}
  :dependencies [[org.clojure/clojure "1.4.0"]
                 [lamina "0.5.0-beta6"]
                 [org.apache.cassandra/cassandra-all "1.1.5"]
                 [tron "0.5.3"]
                 [useful "0.8.4"]
                 [commons-pool "1.6"]
                 [com.taoensso/nippy "0.10.2"]
                 [cc.qbits/tardis "0.1.0"]
                 [clj-time "0.4.4"]]
  :profiles {:1.4  {:dependencies [[org.clojure/clojure "1.4.0"]]}
             :1.5  {:dependencies [[org.clojure/clojure "1.5.0-master-SNAPSHOT"]]}
             :dev  {:dependencies [[codox "0.6.1"]]}
             :test {:dependencies []}}
  :min-lein-version "2.0.0"
  :warn-on-reflection true)