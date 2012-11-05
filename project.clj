(defproject cc.qbits/casyn "0.9.3-SNAPSHOT"
  :description "Cassandra client with support for asynchronous operations"
  :url "https://github.com/mpenet/casyn"
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}
  :dependencies [[org.clojure/clojure "1.4.0"]
                 [lamina "0.5.0-beta7"]
                 [org.apache.cassandra/cassandra-all "1.1.6"]
                 [useful "0.8.6"]
                 [commons-pool "1.6"]
                 [com.taoensso/nippy "1.0.0"]
                 [cc.qbits/tardis "0.2.0"]
                 [cc.qbits/knit "0.1.2"]
                 [clj-time "0.4.4"]]
  :profiles {:1.4  {:dependencies [[org.clojure/clojure "1.4.0"]]}
             :1.5  {:dependencies [[org.clojure/clojure "1.5.0-master-SNAPSHOT"]]}
             :dev  {:dependencies [[codox "0.6.1"]]}
             :test {:dependencies []}}

  :min-lein-version "2.0.0"
  :warn-on-reflection true)