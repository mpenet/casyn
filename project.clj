(defproject cc.qbits/casyn "1.0.0"
  :description "Cassandra client with support for asynchronous operations"
  :url "https://github.com/mpenet/casyn"
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}
  :dependencies [[org.clojure/clojure "1.4.0"]
                 [lamina "0.5.0-beta10"]
                 [org.apache.cassandra/cassandra-all "1.2.1"]
                 [useful "0.8.8"]
                 [commons-pool "1.6"]
                 [com.taoensso/nippy "1.1.0"]
                 [cc.qbits/tardis "0.3.1"]
                 [cc.qbits/knit "0.2.1"]
                 [clj-time "0.4.4"]]
  :profiles {:1.4  {:dependencies [[org.clojure/clojure "1.4.0"]]}
             :1.5  {:dependencies [[org.clojure/clojure "1.5.0-master-SNAPSHOT"]]}
             :test {:dependencies []}}

  :codox {:src-dir-uri "https://github.com/mpenet/casyn/blob/master"
          :src-linenum-anchor-prefix "L"}

  :min-lein-version "2.0.0"
  :warn-on-reflection true)