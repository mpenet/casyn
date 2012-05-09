(defproject casyn "0.1.1-SNAPSHOT"
  :description "Async Thrift based Cassandra Client"
  :dependencies [[org.clojure/clojure "1.4.0"]
                 [org.clojure/core.incubator "0.1.0"]
                 [lamina "0.5.0-alpha3"]
                 [org.apache.cassandra/cassandra-all "1.1.0"]
                 [tron "0.5.3"]
                 [useful "0.8.0"]
                 [commons-pool "1.6"]]

  :dev-dependencies [[codox "0.6.1"]]
  :warn-on-reflection true)