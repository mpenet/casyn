(defproject casyn "0.1.0-SNAPSHOT"
  :description "Async Thrift based Cassandra Client"
  :dependencies [[org.clojure/clojure "1.3.0"]
                 [org.clojure/core.incubator "0.1.0"]
                 [lamina "0.5.0-SNAPSHOT"]
                 [org.apache.cassandra/cassandra-all "1.0.9"]
                 [tron "0.5.3"]
                 [commons-pool "1.6"]]

  :dev-dependencies [[codox "0.6.1"]]
  :warn-on-reflection true)