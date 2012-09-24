(ns casyn.types
  "Internal data types for cassandra results"
  (:import
   [org.apache.cassandra.utils ByteBufferUtil]
   [java.nio ByteBuffer]))

(defrecord Column [name value ttl timestamp])
(defrecord CounterColumn [name value])
(defrecord SuperColumn [name columns])
(defrecord CounterSuperColumn [name columns])

(defrecord KeySlice [row columns])
(defrecord CqlRow [row columns])
(defrecord CqlResult [num type rows])
(defrecord CqlPreparedResult [item-id count variable-names variable-types])

(defprotocol ThriftDecodable
  (thrift->casyn [v] "Transforms a thrift type to clojure friendly type"))

;; extend-protocol messes things up during expansion
(extend-type (Class/forName "[Lorg.apache.cassandra.thrift.ColumnOrSuperColumn;")
  ThriftDecodable
  (thrift->casyn [cs]
    (map thrift->casyn cs)))

(extend-type (Class/forName "[Lorg.apache.cassandra.thrift.KeySlice;")
  ThriftDecodable
  (thrift->casyn [kss]
    (into-array (map thrift->casyn kss))))

(extend-protocol ThriftDecodable

  ByteBuffer
  (thrift->casyn [b]
    (ByteBufferUtil/getArray b))

  java.util.ArrayList
  (thrift->casyn [a]
    (when (> (count a) 0)
      (thrift->casyn (into-array a))))

  java.util.Map
  (thrift->casyn [a]
    (reduce
     (fn [m [k v]]
       (assoc m
         (thrift->casyn k)
         (thrift->casyn v)))
     (array-map)
     a))

  org.apache.cassandra.thrift.Column
  (thrift->casyn [c]
    (Column. (.getName c)
             (.getValue c)
             (.getTtl c)
             (.getTimestamp c)))

  org.apache.cassandra.thrift.CounterColumn
  (thrift->casyn [c]
    (CounterColumn. (.getName c)
                    (.getValue c)))

  org.apache.cassandra.thrift.ColumnOrSuperColumn
  (thrift->casyn [c]
    (thrift->casyn (or (.column c)
                       (.counter_column c)
                       (.super_column c)
                       (.counter_super_column c))))

  org.apache.cassandra.thrift.SuperColumn
  (thrift->casyn [sc]
    (SuperColumn. (.getName sc)
                  (thrift->casyn (.getColumns sc))))


  org.apache.cassandra.thrift.CounterSuperColumn
  (thrift->casyn [sc]
    (CounterSuperColumn. (.getName sc)
                         (thrift->casyn (.getColumns sc))))

  org.apache.cassandra.thrift.KeySlice
  (thrift->casyn [ks]
    (KeySlice. (.getKey ks)
               (map thrift->casyn (.getColumns ks))))

  org.apache.cassandra.thrift.CqlRow
  (thrift->casyn [r]
    (CqlRow. (.getKey r)
             (map thrift->casyn (.getColumns r))))

  org.apache.cassandra.thrift.CqlResult
  (thrift->casyn [r]
    (-> (CqlResult. (.getNum r)
                    (.getType r)
                    (map thrift->casyn (.getRows r)))
        (with-meta {:schema (.getSchema r)})))

  org.apache.cassandra.thrift.CqlPreparedResult
  (thrift->casyn [c]
    (CqlPreparedResult. (.getItemId c)
                        (.getCount c)
                        (.getVariable_names c)
                        (.getVariable_types c)))

  Object
  (thrift->casyn [v] v)

  nil
  (thrift->casyn [v] v))
