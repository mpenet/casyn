(ns qbits.casyn
  "Alias most of the lib for convenience:
* qbits.casyn.client
* qbits.casyn.api
* qbits.casyn.schema
* qbits.casyn.ddl
* qbits.casyn.codecs
* qbits.casyn.codecs.composite
* qbits.casyn.cluster.core
"
  (:require
   [useful.ns :refer [alias-ns]]))

(doseq [module '(client api schema ddl codecs codecs.composite ;; codecs.collection
                        cluster cluster.core)]
  (alias-ns (symbol (str "qbits.casyn." module))))