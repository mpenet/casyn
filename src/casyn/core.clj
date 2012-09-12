(ns casyn.core
  (:require
   [useful.ns :refer [alias-ns]]))

(doseq [module '(api client schema ddl codecs codecs.composite cluster.core)]
  (alias-ns (symbol (str "casyn." module))))