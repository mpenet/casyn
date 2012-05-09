(ns casyn.core
  (:require [useful.ns :refer [alias-ns]]))

(doseq [module '(api client schema ddl codecs cluster.core)]
  (alias-ns (symbol (str "casyn." module))))