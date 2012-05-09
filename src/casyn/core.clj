(ns casyn.core
  (:use [useful.ns :only [alias-ns]]))

(doseq [module '(api client schema ddl codecs cluster.core)]
  (alias-ns (symbol (str "casyn." module))))