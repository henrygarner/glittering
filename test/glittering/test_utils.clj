(ns glittering.test-utils
  (:import [scala Tuple2]))

(defn untuple [^Tuple2 t]
  (let [v (transient [])]
    (conj! v (._1 t))
    (conj! v (._2 t))
    (persistent! v)))

(defn untuple-all [coll]
  (map untuple coll))
