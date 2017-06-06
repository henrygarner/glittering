(ns glittering.cc-test
  (:require [glittering.destructuring :as d]
            [glittering.core :as g]
            [glittering.generators :as gen]
            [glittering.test-utils :refer [untuple-all]]
            [sparkling.conf :as conf]
            [sparkling.core :as spark]
            [clojure.test :refer :all]))

(defn simple-bipartite [n]
  (let [part1 (for [u (range n)]
                (g/edge u (+ u n) 1))]
    (prn part1)
    part1))

(defn two-cliques [n]
  (let [clique1 (for [u (range n)
                      v (range n)]
                  (g/edge u v 1))
        clique2 (for [u (range n)
                      v (range n)]
                  (g/edge (+ u n) (+ v n) 1))]
    (concat clique1 clique2 [(g/edge 0 n 1)])))

(defn connected-component
  []
  (spark/with-context sc (-> (g/conf)
                             (conf/master "local[*]")
                             (conf/app-name "connected-component-test"))
    (let [edges (spark/parallelize sc (simple-bipartite 5))
          components (->> (g/graph-from-edges edges 1)
                          (g/connected-components)
                          (g/vertices)
                          (spark/collect)
                          (vec)
                          (untuple-all)
                          (group-by second)
                          )]
      (prn components))))
