(ns glimmering.pregel-test
  (:require [glimmering.pregel :as p]
            [glimmering.core :as g]
            [glimmering.test-utils :refer [untuple-all]]
            [sparkling.conf :as conf]
            [sparkling.core :as spark]
            [clojure.test :refer :all]))

(deftest label-propagation
  (let [vfn (fn [vid attr message]
              (if (empty? message)
                attr
                (ffirst (sort-by second > message))))
        sfn (p/message-fn
             (fn [{:keys [src-attr dst-attr]}]
               {:src [{dst-attr 1}]
                :dst [{src-attr 1}]}))
        mfn (fn [a b]
              (merge-with + a b))
        init {}
        n 5
        max 10
        clique1 (for [u (range n)
                      v (range n)]
                  (g/edge u v 1))
        clique2 (for [u (range n)
                      v (range n)]
                  (g/edge (+ u n) (+ v n) 1))]
    (spark/with-context sc (-> (g/conf)
                               (conf/master "local[*]")
                               (conf/app-name "pregel-test"))
      (let [edges (spark/parallelize sc (concat clique1 clique2 [(g/edge 0 n 1)]))
            labels (->> (g/graph-from-edges edges 1)
                        (g/map-vertices (fn [vid attr] vid)) 
                        (p/pregel init max vfn sfn mfn)
                        (g/vertices)
                        (spark/collect)
                        (vec)
                        (untuple-all)
                        (group-by second))]
        (testing
            "returns two cliques"
          (is (= 2 (-> labels keys count))))))))
