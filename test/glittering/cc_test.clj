(ns glittering.cc-test
  (:require [glittering.destructuring :as d]
            [glittering.core :as g]
            [glittering.generators :as gen]
            [glittering.test-utils :refer [untuple-all]]
            [sparkling.conf :as conf]
            [sparkling.core :as spark]
            [sparkling.destructuring :as s-de]
            [clojure.test :refer :all]))

(def table-a
  [{:amp-id "5fa91050-8a56-4285-8514-2cab0f95f4d3" :members [1 2 3]}
   {:amp-id "eb0e2e5a-750a-40f7-b0de-8d7e90e82ffb" :members [4 5]}
   {:amp-id "95a4df24-e200-42ca-bafc-f99bd4b22b34" :members [13 14]}])

(def table-b
  [{:amp-id "c566be3f-cfca-4b60-a4cf-4b00eaeee722" :members [6 7 8]}
   {:amp-id "3686d0d5-06f8-4977-90ad-dc718fcd3f73" :members [9 10]}
   {:amp-id "707477b7-6c2a-42c7-87ee-c96d38342189" :members [11 12]}])

(def matching-table
  [{:lhs "5fa91050-8a56-4285-8514-2cab0f95f4d3"
    :rhs "3686d0d5-06f8-4977-90ad-dc718fcd3f73"}
   {:lhs "eb0e2e5a-750a-40f7-b0de-8d7e90e82ffb"
    :rhs "c566be3f-cfca-4b60-a4cf-4b00eaeee722"}])

(defn uuid->long
  [id-str]
  (.getLeastSignificantBits (java.util.UUID/fromString id-str)))

(defn tuplate-table
  [rows]
  (map (fn [row] (spark/tuple (uuid->long (:amp-id row)) row)) rows))

(defn matching->edges
  [rows]
  (map (fn [row] (g/edge (uuid->long (:lhs row)) (uuid->long (:rhs row)) nil)) rows))

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
