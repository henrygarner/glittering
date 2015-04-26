(ns glimmering.api
  (:require [sparkling.scalaInterop :as si]
            [sparkling.function :refer [function2]]
            [t6.from-scala.core :refer ($ $$) :as $])
  (:import [org.apache.spark.graphx Edge EdgeDirection Graph GraphOps Graph$]
           [scala.collection Iterator]
           [scala.reflect ClassTag]
           [scala.Predef$$eq$colon$eq$/MODULE$]
           [glimmering.scalaInterop ScalaFunction2 ScalaFunction3]))

(defn edge [from to weight]
  ($ Edge from to weight))

(defn edges [graph]
  (.edges graph))

(defn vertices [graph]
  (.vertices graph))

(defn triplets [graph]
  (.triplets graph))

(defn map-edges [graph f]
  (.mapEdges graph (new sparkling.scalaInterop.ScalaFunction1 f) si/OBJECT-CLASS-TAG))

(defn inner-join [rdd other f]
  (.innerJoin rdd other
              (new glimmering.scalaInterop.ScalaFunction3 f)
              si/OBJECT-CLASS-TAG
              si/OBJECT-CLASS-TAG))

(defn outer-join-vertices [graph vertices f]
  (.outerJoinVertices graph vertices
                      (new glimmering.scalaInterop.ScalaFunction3 f)
                      si/OBJECT-CLASS-TAG
                      si/OBJECT-CLASS-TAG
                      nil))

(defn map-vertices [graph f]
  (.mapVertices graph
                (new glimmering.scalaInterop.ScalaFunction2 f)
                ($ ClassTag java.lang.Object)
                nil))

(defn map-reduce-triplets [graph send merge]
  (.mapReduceTriplets graph
                      (new sparkling.scalaInterop.ScalaFunction1 send)
                      (new glimmering.scalaInterop.ScalaFunction2 merge)
                      ($/option ($/tuple (.vertices graph)
                                         (EdgeDirection/Out)))
                      si/OBJECT-CLASS-TAG))

(defn aggregate-messages [graph send merge]
  (.aggregateMessages graph
                      (new sparkling.scalaInterop.ScalaFunction1 send)
                      (new glimmering.scalaInterop.ScalaFunction2 merge)
                      (.aggregateMessages$default$3 graph)
                      si/OBJECT-CLASS-TAG))

(defn graph [vertices edges default]
  ($ Graph
     (.rdd vertices)
     (.rdd edges) default
     _ _ si/OBJECT-CLASS-TAG si/OBJECT-CLASS-TAG))

(defn graph-from-edges [edges default]
  (.fromEdges Graph$/MODULE$ (.rdd edges) default
              (.fromEdges$default$3 Graph$/MODULE$)
              (.fromEdges$default$4 Graph$/MODULE$)
              si/OBJECT-CLASS-TAG
              si/OBJECT-CLASS-TAG))


(defn page-rank [graph tol reset-prob]
  (.pageRank (.ops graph) tol reset-prob))

(defn connected-components [graph]
  (.connectedComponents (.ops graph)))

(defn triangle-count [graph]
  (.triangleCount (.ops graph)))

(defn strongly-connected-components [graph iterations]
  (.stronglyConnectedComponents (.ops graph) iterations))
