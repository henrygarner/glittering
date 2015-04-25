(ns shimmer.api
  (:require [sparkling.scalaInterop :as si]
            [sparkling.function :refer [function2]]
            [t6.from-scala.core :refer ($ $$) :as $])
  (:import [org.apache.spark.graphx Edge EdgeDirection Graph GraphOps]
           [scala.collection Iterator]
           [scala.reflect ClassTag]
           [scala.Predef$$eq$colon$eq$/MODULE$]
           [shimmer.scalaInterop ScalaFunction2 ScalaFunction3]))

(defn edge [from to weight]
  (new Edge from to weight))

(defn edges [graph]
  (.edges graph))

(defn vertices [graph]
  (.vertices graph))

(defn triplets [graph]
  (.triplets graph))

(defn map-edges [graph f]
  (.mapEdges graph (new sparkling.scalaInterop.ScalaFunction1 f) si/OBJECT-CLASS-TAG))

(defn map-vertices [graph f]
  (.mapVertices graph
                (new shimmer.scalaInterop.ScalaFunction2 f)
                ($ ClassTag java.lang.Object)
                nil))

(defn map-reduce-triplets [graph send merge]
  (.mapReduceTriplets graph
                      (new sparkling.scalaInterop.ScalaFunction1 send)
                      (new shimmer.scalaInterop.ScalaFunction2 merge)
                      ($/option ($/tuple (.vertices graph)
                                         (EdgeDirection/Out)))
                      si/OBJECT-CLASS-TAG))

(defn graph [vertices edges]
  ($ Graph
     (.rdd vertices)
     (.rdd edges)
     _ _ _ si/OBJECT-CLASS-TAG si/OBJECT-CLASS-TAG))

(defn pregel [graph init vf sf mf]
  (println "PREGEL ##################")
  (let [max 10
        dir (EdgeDirection/Either)
        v (new shimmer.scalaInterop.ScalaFunction3 vf)
        s (new sparkling.scalaInterop.ScalaFunction1 sf)
        m (new shimmer.scalaInterop.ScalaFunction2 mf)
        class-tag ($ ClassTag java.lang.Object)]
    (.pregel (.ops graph) init max dir v s m class-tag)))

(defn page-rank [graph tol reset-prob]
  (.pageRank (.ops graph) tol reset-prob))

(defn connected-components [graph]
  (.connectedComponents (.ops graph)))

(defn triangle-count [graph]
  (.triangleCount (.ops graph)))

(defn strongly-connected-components [graph iterations]
  (.stronglyConnectedComponents (.ops graph) iterations))
