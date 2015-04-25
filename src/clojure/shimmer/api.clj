(ns shimmer.api
  (:require [sparkling.scalaInterop :as si]
            [sparkling.function :refer [function2]]
            [t6.from-scala.core :refer ($ $$) :as $])
  (:import [org.apache.spark.graphx Edge EdgeDirection Graph GraphOps Graph$]
           [scala.collection Iterator]
           [scala.reflect ClassTag]
           [scala.Predef$$eq$colon$eq$/MODULE$]
           [shimmer.scalaInterop ScalaFunction2 ScalaFunction3]))

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
              (new shimmer.scalaInterop.ScalaFunction3 f)
              si/OBJECT-CLASS-TAG
              si/OBJECT-CLASS-TAG))

(defn outer-join-vertices [graph vertices f]
  (.outerJoinVertices graph vertices
                      (new shimmer.scalaInterop.ScalaFunction3 f)
                      si/OBJECT-CLASS-TAG
                      si/OBJECT-CLASS-TAG
                      nil))

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

(defn aggregate-messages [graph send merge]
  (.aggregateMessages graph
                      (new sparkling.scalaInterop.ScalaFunction1 send)
                      (new shimmer.scalaInterop.ScalaFunction2 merge)
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

(defn pregel [graph init max vf sf mf]
  (println "PREGEL ##################")
  (let [
        dir (EdgeDirection/Either)
        v (new shimmer.scalaInterop.ScalaFunction3 vf)
        s (new sparkling.scalaInterop.ScalaFunction1 sf)
        m (new shimmer.scalaInterop.ScalaFunction2 mf)
        class-tag ($ ClassTag java.lang.Object)]
    (.pregel (.ops graph) init max dir v s m class-tag)))


(defn pregel-impl [graph init max-iterations vf sf mf]
  (let [dir (EdgeDirection/Either)
        g (map-vertices graph (fn [vid attr]
                                (vf vid attr init)))]
    (loop [g g
           messages (aggregate-messages g sf mf)
           i 0]
      (if (and (> (.count messages) 0)
               (< i max-iterations))
        (let [new-verts (.cache (inner-join (vertices g) messages vf))
              old-g g
              g (.cache (outer-join-vertices g new-verts
                                             (fn [vid old new-opt]
                                               ($ new-opt getOrElse ($/fn [] old)))))
              old-messages messages
              messages (.cache (aggregate-messages g sf mf))]
          (println "Pregel iteration: " i "messages count: " (.count messages))
          (.unpersist old-messages false)
          (.unpersist new-verts false)
          (.unpersistVertices old-g false)
          (.unpersist (edges old-g) false)
          (recur g messages (inc i)))
        g))))

(defn page-rank [graph tol reset-prob]
  (.pageRank (.ops graph) tol reset-prob))

(defn connected-components [graph]
  (.connectedComponents (.ops graph)))

(defn triangle-count [graph]
  (.triangleCount (.ops graph)))

(defn strongly-connected-components [graph iterations]
  (.stronglyConnectedComponents (.ops graph) iterations))
