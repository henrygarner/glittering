(ns glinting.core
  (:require [sparkling.scalaInterop :as si]
            [sparkling.conf :as conf]
            [t6.from-scala.core :refer [$]])
  (:import [glinting.scalaInterop ScalaFunction2 ScalaFunction3]
           [sparkling.scalaInterop ScalaFunction1]
           [scala.reflect ClassTag]
           [org.apache.spark.graphx Pregel Edge Graph$]
           [org.apache.spark SparkConf]))

(defn set-glinting-registrator [conf]
  (conf/set conf "spark.kryo.registrator" "glinting.serialization.Registrator"))

(defn conf []
  (-> (SparkConf.)
      (conf/set "spark.serializer" "org.apache.spark.serializer.KryoSerializer")
      (set-glinting-registrator))) 

(defn edge [from to weight]
  ($ Edge from to weight))

(defn edges [graph]
  (.edges graph))

(defn vertices [graph]
  (.vertices graph))

(defn map-vertices [f graph]
  (.mapVertices graph
                (new ScalaFunction2 f)
                ($ ClassTag java.lang.Object)
                nil))

(defn inner-join [f other rdd]
  (.innerJoin rdd other
              (new ScalaFunction3 f)
              si/OBJECT-CLASS-TAG
              si/OBJECT-CLASS-TAG))

(defn outer-join-vertices [f vertices graph]
  (.outerJoinVertices graph vertices
                      (new ScalaFunction3 f)
                      si/OBJECT-CLASS-TAG
                      si/OBJECT-CLASS-TAG
                      nil))

(defn aggregate-messages [send merge graph]
  (.aggregateMessages graph
                      (new ScalaFunction1 send)
                      (new ScalaFunction2 merge)
                      (.aggregateMessages$default$3 graph)
                      si/OBJECT-CLASS-TAG))

(defn graph-from-edges [edges default]
  (.fromEdges Graph$/MODULE$ (.rdd edges) default
              (.fromEdges$default$3 Graph$/MODULE$)
              (.fromEdges$default$4 Graph$/MODULE$)
              si/OBJECT-CLASS-TAG
              si/OBJECT-CLASS-TAG))
