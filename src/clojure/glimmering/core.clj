(ns glimmering.core
  (:require [sparkling.scalaInterop :as si]
            [sparkling.conf :as conf]
            [t6.from-scala.core :refer [$]])
  (:import [glimmering.scalaInterop ScalaFunction2 ScalaFunction3]
           [sparkling.scalaInterop ScalaFunction1]
           [scala.reflect ClassTag]
           [org.apache.spark.graphx Pregel Edge Graph$]
           [org.apache.spark SparkConf]))

(defn set-glimmering-registrator [conf]
  (conf/set conf "spark.kryo.registrator" "glimmering.serialization.Registrator"))

(defn conf []
  (-> (SparkConf.)
      (conf/set "spark.serializer" "org.apache.spark.serializer.KryoSerializer")
      (set-glimmering-registrator))) 

(defn edge [from to weight]
  ($ Edge from to weight))

(defn edges [graph]
  (.edges graph))

(defn vertices [graph]
  (.vertices graph))

(defn map-vertices [graph f]
  (.mapVertices graph
                (new ScalaFunction2 f)
                ($ ClassTag java.lang.Object)
                nil))

(defn inner-join [rdd other f]
  (.innerJoin rdd other
              (new ScalaFunction3 f)
              si/OBJECT-CLASS-TAG
              si/OBJECT-CLASS-TAG))

(defn outer-join-vertices [graph vertices f]
  (.outerJoinVertices graph vertices
                      (new ScalaFunction3 f)
                      si/OBJECT-CLASS-TAG
                      si/OBJECT-CLASS-TAG
                      nil))

(defn aggregate-messages [graph send merge]
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
