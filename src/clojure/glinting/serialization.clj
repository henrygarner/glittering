(ns glinting.serialization
  (:require [sparkling.serialization :as registrator]
            [glinting.core :as g])
  (:import [org.apache.spark.serializer KryoRegistrator]
           [com.esotericsoftware.kryo Kryo Serializer]
           [org.objenesis.strategy StdInstantiatorStrategy]
           [com.esotericsoftware.kryo.io Output Input]
           [org.apache.spark.graphx Edge]))

(deftype Registrator []
  KryoRegistrator
  (#^void registerClasses [#^KryoRegistrator this #^Kryo kryo]
    (try
      (.setInstantiatorStrategy kryo (StdInstantiatorStrategy.))
       
      (require 'sparkling.serialization)
      (require 'clojure.tools.logging)
      
      (registrator/register-base-classes kryo)
      (registrator/register kryo org.apache.spark.graphx.Edge)
      (registrator/register kryo org.apache.spark.graphx.impl.VertexAttributeBlock)
      
      (catch Exception e
        (RuntimeException. "Failed to register kryo!" e)))))
