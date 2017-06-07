(ns glittering.utils
  (:import [sparkling.scalaInterop ScalaFunction0]))

(defn or-nil [x]
  (.getOrElse x (ScalaFunction0. (fn [] nil))))
