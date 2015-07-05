(ns glittering.utils
  (:require [t6.from-scala.core :as $ :refer [$]]))

(defn or-nil [x]
  ($ x getOrElse ($/fn [] nil)))
