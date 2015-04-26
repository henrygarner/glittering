(ns glimmering.generators
  (:import [org.apache.spark.graphx.util GraphGenerators$]))

(defn log-normal [conf vertices-count {:keys [partitions-count mu sigma seed]
                                       :or {partitions-count 0
                                            mu 4.0 sigma 1.3 seed -1}}]
  (.logNormalGraph GraphGenerators$/MODULE$ (.sc conf) vertices-count partitions-count mu sigma seed))
