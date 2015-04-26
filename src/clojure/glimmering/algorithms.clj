(ns glimmering.algorithms)

(defn page-rank [tol reset-prob graph]
  (.pageRank (.ops graph) tol reset-prob))

(defn connected-components [graph]
  (.connectedComponents (.ops graph)))

(defn triangle-count [graph]
  (.triangleCount (.ops graph)))

(defn strongly-connected-components [iterations graph]
  (.stronglyConnectedComponents (.ops graph) iterations))
