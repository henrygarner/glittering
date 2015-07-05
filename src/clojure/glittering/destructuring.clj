(ns glittering.destructuring)

(defn message-fn [f]
  (fn [ctx]
    (let [incoming {:src-id   (.srcId ctx)
                    :src-attr (.srcAttr ctx)
                    :dst-id   (.dstId ctx)
                    :dst-attr (.dstAttr ctx)
                    :attr     (.attr ctx)}
          outgoing (f incoming)]
      (doseq [[dest message] outgoing]
        (case dest
          :src (.sendToSrc ctx message)
          :dst (.sendToDst ctx message))))))



#_(defn attr-attr-fn [f]
    (fn [a b]
      (f (.attr a) (.attr b))))
