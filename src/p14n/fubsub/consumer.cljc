(ns p14n.fubsub.consumer)

(def consumer-head-key-part "ch")
(def consumer-processing-key-part "cp")
(def topic-key-part "tp")

(def processor-status-available "a")
(def processor-status-processing "p")

(defn ordered-msgs->consumer-head-tx [topic consumer msgs]
  (let [head (-> msgs last first last)]
    [[consumer-head-key-part topic consumer] head]))

(defn topic-msgs->consumer-processing-txs [topic consumer node msgs]
  (mapv (fn [[k _]]
          [[consumer-processing-key-part topic consumer (last k)] [processor-status-available node]])
        msgs))

(defn select-new-messages [{:keys [topic
                                   consumer
                                   node
                                   get-value
                                   get-range-after
                                   threads]}]
  (let [head (get-value [consumer-head-key-part topic consumer])
        msgs (get-range-after [topic-key-part topic head] threads)]
    (concat [(ordered-msgs->consumer-head-tx topic consumer msgs)]
            (topic-msgs->consumer-processing-txs topic consumer node msgs))))
