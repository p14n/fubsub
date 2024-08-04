(ns p14n.fubsub.consumer)

(def consumer-head-key-part "ch")
(def consumer-processing-key-part "cp")
(def topic-key-part "tp")

(def processor-status-available "a")
(def processor-status-processing "p")

(defn ordered-msgs->consumer-head-tx [topic consumer msgs]
  (let [[_ _ msg-id] (-> msgs last first)]
    [[consumer-head-key-part topic consumer] msg-id]))

(defn topic-msgs->consumer-processing-txs [topic consumer node msgs]
  (mapv (fn [[[_ _ msg-id key] _]]
          [[consumer-processing-key-part topic consumer msg-id key] [processor-status-available node]])
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
