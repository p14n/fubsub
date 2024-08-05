(ns p14n.fubsub.consumer
  (:require [p14n.fubsub.common :refer [consumer-head-key-part
                                        consumer-processing-key-part
                                        topic-key-part
                                        processor-status-available]]))

(defn ordered-msgs->consumer-head-tx [topic consumer msgs]
  (let [[_ _ msg-id] (-> msgs last first)]
    [[consumer-head-key-part topic consumer] msg-id]))

(defn topic-msgs->consumer-processing-txs [topic consumer node msgs]
  (mapv (fn [[[_ _ msg-id key] _]]
          [[consumer-processing-key-part topic consumer msg-id key] [processor-status-available node]])
        msgs))

(defn select-new-messages
  [{:keys [get-range-after threads get-value]}
   {:keys [topic consumer node]}]
  (let [head (get-value [consumer-head-key-part topic consumer])
        msgs (get-range-after [topic-key-part topic head] threads)]
    (concat [(ordered-msgs->consumer-head-tx topic consumer msgs)]
            (topic-msgs->consumer-processing-txs topic consumer node msgs))))
