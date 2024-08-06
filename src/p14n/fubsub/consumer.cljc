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
  [{:keys [get-range-after threads get-value] :as ctx}
   {:keys [topic consumer]}]
  (let [head (get-value ctx [consumer-head-key-part topic consumer])]
    (get-range-after ctx [topic-key-part topic head] threads)))

(defn select-new-messages-tx
  [_ {:keys [topic consumer node msgs]}]
  (concat [(ordered-msgs->consumer-head-tx topic consumer msgs)]
          (topic-msgs->consumer-processing-txs topic consumer node msgs)))

(defn topic-check
  [{:keys [put-all notify-processors] :as ctx}
   {:keys [topic consumer node]}]
  (let [msgs (select-new-messages ctx {:topic topic
                                       :consumer consumer
                                       :node node})]
    (when (seq msgs)
      (put-all ctx (select-new-messages-tx ctx {:topic topic
                                                :consumer consumer
                                                :node node
                                                :msgs msgs}))
      (notify-processors ctx {:topic topic
                              :consumer consumer
                              :node node
                              :msgs msgs}))))


