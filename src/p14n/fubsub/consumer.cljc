(ns p14n.fubsub.consumer
  (:require [p14n.fubsub.common :refer [consumer-head-key-part
                                        consumer-processing-key-part
                                        topic-key-part
                                        processor-status-available]]
            [p14n.fubsub.data :as d]))

(defn ordered-msgs->consumer-head-tx [topic consumer msgs]
  (let [[_ _ msg-id] (-> msgs last first)]
    [[consumer-head-key-part topic consumer] [msg-id]]))

(defn topic-msgs->consumer-processing-txs [{:keys [current-timestamp-function]}
                                           {:keys [topic consumer node msgs]}]
  (mapv (fn [[[_ _ msg-id key] _]]
          [[consumer-processing-key-part topic consumer msg-id key] [processor-status-available node (current-timestamp-function)]])
        msgs))

(defn select-new-messages
  [{:keys [get-range-after threads get-value] :as ctx}
   {:keys [topic consumer]}]
  (let [[head] (get-value ctx [consumer-head-key-part topic consumer])]
    (get-range-after ctx [topic-key-part topic (or head "")] threads)))

(defn select-new-messages-tx
  [ctx {:keys [topic consumer msgs] :as data}]
  (concat [(ordered-msgs->consumer-head-tx topic consumer msgs)]
          (topic-msgs->consumer-processing-txs ctx data)))

(defn topic-check
  [{:keys [put-all notify-processors tx-wrapper] :as ctx}
   {:keys [topic consumer node]}]
  (let [msgs (tx-wrapper ctx
                         #(let [ctx-tx (d/ctx-with-tx ctx %)
                                msgs (select-new-messages ctx-tx
                                                          {:topic topic
                                                           :consumer consumer
                                                           :node node})]
                            (when (seq msgs)
                              (put-all ctx-tx (select-new-messages-tx ctx {:topic topic
                                                                           :consumer consumer
                                                                           :node node
                                                                           :msgs msgs})))
                            msgs))]
    (when (seq msgs)
      (notify-processors ctx {:topic topic
                              :consumer consumer
                              :node node
                              :msgs msgs}))))


;Use semaphore
;consumer waits on (.tryAquire timeout)
;watch calls release on semaphore, releasing consumer
;consumer performs topic check
;when complete, the consumer
; - cycles if it got its full batch size last time 
; - or checks if there is still an active watch (AtomicBoolean). If not, one is set on topic head key
;when the watch is triggered, it drains the semaphoe, then releases
;processors get their own v thread, locking on the existing key lock to avoid thrashing