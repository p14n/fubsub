(ns p14n.fubsub.consumer
  (:require [p14n.fubsub.common :refer [consumer-head-key-part
                                        consumer-processing-key-part
                                        topic-key-part
                                        processor-status-available]]
            [p14n.fubsub.data :as d]
            [p14n.fubsub.concurrency :as ccy])
  (:import [java.lang Exception]))

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
  "Check for new messages on the given topic/consumer. 
   If new messages are found, put them in the processing queue and notify processors.
   
   Returns true if the number of messages was less than capacity, indicating 'complete'."

  [{:keys [put-all notify-processors tx-wrapper threads] :as ctx}
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
                              :msgs msgs}))
    (if (<  (count msgs) threads) true false)))

(defn consumer-loop [{:keys [consumer-poll-ms error-log info-log]}
                     consumer-running?
                     watch-semaphore
                     topic-check-function
                     set-watch-function]
  (while (.get consumer-running?)
    (info-log "Waiting for messages")
    (ccy/acquire-semaphore watch-semaphore consumer-poll-ms)
    (loop []
      (when (.get consumer-running?)
        (info-log "Fetching messages")
        (let [complete (try (topic-check-function)
                            (catch Exception e
                              (error-log "Failed to check topic" e)
                              true))]
          (if complete
            (try (set-watch-function)
                 (catch Exception e (error-log "Failed to set watch" e)))
            (recur)))))))



