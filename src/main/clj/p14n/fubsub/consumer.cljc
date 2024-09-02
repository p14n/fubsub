(ns p14n.fubsub.consumer
  (:require [p14n.fubsub.common :refer [consumer-head-key-part
                                        consumer-processing-key-part
                                        topic-key-part
                                        processor-status-available]]
            [p14n.fubsub.concurrency :as ccy]
            [p14n.fubsub.util :as u]
            [p14n.fubsub.logging :as log])
  (:import [java.lang Exception]))

(defn ordered-msgs->consumer-head-tx [ctx topic consumer msgs]
  (let [[_ _ msg-id] (->> msgs last first (u/key-without-subspace ctx))]
    [[consumer-head-key-part topic consumer] [msg-id]]))

(defn p> [x] (println x) x)

(defn topic-msgs->consumer-processing-txs [{:keys [current-timestamp-function handlers] :as ctx}
                                           {:keys [topic consumer node msgs]}]
  (let [handler-names (keys (u/handlers-by-name handlers topic))]
    (->> msgs
         (mapv (fn [[msg-keys _]]
                 (let [[_ _ msg-id key] (u/key-without-subspace ctx msg-keys)]
                   (mapv #(do [[consumer-processing-key-part topic consumer msg-id key %]
                               [processor-status-available node (current-timestamp-function)]])
                         handler-names))))
         (apply concat))))

(defn select-new-messages
  [{:keys [get-range-after threads get-value] :as ctx}
   {:keys [topic consumer]}]
  (let [[head] (get-value ctx [consumer-head-key-part topic consumer])]
    (get-range-after ctx [topic-key-part topic (or head "")] threads)))

(defn select-new-messages-tx
  [ctx {:keys [topic consumer msgs] :as data}]
  (concat [(ordered-msgs->consumer-head-tx ctx topic consumer msgs)]
          (topic-msgs->consumer-processing-txs ctx data)))

(defn topic-check
  "Check for new messages on the given topic/consumer. 
   If new messages are found, put them in the processing queue and notify processors.
   
   Returns true if the number of messages was less than capacity, indicating 'complete'."

  [{:keys [put-all notify-processors tx-wrapper threads] :as ctx}
   {:keys [topic consumer node]}]
  (let [msgs (tx-wrapper ctx
                         #(let [ctx-tx (u/ctx-with-tx ctx %)
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

(defn consumer-loop [{:keys [consumer-poll-ms logger
                             consumer-running? watch-semaphore]}
                     topic-check-function
                     set-watch-function]
  (log/info logger :consumer/consumer-loop "Waiting for messages")
  (when (ccy/acquire-semaphore watch-semaphore consumer-poll-ms)
    (loop []
      (when (.get consumer-running?)
        (log/info logger :consumer/consumer-loop "Fetching messages")
        (let [complete (try (topic-check-function)
                            (catch Exception e
                              (log/error logger :consumer/consumer-loop "Failed to check topic" e)
                              true))]
          (if complete
            (try (set-watch-function)
                 (catch Exception e (log/error logger :consumer/consumer-loop "Failed to set watch" e)))
            (recur)))))))



