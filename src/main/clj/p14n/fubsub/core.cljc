(ns p14n.fubsub.core
  (:require [p14n.fubsub.consumer :as consumer]
            [p14n.fubsub.processor :as processor]
            [p14n.fubsub.common :as common :refer [topic-head-key-part
                                                   processor-status-available
                                                   processor-status-processing]]
            [p14n.fubsub.data :as d]
            [p14n.fubsub.util :as u]
            [p14n.fubsub.concurrency :as ccy]
            [p14n.fubsub.logging :as log])
  (:import [java.util UUID]
           [java.lang Exception]))

(defn lock-and-process-message [{:keys [logger] :as ctx}
                                {:keys [topic consumer key] :as data}]
  (let [lock-key (str topic "/" consumer "/" key)]
    (log/info logger :core/lock-and-process-message (str "Running process" lock-key))
    (ccy/acquire-lock lock-key)
    (log/info logger :core/lock-and-process-message (str "Acquired lock " lock-key))
    (try
      (processor/process-message ctx data)
      (catch Exception e (log/error logger :core/lock-and-process-message  "Error in processor" e))
      (finally (ccy/release-lock lock-key)))))


(defn find-resubmitable [{:keys [resubmit-available-ms resubmit-processing-ms
                                 current-timestamp-function put-all logger] :as ctx
                          :or {resubmit-available-ms 2000}} topic consumer node]
  (let [reprocess-available-threshold (current-timestamp-function  (u/now-minus-ms resubmit-available-ms))
        reprocess-processing-threshold (when resubmit-processing-ms
                                         (current-timestamp-function (u/now-minus-ms resubmit-processing-ms)))
        msgs (processor/get-all-processing ctx {:topic topic
                                                :consumer consumer})
        available (->> msgs
                       (filter (fn [[_ [status _ timestamp]]]
                                 (and (= status processor-status-available)
                                      (u/first-timestamp-is-earliest timestamp reprocess-available-threshold)))))
        processing (some->> (when reprocess-processing-threshold msgs)
                            (filter (fn [[_ [status _ timestamp]]]
                                      (and (= status processor-status-processing)
                                           (u/first-timestamp-is-earliest timestamp reprocess-processing-threshold)))))
        to-resubmit (->> (concat available processing)
                         (map #(->> % first (take-last 3) (vec))))]
    (when (seq processing)
      (put-all ctx (->> processing
                        (map (fn [[k _]] [(u/key-without-subspace ctx k)
                                          [processor-status-available node (current-timestamp-function)]])))))
    (when (seq to-resubmit)
      (log/info logger :core/find-resubmitable {:desc "Resubmitting"
                                                :to-resubmit to-resubmit}))
    to-resubmit))

(defn resubmit-abandoned
  [{:keys [tx-wrapper] :as ctx}
   {:keys [topic consumer node] :as data}
   resubmit-function]
  (let [to-resubmit (tx-wrapper ctx #(find-resubmitable (u/ctx-with-tx ctx %) topic consumer node))]
    (when (seq to-resubmit)
      (resubmit-function ctx data to-resubmit))))


(defn- notify-processors-async
  [{:keys [handlers] :as ctx}
   {:keys [topic consumer node msgs]}]
  (doseq [msg msgs]
    (doseq [handler (get handlers topic)]
      (when handler
        (let [msg-key (u/key-without-subspace ctx (first msg))
              [_ _ messageid key] msg-key]
          (ccy/run-async
           #(lock-and-process-message ctx {:topic topic
                                           :consumer consumer
                                           :node node
                                           :key key
                                           :messageid messageid
                                           :handler handler
                                           :handler-name (u/get-handler-name handler)})))))))

(defn run-resubmit-thread
  [{:keys [logger] :as context}
   {:keys [topic consumer node] :as data}]
  (log/info logger :core/run-resubmit "Running resubmit thread")
  (try (resubmit-abandoned context data
                           (fn [{:keys [handlers]} _ to-resubmit]
                             (let [handlers-by-name (u/handlers-by-name handlers topic)]
                               (doseq [[msgid key handler-name] to-resubmit]
                                 (let [handler (get handlers-by-name handler-name)]
                                   (ccy/run-async
                                    (fn [] (lock-and-process-message
                                            context {:topic topic
                                                     :consumer consumer
                                                     :node node
                                                     :key key
                                                     :messageid msgid
                                                     :handler handler
                                                     :handler-name handler-name}))))))))
       (catch Exception e (log/error logger :core/run-resubmit "Error in resubmit" e))))

(defn start-topic-consumer [{:keys [logger resubmit-available-ms] :as context}
                            {:keys [consumer-name topic node consumer-running?]}]
  (let [watch-semaphore (ccy/semaphore 1)
        watch-active? (ccy/atomic-boolean false)
        ctx (merge context {:logger (log/map* #(-> %
                                                   (assoc :topic topic)
                                                   (assoc :consumer consumer-name)
                                                   (assoc :node node)) logger)}
                   (u/quickmap consumer-running? watch-semaphore))
        topic-check-function #(consumer/topic-check ctx {:topic topic
                                                         :consumer consumer-name
                                                         :node node})
        set-watch-function #(do (when (not (.get watch-active?))
                                  (log/info logger :core/start-topic-consumer "Setting watch")
                                  (.set watch-active? true)
                                  (d/set-watch ctx [topic-head-key-part topic]
                                               (fn []
                                                 (log/info logger :core/start-topic-consumer "Watch fired")
                                                 (.set watch-active? false)
                                                 (.drainPermits watch-semaphore)
                                                 (.release watch-semaphore)))))]
    (ccy/run-async-while #(.get consumer-running?) 0
                         (fn []
                           (consumer/consumer-loop ctx
                                                   topic-check-function
                                                   set-watch-function)))
    (ccy/run-async-while #(.get consumer-running?)
                         (or resubmit-available-ms 2000)
                         #(run-resubmit-thread ctx {:topic topic
                                                    :consumer consumer-name
                                                    :node node}))
    #(do (.drain watch-semaphore)
         (.release watch-semaphore))))

(defn start-consumer [{:keys [handlers consumer-name ;Mandatory
                              consumer-poll-ms node fetch-size cluster-file
                              resubmit-available-ms resubmit-processing-ms
                              subspace handler-context logger db]
                       :or {fetch-size 10
                            node (str (UUID/randomUUID))
                            consumer-poll-ms 10000}}]
  (let [database (or db (if cluster-file
                          (d/open-db cluster-file)
                          (d/open-db)))
        consumer-running? (ccy/atomic-boolean true)
        context {:db database
                 :threads fetch-size
                 :resubmit-available-ms resubmit-available-ms
                 :resubmit-processing-ms resubmit-processing-ms
                 :consumer-poll-ms consumer-poll-ms
                 :current-timestamp-function u/current-timestamp-string
                 :notify-processors notify-processors-async
                 :handlers handlers
                 :get-range-after d/get-range-after
                 :get-value d/get-value
                 :get-range-before d/get-range-before
                 :put-all d/put-all
                 :compare-and-clear d/compare-and-clear
                 :tx-wrapper d/with-transaction
                 :id-formatter d/versionstamp->id-string
                 :subspace subspace
                 :handler-context handler-context
                 :logger (or logger (log/->StdoutLogger))}
        shutdowns (doall (->> (keys handlers)
                              (map (fn [topic]
                                     (start-topic-consumer context (u/quickmap consumer-name topic node consumer-running?))))))
        shutdown-fn #(do (.set consumer-running? false)
                         (doseq [shutdown-function shutdowns]
                           (try (shutdown-function)
                                (catch Exception _)))
                         (ccy/shutdown-executor)
                         (when db (.close database)))]
    shutdown-fn))


