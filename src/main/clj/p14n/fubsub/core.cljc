(ns p14n.fubsub.core
  (:require [p14n.fubsub.consumer :as consumer]
            [p14n.fubsub.processor :as processor]
            [p14n.fubsub.common :as common :refer [topic-head-key-part]]
            [p14n.fubsub.data :as d]
            [p14n.fubsub.util :as u]
            [p14n.fubsub.concurrency :as ccy])
  (:import [java.util UUID]
           [java.lang Exception]
           [java.io StringWriter PrintWriter]))

(defn- stacktrace->string [e]
  (let [sw (StringWriter.)]
    (.printStackTrace e (PrintWriter. sw))
    (.toString sw)))

(defn- notify-processors-async
  [{:keys [handlers error-log info-log subspace] :as ctx}
   {:keys [topic consumer node msgs]}]
  (doseq [msg msgs]
    (doseq [handler (get handlers topic)]
      (when handler
        (let [msg-key (if subspace (drop (count subspace) (first msg)) (first msg))
              [_ _ _ key] msg-key
              _ (println "notify-processors-async" msg-key)
              lock-key (str topic "/" consumer "/" key)]
          (ccy/run-async
           #(do
              (info-log (str "Running process" lock-key))
              (ccy/acquire-lock lock-key)
              (info-log (str "Acquired lock " lock-key))
              (try
                (processor/process-message ctx {:topic topic
                                                :consumer consumer
                                                :node node
                                                :msg msg
                                                :handler handler})
                (catch Exception e (error-log "Error in processor" e))
                (finally (ccy/release-lock lock-key))))))))))

(defn start-topic-consumer [context {:keys [consumer-name topic node consumer-running? error-log info-log]}]
  (let [watch-semaphore (ccy/semaphore 1)
        watch-active? (ccy/atomic-boolean false)
        ierror-log (or error-log #(let [[msg ex] %&]
                                    (if ex
                                      (println (u/current-timestamp-string) consumer-name topic "ERROR" msg (stacktrace->string ex))
                                      (println (u/current-timestamp-string) consumer-name topic "ERROR" msg))))
        iinfo-log (or info-log #(println (u/current-timestamp-string) consumer-name topic "INFO" %))
        ctx (merge context {:error-log ierror-log
                            :info-log iinfo-log})]
    (ccy/run-async
     (fn []
       (consumer/consumer-loop
        (merge ctx (u/quickmap consumer-running? watch-semaphore))
        #(consumer/topic-check ctx {:topic topic
                                    :consumer consumer-name
                                    :node node})
        #(do (when (not (.get watch-active?))
               (iinfo-log "Setting watch")
               (.set watch-active? true)
               (d/set-watch ctx [topic-head-key-part topic]
                            (fn []
                              (iinfo-log "Watch fired")
                              (.set watch-active? false)
                              (.drainPermits watch-semaphore)
                              (.release watch-semaphore))))))))
    #(do (.drain watch-semaphore)
         (.release watch-semaphore))))

(defn start-consumer [{:keys [handlers consumer-name ;Mandatory
                              consumer-poll-ms node fetch-size cluster-file
                              error-log info-log subspace]
                       :or {fetch-size 10
                            node (str (UUID/randomUUID))
                            consumer-poll-ms 10000}}]
  (let [db (if cluster-file
             (d/open-db cluster-file)
             (d/open-db))
        consumer-running? (ccy/atomic-boolean true)
        context {:db db
                 :threads fetch-size
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
                 :subspace subspace}
        shutdowns (doall (->> (keys handlers)
                              (map (fn [topic]
                                     (start-topic-consumer context (u/quickmap consumer-name topic node consumer-running? error-log info-log))))))]

    #(do (.set consumer-running? false)
         (doseq [shutdown-function shutdowns]
           (try (shutdown-function)
                (catch Exception _)))
         (ccy/shutdown-executor)
         (.close db))))


