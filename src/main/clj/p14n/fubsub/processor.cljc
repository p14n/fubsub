(ns p14n.fubsub.processor
  (:require [p14n.fubsub.common :refer [consumer-processing-key-part
                                        topic-key-part
                                        processor-status-processing
                                        processor-status-available]]
            [p14n.fubsub.util :as u :refer [assoc-if]]
            [p14n.fubsub.logging :as log]))

(defn check-for-key-in-flight
  [{:keys [get-range-before] :as ctx}
   {:keys [topic consumer messageid key]}]
  (some->> (get-range-before ctx [consumer-processing-key-part topic consumer messageid])
           (filter #(= key (-> % first last)))
           (map first)
           (map drop-last)
           (map last)))

(defn mark-as-processing
  [{:keys [put-all] :as ctx}
   {:keys [topic consumer messageid key node timestamp]}]
  (put-all ctx [[[consumer-processing-key-part topic consumer messageid key] [processor-status-processing node timestamp]]]))

(defn remove-processing-mark
  [{:keys [compare-and-clear] :as ctx}
   {:keys [topic consumer messageid key timestamp node]}]
  (compare-and-clear ctx [[consumer-processing-key-part topic consumer messageid key] [processor-status-processing node timestamp]]))

(defn check-processable [{:keys [get-value tx-wrapper] :as ctx}
                         {:keys [topic consumer messageid key] :as data}]
  (tx-wrapper ctx
              #(let [ctx-tx (merge (u/ctx-with-tx ctx %))
                     in-flight (check-for-key-in-flight ctx-tx data)
                     [status] (get-value ctx-tx [consumer-processing-key-part topic consumer messageid key])
                     in-flight? (seq in-flight)
                     still-available? (= status processor-status-available)
                     ready-to-process? (and (not in-flight?)
                                            still-available?)]
                 (when ready-to-process?
                   (mark-as-processing ctx-tx data))
                 [ready-to-process? in-flight? still-available?])))


(defn process-and-remove-mark [{:keys [get-value handler-context tx-wrapper] :as ctx}
                               {:keys [topic messageid key handler
                                       human-readable-id] :as data}]
  (tx-wrapper ctx
              #(let [ctx-tx (u/ctx-with-tx ctx %)
                     [msg time type datacontenttype source] (get-value ctx-tx [topic-key-part topic messageid key])]
                 (handler (-> ctx-tx
                              (merge handler-context))
                          (-> {:subject key
                               :id human-readable-id
                               :data msg}
                              (assoc-if :time time)
                              (assoc-if :type type)
                              (assoc-if :datacontenttype datacontenttype)
                              (assoc-if :source source)))
                 (remove-processing-mark ctx-tx data))))

(defn process-message
  [{:keys [current-timestamp-function
           logger id-formatter] :as ctx}
   {:keys [messageid] :as data}]
  (loop [remaining-attempts 50]
    (let [human-readable-id (id-formatter messageid)
          processing-timestamp (current-timestamp-function)
          m-logger (log/map* #(assoc % :messageid human-readable-id) logger)
          [ready-to-process? in-flight? still-available?] (check-processable (assoc ctx :logger m-logger)
                                                                             (assoc data :timestamp processing-timestamp))]
      (if ready-to-process?
        (do (log/info logger :processor/process-message "Starting processing")
            (process-and-remove-mark (assoc ctx :logger m-logger)
                                     (assoc data
                                            :timestamp processing-timestamp
                                            :human-readable-id human-readable-id)))

        (cond
          (>= 0 remaining-attempts) (log/warn logger :processor/process-message "Previous messages found for key after multiple attempts - stopping handler")
          (not (and in-flight? still-available?)) (log/warn logger :processor/process-message "Message has been handled - stopping handler")
          :else (recur (dec remaining-attempts)))))))

(defn get-all-processing
  [{:keys [get-range-after] :as ctx}
   {:keys [topic consumer]}]
  (some->> (get-range-after ctx [consumer-processing-key-part topic consumer ""] 100)))
