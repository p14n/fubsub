(ns p14n.fubsub.processor
  (:require [p14n.fubsub.common :refer [consumer-processing-key-part
                                        topic-key-part
                                        processor-status-processing
                                        processor-status-available]]
            [p14n.fubsub.util :as u :refer [assoc-if]]
            [p14n.fubsub.logging :as log]))

(defn key-from-processing-record [[[_ _ _ _ k] _]] k)
(defn msg-id-from-processing-record [[[_ _ _ m] _]] m)

(defn check-for-key-in-flight
  [{:keys [get-range-before] :as ctx}
   {:keys [topic consumer messageid key]}]
  (->> (get-range-before ctx [consumer-processing-key-part topic consumer messageid])
       (filter #(= key (key-from-processing-record %)))
       (map msg-id-from-processing-record)))

(defn mark-as-processing
  [{:keys [put-all] :as ctx}
   {:keys [topic consumer messageid key node timestamp handler-name]}]
  (put-all ctx [[[consumer-processing-key-part topic consumer messageid key handler-name] [processor-status-processing node timestamp]]]))

(defn remove-processing-mark
  [{:keys [compare-and-clear] :as ctx}
   {:keys [topic consumer messageid key timestamp node handler-name]}]
  (compare-and-clear ctx [[consumer-processing-key-part topic consumer messageid key handler-name] [processor-status-processing node timestamp]]))

(defn check-processable [{:keys [get-value tx-wrapper] :as ctx}
                         {:keys [topic consumer messageid key handler-name] :as data}]
  (tx-wrapper ctx
              #(let [ctx-tx (merge (u/ctx-with-tx ctx %))
                     in-flight (check-for-key-in-flight ctx-tx data)
                     [status] (get-value ctx-tx [consumer-processing-key-part topic consumer messageid key handler-name])
                     in-flight? (seq in-flight)
                     still-available? (= status processor-status-available)
                     ready-to-process? (and (not in-flight?)
                                            still-available?)]
                 (when ready-to-process?
                   (mark-as-processing ctx-tx data))
                 [ready-to-process? in-flight? still-available?])))

(defn process-and-remove-mark [{:keys [get-value handler-context tx-wrapper logger] :as ctx}
                               {:keys [topic messageid key handler
                                       human-readable-id] :as data}]
  (log/info logger :processor/process-and-remove-mark "Starting processing")
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
          ctx-new (assoc ctx :logger m-logger)
          data-new (assoc data
                          :timestamp processing-timestamp
                          :human-readable-id human-readable-id)
          [ready-to-process? in-flight? still-available?] (check-processable ctx-new data-new)]
      (cond
        ready-to-process? (process-and-remove-mark ctx-new data-new)
        (>= 0 remaining-attempts) (log/warn logger :processor/process-message "Previous messages found for key after multiple attempts - stopping handler")
        (not (and in-flight? still-available?)) (log/warn logger :processor/process-message "Message has been handled - stopping handler")
        :else (recur (dec remaining-attempts))))))

(defn get-all-processing
  [{:keys [get-range-after] :as ctx}
   {:keys [topic consumer]}]
  (some->> (get-range-after ctx [consumer-processing-key-part topic consumer ""] 100)))
