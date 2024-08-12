(ns p14n.fubsub.processor
  (:require [p14n.fubsub.common :refer [consumer-processing-key-part
                                        topic-key-part
                                        processor-status-processing]]
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

(defn process-message
  [{:keys [get-value current-timestamp-function handler-context
           tx-wrapper logger id-formatter] :as ctx}
   {:keys [topic consumer node messageid key handler]}]
  (loop [remaining-attempts 50]
    (let [human-readable-id (id-formatter messageid)
          processing-timestamp (current-timestamp-function)
          m-logger (log/map* #(assoc % :messageid human-readable-id) logger)
          in-flight (tx-wrapper ctx
                                #(let [ctx-tx (merge (u/ctx-with-tx ctx %)
                                                     {:logger m-logger})
                                       in-flight (check-for-key-in-flight ctx-tx
                                                                          {:topic topic
                                                                           :consumer consumer
                                                                           :messageid messageid
                                                                           :key key})]
                                   (when (not (seq in-flight))
                                     (mark-as-processing ctx-tx
                                                         {:topic topic
                                                          :consumer consumer
                                                          :messageid messageid
                                                          :timestamp processing-timestamp
                                                          :key key
                                                          :node node}))
                                   in-flight))]
      (if (not (seq in-flight))
        (do (log/info logger :processor/process-message "Starting processing")
            (tx-wrapper ctx
                        #(let [ctx-tx (u/ctx-with-tx ctx %)
                               [msg time type datacontenttype source] (get-value ctx-tx [topic-key-part topic messageid key])]
                           (handler (-> ctx-tx
                                        (assoc :logger m-logger)
                                        (merge handler-context))
                                    (-> {:subject key
                                         :id human-readable-id
                                         :data msg}
                                        (assoc-if :time time)
                                        (assoc-if :type type)
                                        (assoc-if :datacontenttype datacontenttype)
                                        (assoc-if :source source)))
                           (remove-processing-mark ctx-tx
                                                   {:topic topic
                                                    :node node
                                                    :timestamp processing-timestamp
                                                    :consumer consumer
                                                    :messageid messageid
                                                    :key key}))))
        (if (>= 0 remaining-attempts)
          (log/warn logger :processor/process-message "Previous messages found for key after multiple attempts - stopping handler")
          (recur (dec remaining-attempts)))))))

(defn get-all-processing
  [{:keys [get-range-after] :as ctx}
   {:keys [topic consumer]}]
  (some->> (get-range-after ctx [consumer-processing-key-part topic consumer ""] 100)))
