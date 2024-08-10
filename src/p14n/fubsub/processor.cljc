(ns p14n.fubsub.processor
  (:require [p14n.fubsub.common :refer [consumer-processing-key-part
                                        topic-key-part
                                        processor-status-processing]]
            [p14n.fubsub.data :as d]
            [p14n.fubsub.util :as u :refer [assoc-if]]))

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
  [{:keys [get-value current-timestamp-function
           tx-wrapper info-log error-log id-formatter] :as ctx}
   {:keys [topic consumer node msg handler]}]
  (loop [remaining-attempts 50]
    (let [[[_ _ messageid key] _] msg
          human-readable-id (id-formatter messageid)
          processing-timestamp (current-timestamp-function)
          minfo-log #(info-log (str "msg:" human-readable-id " " %))
          merror-log #(error-log (concat [(str "msg:" human-readable-id " " %)] %&))
          in-flight (tx-wrapper ctx
                                #(let [ctx-tx (merge (u/ctx-with-tx ctx %)
                                                     {:info-log minfo-log
                                                      :error-log merror-log})
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
        (do (minfo-log "Starting processing")
            (tx-wrapper ctx
                        #(let [ctx-tx (u/ctx-with-tx ctx %)
                               [msg time type datacontenttype source] (get-value ctx-tx [topic-key-part topic messageid key])]
                           (handler (-> ctx-tx
                                        (assoc :info-log minfo-log)
                                        (assoc :error-log merror-log))
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
          (minfo-log "Previous messages found for key after multiple attempts - stopping handler")
          (recur (dec remaining-attempts)))))))

