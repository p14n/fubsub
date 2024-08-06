(ns p14n.fubsub.processor
  (:require [p14n.fubsub.common :refer [consumer-processing-key-part
                                        topic-key-part
                                        processor-status-processing]]))

(defn check-for-key-in-flight
  [{:keys [get-range-before]}
   {:keys [topic consumer messageid key]}]
  (some->> (get-range-before [consumer-processing-key-part topic consumer messageid])
           (filter #(= key (-> % first last)))
           (map first)
           (map drop-last)
           (map last)))

(defn mark-as-processing
  [{:keys [put-all]}
   {:keys [topic consumer messageid key node]}]
  (put-all [[[consumer-processing-key-part topic consumer messageid key] [processor-status-processing node]]]))

(defn remove-processing-mark
  [{:keys [delete]}
   {:keys [topic consumer messageid key]}]
  (delete [[consumer-processing-key-part topic consumer messageid key]]))

(defn process-message
  [{:keys [get-value] :as ctx}
   {:keys [topic consumer node msg handler]}]
  (let [[[_ _ messageid key] _] msg
        in-flight (check-for-key-in-flight ctx
                                           {:topic topic
                                            :consumer consumer
                                            :messageid messageid
                                            :key key})]
    (when (not (seq in-flight))
      (mark-as-processing ctx
                          {:topic topic
                           :consumer consumer
                           :messageid messageid
                           :key key
                           :node node})
      (handler ctx (get-value [topic-key-part topic messageid key]))
      (remove-processing-mark ctx
                              {:topic topic
                               :consumer consumer
                               :messageid messageid
                               :key key}))))
    