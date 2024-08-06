(ns p14n.fubsub.processor
  (:require [p14n.fubsub.common :refer [consumer-processing-key-part
                                        topic-key-part
                                        processor-status-processing]]))

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
   {:keys [topic consumer messageid key node]}]
  (put-all ctx [[[consumer-processing-key-part topic consumer messageid key] [processor-status-processing node]]]))

(defn remove-processing-mark
  [{:keys [delete] :as ctx}
   {:keys [topic consumer messageid key]}]
  (delete ctx [[consumer-processing-key-part topic consumer messageid key]]))

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
      (handler ctx (get-value ctx [topic-key-part topic messageid key]))
      (remove-processing-mark ctx
                              {:topic topic
                               :consumer consumer
                               :messageid messageid
                               :key key}))))
    