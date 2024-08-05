(ns p14n.fubsub.processor
  (:require [p14n.fubsub.common :refer [consumer-processing-key-part
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
  [{:keys [update]}
   {:keys [topic consumer messageid key node]}]
  (update [[[consumer-processing-key-part topic consumer messageid key] [processor-status-processing node]]]))

(defn remove-processing-mark
  [{:keys [delete]}
   {:keys [topic consumer messageid key]}]
  (delete [[consumer-processing-key-part topic consumer messageid key]]))