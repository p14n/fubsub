(ns p14n.fubsub.processor
  (:require [p14n.fubsub.common :refer [consumer-processing-key-part
                                        processor-status-processing]]))

(defn check-for-key-in-flight [{:keys [topic consumer messageid key get-range-before]}]
  (some->> (get-range-before [consumer-processing-key-part topic consumer messageid])
           (filter #(= key (-> % first last)))
           (map first)
           (map drop-last)
           (map last)))

(defn mark-as-processing [{:keys [topic consumer messageid key node update]}]
  (update [[[consumer-processing-key-part topic consumer messageid key] [processor-status-processing node]]]))

(defn remove-processing-mark [{:keys [topic consumer messageid key delete]}]
  (delete [[consumer-processing-key-part topic consumer messageid key]]))