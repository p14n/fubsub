(ns p14n.fubsub.producer
  (:require [p14n.fubsub.data :as d]
            [p14n.fubsub.util :as u]
            [p14n.fubsub.common :as common :refer [topic-key-part
                                                   topic-head-key-part]])
  (:import [com.apple.foundationdb.tuple Versionstamp]))

(defn put-message
  ([topic msg subject-key]
   (put-message topic msg subject-key {}))
  ([topic msg subject-key {:keys [type datacontenttype source subspace]}]
   (d/with-transaction {}
     #(let [ctx {:tx % :subspace subspace}]
        (d/set-with-version-key ctx [[topic-key-part topic (Versionstamp/incomplete) subject-key]
                                     [msg (u/current-timestamp-string) type datacontenttype source]])
        (d/set-with-version-value ctx [[topic-head-key-part topic] [(Versionstamp/incomplete)]])))))