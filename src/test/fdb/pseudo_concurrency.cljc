(ns pseudo-concurrency
  (:import [java.util Collections ArrayList]))

(def once-threads (atom []))
(def continuous-threads (atom []))

(defn run-async [f]
  (swap! once-threads conj [(constantly true) f true]))

(defn run-async-while [pred _ f]
  (swap! continuous-threads conj [pred f false]))

(defn acquire-semaphore [s _]
  (> (.availablePermits s) 0))

(defn seeded-shuffle [random coll]
  (let [a (ArrayList. coll)]
    (Collections/shuffle a random)
    (vec (.toArray a))))

(defn shuffle-and-pop-first-thread-function [random]
  (let [all-threads (concat @once-threads @continuous-threads)
        shuffled-threads (seeded-shuffle random all-threads)
        [thread-fn & other-threads] shuffled-threads
        [pred f remove?] thread-fn]
    (when remove?
      (reset! once-threads (filter last other-threads)))
    [pred f]))

(defn reset-threads []
  (reset! once-threads [])
  (reset! continuous-threads []))
