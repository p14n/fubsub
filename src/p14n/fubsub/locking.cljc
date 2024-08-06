(ns p14n.fubsub.locking
  (:import [java.util.concurrent.locks ReentrantLock]
           [java.util.concurrent ConcurrentHashMap]
           [java.util.function BiFunction]))

(def ^:private ^ConcurrentHashMap locks (ConcurrentHashMap.))

(defn- unlock-if-locked-by-thread
  "If the current thread holds the `lock`, unlock it."
  [lock]
  (when (.isHeldByCurrentThread lock)
    (.unlock lock)
    nil))

(defn- lock-if-not-locked-by-thread
  "If the current thread does not hold the `lock`, lock it."
  [lock]
  (when-not (.isHeldByCurrentThread lock)
    (.lock lock)
    nil))

(def ^BiFunction lock-getter
  "For use in ConcurrentHashMap.compute
   If a lock has already been created for the key, increment the active count and return both.
   If no lock exists, create one and return it with an active count of 1, locking on return.
   Locking must happen (for the first thread only - any subsequent ones would case a deadlock) withing the call to compute to avoid a race condition where the entry is
   added to the map but the lock is stolen by another thread between returning and locking.
   
   It is necessary to keep a seperate active count, synchronised with the lock.  This is to avoid the following race condition:
   * Primary thread adds the lock to the map and locks it
   * Secondary thread reads the lock from the map, but hasn't yet locked it
   * Primary thread unlocks the lock, and seeing no threads waiting, removes it from the map
   * Secondary thread locks the lock, but the lock has been removed from the map
   * A third thread gets a new lock from the map: two threads are now operating on the same key"
  (reify
    BiFunction
    (apply [_ _ [active lock]]
      (if (and lock active (> active 0))
        [(inc active) lock]
        [1 (doto
            (ReentrantLock.)
             (.lock))]))))

(def ^BiFunction lock-remover
  "For use in ConcurrentHashMap.compute
   If the active count is greater than 1, decrement it and return both.
   If the active count is 1, this thread is the only user; unlock the lock and return nil,
   indicating that the entry should be removed."
  (reify
    BiFunction
    (apply [_ _ [active lock]]
      (when (and lock active)
        (unlock-if-locked-by-thread lock)
        (if (> active 1)
          [(dec active) lock]
          nil)))))

(defn acquire-lock
  "Acquire a lock for the given key. If the lock is held by another thread, block until it is released."
  [key]
  (let [[_ lock] (.compute locks key lock-getter)]
    (lock-if-not-locked-by-thread lock)))

(defn release-lock
  "Release a lock for the given key."
  [key]
  (let [[active _] (.compute locks key lock-remover)]
    active))
