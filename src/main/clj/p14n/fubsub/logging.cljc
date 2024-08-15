(ns p14n.fubsub.logging
  (:require [p14n.fubsub.util :refer [current-timestamp-string]])
  (:import [java.io PrintWriter StringWriter]))

(defprotocol Logger
  (log [this msg]))

(defn map*
  [f logger]
  (reify Logger
    (log [_ msg] (log logger (f msg)))))

(defn stacktrace
  [e]
  (if e
    (let [sw (StringWriter.)
          pw (PrintWriter. sw)]
      (.printStackTrace e pw)
      (.toString sw))
    ""))

(defn assoc-if
  [m k v]
  (if v (assoc m k v)
      m))

(defn make-log-msg-or-description
  [service level msg exception]
  (let [merged-msg (-> {:service service :level level :timestamp (current-timestamp-string)}
                       (assoc-if :ex exception)
                       (assoc-if :body (when (coll? msg) msg))
                       (assoc-if :message (when (string? msg) msg)))]
    (if exception
      (assoc merged-msg :stacktrace (stacktrace exception))
      merged-msg)))

(defn log-msg-or-description
  [logger service level msg exception]
  (log logger (make-log-msg-or-description service level msg exception)))

(defn debug
  [logger service msg]
  (log-msg-or-description logger service :debug msg nil)
  logger)

(defn info
  [logger service msg]
  (log-msg-or-description logger service :info msg nil)
  logger)

(defn warn
  [logger service msg]
  (log-msg-or-description logger service :warn msg nil)
  logger)

(defn error
  ([logger service msg] (error logger service msg nil))
  ([logger service msg exception] (log-msg-or-description logger service :error msg exception)))

(defrecord TestLogger [logs]
  Logger
  (log [_ msg] (swap! logs conj msg)))

(defrecord StdoutLogger []
  Logger
  (log [_ msg] (println msg)))
