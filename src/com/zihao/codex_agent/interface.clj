(ns com.zihao.codex-agent.interface
  (:require [com.zihao.codex-agent.service :as service]))

(defn start!
  [config]
  (service/start! config))

(defn close!
  [svc]
  (service/close! svc))

(defn handle-message!
  [svc message callbacks]
  (service/handle-message! svc message callbacks))

(defn promote-session!
  [svc request]
  (service/promote-session! svc request))
