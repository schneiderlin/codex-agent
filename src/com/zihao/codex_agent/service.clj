(ns com.zihao.codex-agent.service
  (:require [com.zihao.codex-agent.session :as session]
            [com.zihao.codex-agent.store-edn :as store-edn]
            [com.zihao.codex-app-server.interface :as app-server])
  (:import [java.util.concurrent.locks ReentrantLock]))

(defrecord Service [config store app-server owns-app-server? locks*])

(defn- app-server-service
  [config]
  (if-let [service (:codex-app-server-service config)]
    {:service service
     :owned? false}
    {:service (app-server/start! (:codex-app-server config))
     :owned? true}))

(defn start!
  [{:keys [store-path] :as config}]
  (let [{app-service :service owned? :owned?} (app-server-service config)]
    (->Service config
               (store-edn/open! {:path store-path})
               app-service
               owned?
               (atom {}))))

(defn close!
  [^Service service]
  (when (:owns-app-server? service)
    (app-server/close! (:app-server service)))
  nil)

(defn- lock-for-session
  [service key]
  (or (get @(:locks* service) key)
      (let [lock (ReentrantLock.)]
        (get (swap! (:locks* service)
                    (fn [locks]
                      (if (contains? locks key)
                        locks
                        (assoc locks key lock))))
             key))))

(defn- ensure-session!
  [store key message now]
  (store-edn/update-session!
   store
   key
   (fn [current]
     (or current
         (session/new-session message now)))))

(defn- result-base
  [{:keys [channel external-session-id]} codex-thread-id]
  (cond-> {:channel channel
           :external-session-id (str external-session-id)}
    (some? codex-thread-id)
    (assoc :codex-thread-id codex-thread-id)))

(defn- safe-event!
  [callbacks event]
  (when-let [f (:on-event! callbacks)]
    (try
      (f event)
      (catch Throwable _))))

(defn- delivery-reply
  [reply-text result]
  {:text (or reply-text "")
   :result result})

(defn- run-reply-callback!
  [callbacks reply]
  (if-let [f (:on-reply! callbacks)]
    (do
      (f reply)
      :delivered)
    :not-configured))

(defn- app-session-key
  [[channel external-session-id]]
  (str (name channel) ":" external-session-id))

(defn- mark-thread-started!
  [service key event]
  (let [thread-id (:codex-thread-id event)
        now (session/now-iso)]
    (store-edn/update-session!
     (:store service)
     key
     (fn [current]
       (-> (or current
               {:processed-message-ids []})
           (session/set-codex-thread-id thread-id now)
           (session/mark-status :running now))))))

(defn- callback-request
  [service key callbacks latest-thread-id]
  {:on-thread-started
   (fn [event]
     (reset! latest-thread-id (:codex-thread-id event))
     (mark-thread-started! service key event)
     (safe-event! callbacks (assoc event :type :codex-agent/codex-thread-started)))

   :on-event
   (fn [event]
     (safe-event! callbacks event))

   :on-dynamic-tool-call
   (:on-dynamic-tool-call! callbacks)})

(defn- persist-completed-turn!
  [service key message session-before codex-thread-id delivery-status]
  (let [now (session/now-iso)
        external-message-id (:external-message-id message)]
    (store-edn/update-session!
     (:store service)
     key
     (fn [current]
       (-> (or current session-before)
           (session/set-codex-thread-id codex-thread-id now)
           (session/remember-message-id external-message-id
                                        (or (get-in service [:config :processed-message-limit])
                                            session/default-processed-message-limit))
           (session/mark-status :idle now)
           (session/set-delivery-status delivery-status now))))))

(defn handle-message!
  [^Service service message callbacks]
  (let [key (session/session-key message)
        lock (lock-for-session service key)]
    (if-not (.tryLock lock)
      (assoc (result-base message (some-> (store-edn/get-session (:store service) key)
                                          :codex-thread-id))
             :status :busy)
      (try
        (let [store (:store service)
              external-message-id (:external-message-id message)
              existing (store-edn/get-session store key)]
          (if (session/processed-message? existing external-message-id)
            (assoc (result-base message (:codex-thread-id existing))
                   :status :duplicate)
            (let [now (session/now-iso)
                  session-before (ensure-session! store key message now)
                  _ (store-edn/update-session!
                     store
                     key
                     (fn [current]
                       (session/mark-status current :running (session/now-iso))))
                  latest-thread-id (atom (:codex-thread-id session-before))
                  input-items (session/content->input-items (:content message))
                  app-result (app-server/run-turn!
                              (:app-server service)
                              {:session-key (app-session-key key)
                               :codex-thread-id (:codex-thread-id session-before)
                               :input-items input-items
                               :callbacks (callback-request service key callbacks latest-thread-id)})
                  codex-thread-id (or (:codex-thread-id app-result)
                                      @latest-thread-id
                                      (:codex-thread-id session-before))
                  reply-text (:text app-result)
                  status (:status app-result)]
              (if (= :completed status)
                (let [result (assoc (result-base message codex-thread-id)
                                    :status :completed
                                    :reply-text (or reply-text ""))
                      _ (persist-completed-turn! service
                                                 key
                                                 message
                                                 session-before
                                                 codex-thread-id
                                                 :pending)
                      delivery-status (try
                                        (run-reply-callback! callbacks
                                                             (delivery-reply reply-text result))
                                        (catch Throwable t
                                          (safe-event! callbacks
                                                       {:type :codex-agent/reply-callback-failed
                                                        :throwable t})
                                          :failed))]
                  (persist-completed-turn! service
                                           key
                                           message
                                           session-before
                                           codex-thread-id
                                           delivery-status)
                  (assoc result :delivery-status delivery-status))
                (let [result (assoc (result-base message codex-thread-id)
                                    :status status
                                    :reply-text (or reply-text "")
                                    :error-message (:error-message app-result))]
                  (persist-completed-turn! service
                                           key
                                           message
                                           session-before
                                           codex-thread-id
                                           :not-delivered)
                  result)))))
        (catch Throwable t
          (let [now (session/now-iso)]
            (store-edn/update-session!
             (:store service)
             key
             (fn [current]
               (if current
                 (session/mark-status current :idle now)
                 current))))
          (throw t))
        (finally
          (.unlock lock))))))
