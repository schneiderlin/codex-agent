(ns com.zihao.codex-agent.service
  (:require [clojure.core.async :as async]
            [com.zihao.codex-agent.session :as session]
            [com.zihao.codex-agent.store-edn :as store-edn]
            [com.zihao.codex-app-server.interface :as app-server])
  (:import [java.util.concurrent.locks ReentrantLock]))

(declare handle-message! safe-event!)

(def default-submit-worker-count 4)
(def default-submit-close-timeout-ms 1000)

(def ^:private empty-submission-queue clojure.lang.PersistentQueue/EMPTY)

(defrecord SubmissionDispatcher [ready-ch worker-futures !queues !closed?])

(defrecord Service [config store app-server owns-app-server? locks* dispatcher* closed?*])

(defn- message-id-key
  [message]
  (when-some [message-id (:external-message-id message)]
    (str message-id)))

(defn- schedule-session!
  [dispatcher key]
  (when-not @(:!closed? dispatcher)
    (async/put! (:ready-ch dispatcher) key)))

(defn- enqueue-submission!
  [^SubmissionDispatcher dispatcher key message callbacks]
  (if @(:!closed? dispatcher)
    {:status :closed
     :channel (first key)
     :external-session-id (second key)}
    (let [result (atom nil)
          schedule? (atom false)]
      (swap!
       (:!queues dispatcher)
       (fn [queues]
         (let [entry (get queues key {:items empty-submission-queue
                                      :queued-message-ids #{}
                                      :running? false})
               message-id (message-id-key message)]
           (if (and message-id
                    (contains? (:queued-message-ids entry) message-id))
             (do
               (reset! result
                       {:status :duplicate
                        :channel (first key)
                        :external-session-id (second key)})
               queues)
             (let [items* (conj (:items entry)
                                {:message message
                                 :callbacks callbacks})
                   entry* (cond-> (assoc entry
                                          :items items*
                                          :running? true)
                            message-id
                            (update :queued-message-ids conj message-id))]
               (when-not (:running? entry)
                 (reset! schedule? true))
               (reset! result
                       {:status :queued
                        :channel (first key)
                        :external-session-id (second key)
                        :queue-depth (count items*)})
               (assoc queues key entry*))))))
      (when @schedule?
        (schedule-session! dispatcher key))
      @result)))

(defn- take-next-submission!
  [^SubmissionDispatcher dispatcher key]
  (let [submission (atom nil)]
    (swap!
     (:!queues dispatcher)
     (fn [queues]
       (let [entry (get queues key)
             items (:items entry)]
         (if (seq items)
           (let [item (peek items)
                 message-id (message-id-key (:message item))
                 items* (pop items)
                 entry* (cond-> (assoc entry :items items*)
                          message-id
                          (update :queued-message-ids disj message-id))]
             (reset! submission item)
             (assoc queues key entry*))
           (dissoc queues key)))))
    @submission))

(defn- complete-submission!
  [^SubmissionDispatcher dispatcher key]
  (let [schedule? (atom false)]
    (swap!
     (:!queues dispatcher)
     (fn [queues]
       (if-let [entry (get queues key)]
         (if (seq (:items entry))
           (do
             (reset! schedule? true)
             (assoc queues key (assoc entry :running? true)))
           (dissoc queues key))
         queues)))
    (when @schedule?
      (schedule-session! dispatcher key))))

(defn- discard-pending-submissions!
  [^SubmissionDispatcher dispatcher key]
  (let [discarded (atom 0)]
    (swap!
     (:!queues dispatcher)
     (fn [queues]
       (if-let [entry (get queues key)]
         (let [n (count (:items entry))]
           (reset! discarded n)
           (if (:running? entry)
             (assoc queues key (assoc entry
                                      :items empty-submission-queue
                                      :queued-message-ids #{}))
             (dissoc queues key)))
         queues)))
    @discarded))

(defn- submission-worker-loop!
  [dispatcher process!]
  (loop []
    (when-let [key (async/<!! (:ready-ch dispatcher))]
      (when-let [submission (take-next-submission! dispatcher key)]
        (try
          (process! submission)
          (catch Throwable t
            (safe-event! (:callbacks submission)
                         {:type :codex-agent/submitted-message-failed
                          :message (:message submission)
                          :throwable t})))
        (complete-submission! dispatcher key))
      (recur))))

(defn- start-submission-dispatcher!
  [process! worker-count]
  (let [ready-ch (async/chan)
        !queues (atom {})
        !closed? (atom false)
        dispatcher-promise (promise)
        worker-futures (mapv (fn [_]
                               (future
                                 (submission-worker-loop! @dispatcher-promise process!)))
                             (range worker-count))
        dispatcher (->SubmissionDispatcher ready-ch worker-futures !queues !closed?)]
    (deliver dispatcher-promise dispatcher)
    dispatcher))

(defn- close-submission-dispatcher!
  [^SubmissionDispatcher dispatcher timeout-ms]
  (reset! (:!closed? dispatcher) true)
  (reset! (:!queues dispatcher) {})
  (async/close! (:ready-ch dispatcher))
  (doseq [worker (:worker-futures dispatcher)]
    (deref worker timeout-ms nil))
  nil)

(defn- submit-worker-count
  [config]
  (max 1 (long (or (:submit-worker-count config)
                   default-submit-worker-count))))

(defn- submit-close-timeout-ms
  [config]
  (long (or (:submit-close-timeout-ms config)
            default-submit-close-timeout-ms)))

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
               (atom {})
               (atom nil)
               (atom false))))

(defn close!
  [^Service service]
  (reset! (:closed?* service) true)
  (when-let [dispatcher @(:dispatcher* service)]
    (close-submission-dispatcher! dispatcher
                                  (submit-close-timeout-ms (:config service)))
    (reset! (:dispatcher* service) nil))
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

(defn- emit-codex-event!
  [callbacks type data]
  (safe-event! callbacks (assoc data :type type)))

(defn- delivery-reply
  [reply-text result]
  {:text (or reply-text "")
   :result result})

(defn- run-reply-callback!
  [callbacks reply]
  (if-let [f (:on-reply! callbacks)]
    {:status :delivered
     :result (f reply)}
    {:status :not-configured}))

(defn- promotion-external-session-id
  [delivery]
  (let [result (:result delivery)]
    (or (:promote-external-session-id result)
        (:codex-agent/promote-external-session-id result))))

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

   :on-item-started
   (fn [item]
     (emit-codex-event! callbacks :codex/item-started item))

   :on-item-completed
   (fn [item]
     (emit-codex-event! callbacks :codex/item-completed item))

   :on-command-delta
   (fn [delta]
     (safe-event! callbacks {:type :codex/command-delta
                             :delta delta}))

   :on-dynamic-tool-call
   (:on-dynamic-tool-call! callbacks)})

(defn- app-server-request-options
  [service callbacks]
  (merge (select-keys (:config service)
                      [:dynamic-tools :experimental-api? :workdir-path])
         (select-keys callbacks
                      [:dynamic-tools :experimental-api? :workdir-path])))

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

(defn promote-session!
  [^Service service {:keys [channel from-external-session-id to-external-session-id]}]
  (let [from-key (session/session-key {:channel channel
                                       :external-session-id from-external-session-id})
        to-key (session/session-key {:channel channel
                                     :external-session-id to-external-session-id})
        now (session/now-iso)]
    (store-edn/promote-session!
     (:store service)
     from-key
     to-key
     #(session/retarget-session % to-key now))))

(defn stop-session!
  "Interrupt the active Codex turn for a session without taking that session's
   message lock. This lets an out-of-band control message stop a running turn."
  [^Service service message]
  (let [store (:store service)
        requested-key (session/session-key message)
        key (store-edn/canonical-session-key store requested-key)
        stored-session (store-edn/get-session store key)
        discarded-count (if-let [dispatcher @(:dispatcher* service)]
                          (discard-pending-submissions! dispatcher key)
                          0)
        interrupt-result (app-server/interrupt-turn!
                          (:app-server service)
                          {:session-key (app-session-key key)})
        codex-thread-id (or (:codex-thread-id interrupt-result)
                            (:codex-thread-id stored-session))]
    (cond-> (merge
             (result-base {:channel (first key)
                           :external-session-id (second key)}
                          codex-thread-id)
             {:status (if (:interrupted? interrupt-result) :interrupted :idle)
              :interrupted? (boolean (:interrupted? interrupt-result))}
             (select-keys interrupt-result [:reason :error-message :codex-turn-id]))
      (pos? discarded-count)
      (assoc :discarded-message-count discarded-count))))

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
                              (merge
                               (app-server-request-options service callbacks)
                               {:session-key (app-session-key key)
                                :codex-thread-id (:codex-thread-id session-before)
                                :input-items input-items
                                :callbacks (callback-request service key callbacks latest-thread-id)}))
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
                      delivery (try
                                 (run-reply-callback! callbacks
                                                      (delivery-reply reply-text result))
                                 (catch Throwable t
                                   (safe-event! callbacks
                                                {:type :codex-agent/reply-callback-failed
                                                 :throwable t})
                                   {:status :failed}))
                      promote-to (promotion-external-session-id delivery)
                      promoted? (and promote-to
                                     (not= (str promote-to)
                                           (str (:external-session-id message))))
                      final-key (if promoted?
                                  (do
                                    (promote-session! service
                                                      {:channel (:channel message)
                                                       :from-external-session-id (:external-session-id message)
                                                       :to-external-session-id promote-to})
                                    (session/session-key {:channel (:channel message)
                                                          :external-session-id promote-to}))
                                  key)
                      final-result (cond-> result
                                     promoted?
                                     (assoc :external-session-id (str promote-to)
                                            :previous-external-session-id (str (:external-session-id message))
                                            :promoted? true))]
                  (persist-completed-turn! service
                                           final-key
                                           (assoc message :external-session-id (second final-key))
                                           session-before
                                           codex-thread-id
                                           (:status delivery))
                  (assoc final-result :delivery-status (:status delivery)))
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

(defn- process-submission!
  [service {:keys [message callbacks]}]
  (handle-message! service message callbacks))

(defn- ensure-submission-dispatcher!
  [^Service service]
  (when @(:closed?* service)
    (throw (ex-info "Codex Agent service is closed"
                    {:type :codex-agent/service-closed})))
  (or @(:dispatcher* service)
      (let [dispatcher (start-submission-dispatcher!
                        #(process-submission! service %)
                        (submit-worker-count (:config service)))
            installed (swap! (:dispatcher* service)
                             (fn [current]
                               (or current dispatcher)))]
        (when-not (identical? installed dispatcher)
          (close-submission-dispatcher! dispatcher 0))
        installed)))

(defn submit-message!
  "Queue a message for asynchronous per-session FIFO handling.

   This is the non-blocking adapter entrypoint. It keeps ordinary messages for
   one external session ordered while allowing different sessions to run in
   parallel. The queue is in-memory and is discarded when the service closes."
  [^Service service message callbacks]
  (let [store (:store service)
        key (store-edn/canonical-session-key store (session/session-key message))
        existing (store-edn/get-session store key)]
    (cond
      @(:closed?* service)
      {:status :closed
       :channel (first key)
       :external-session-id (second key)}

      (session/processed-message? existing (:external-message-id message))
      (assoc (result-base {:channel (first key)
                           :external-session-id (second key)}
                          (:codex-thread-id existing))
             :status :duplicate)

      :else
      (enqueue-submission! (ensure-submission-dispatcher! service)
                           key
                           message
                           callbacks))))
