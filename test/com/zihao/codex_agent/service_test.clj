(ns com.zihao.codex-agent.service-test
  (:require [clojure.edn :as edn]
            [clojure.java.io :as io]
            [clojure.test :refer [deftest is testing]]
            [com.zihao.codex-agent.interface :as codex-agent]
            [com.zihao.codex-app-server.interface :as app-server]))

(defn- temp-store-path
  []
  (let [dir (doto (io/file "tmp" "codex-agent-service-tests" (str (java.util.UUID/randomUUID)))
              (.mkdirs))]
    (.getPath (io/file dir "sessions.edn"))))

(defn- message
  ([message-id text]
   (message "chat-1" message-id text))
  ([session-id message-id text]
   {:channel :feishu
    :external-session-id session-id
    :external-message-id message-id
    :content [{:type :text :text text}]}))

(deftest handle-message-starts-thread-persists-session-and-replies
  (testing "first external message creates a Codex thread and persists before reply callback"
    (let [path (temp-store-path)
          service (codex-agent/start! {:store-path path
                                       :codex-app-server-service ::fake})
          replies (atom [])
          requests (atom [])]
      (with-redefs [app-server/run-turn!
                    (fn [_ request]
                      (swap! requests conj request)
                      ((get-in request [:callbacks :on-thread-started])
                       {:codex-thread-id "remote-thread-1"
                        :codex-thread-title "Remote"})
                      {:status :completed
                       :codex-thread-id "remote-thread-1"
                       :text "hello back"})]
        (let [result (codex-agent/handle-message!
                      service
                      (message "m1" "hello")
                      {:on-reply! (fn [reply]
                                    (swap! replies conj {:reply reply
                                                         :store (edn/read-string (slurp path))}))})
              key [:feishu "chat-1"]
              root (edn/read-string (slurp path))]
          (is (= {:status :completed
                  :channel :feishu
                  :external-session-id "chat-1"
                  :codex-thread-id "remote-thread-1"
                  :reply-text "hello back"
                  :delivery-status :delivered}
                 result))
          (is (= "feishu:chat-1" (:session-key (first @requests))))
          (is (= [{:type "text" :text "hello" :text_elements []}]
                 (:input-items (first @requests))))
          (is (= "remote-thread-1"
                 (get-in root [:sessions key :codex-thread-id])))
          (is (= ["m1"]
                 (get-in root [:sessions key :processed-message-ids])))
          (is (= :delivered
                 (get-in root [:sessions key :last-delivery-status])))
          (is (= "remote-thread-1"
                 (get-in (-> @replies first :store) [:sessions key :codex-thread-id]))))))))

(deftest repeated-message-reuses-stored-codex-thread-and-duplicate-is-idempotent
  (testing "new messages reuse the stored thread and duplicate message ids do not run turns"
    (let [path (temp-store-path)
          service (codex-agent/start! {:store-path path
                                       :codex-app-server-service ::fake})
          requests (atom [])]
      (with-redefs [app-server/run-turn!
                    (fn [_ request]
                      (swap! requests conj request)
                      (let [thread-id (or (:codex-thread-id request) "remote-thread-1")]
                        (when-not (:codex-thread-id request)
                          ((get-in request [:callbacks :on-thread-started])
                           {:codex-thread-id thread-id}))
                        {:status :completed
                         :codex-thread-id thread-id
                         :text "ok"}))]
        (codex-agent/handle-message! service (message "m1" "first") {})
        (let [second-result (codex-agent/handle-message! service (message "m2" "second") {})
              duplicate-result (codex-agent/handle-message! service (message "m2" "second again") {})]
          (is (= "remote-thread-1" (:codex-thread-id second-result)))
          (is (= "remote-thread-1" (:codex-thread-id (second @requests))))
          (is (= :duplicate (:status duplicate-result)))
          (is (= 2 (count @requests))))))))

(deftest reply-callback-failure-is-recorded-without-rolling-back-turn
  (testing "callback failure updates delivery state but keeps completed turn result"
    (let [path (temp-store-path)
          service (codex-agent/start! {:store-path path
                                       :codex-app-server-service ::fake})]
      (with-redefs [app-server/run-turn!
                    (fn [_ request]
                      ((get-in request [:callbacks :on-thread-started])
                       {:codex-thread-id "remote-thread-1"})
                      {:status :completed
                       :codex-thread-id "remote-thread-1"
                       :text "reply"})]
        (let [result (codex-agent/handle-message!
                      service
                      (message "m1" "hello")
                      {:on-reply! (fn [_]
                                    (throw (ex-info "reply failed" {})))})
              root (edn/read-string (slurp path))]
          (is (= :completed (:status result)))
          (is (= :failed (:delivery-status result)))
          (is (= :failed
                 (get-in root [:sessions [:feishu "chat-1"] :last-delivery-status]))))))))

(deftest reply-callback-can-promote-bootstrap-session
  (testing "reply delivery can promote a bootstrap key to the channel thread id"
    (let [path (temp-store-path)
          service (codex-agent/start! {:store-path path
                                       :codex-app-server-service ::fake})
          bootstrap-message {:channel :feishu
                             :external-session-id "bootstrap:om_1"
                             :external-message-id "om_1"
                             :content [{:type :text :text "hello"}]}]
      (with-redefs [app-server/run-turn!
                    (fn [_ request]
                      ((get-in request [:callbacks :on-thread-started])
                       {:codex-thread-id "remote-thread-1"})
                      {:status :completed
                       :codex-thread-id "remote-thread-1"
                       :text "reply"})]
        (let [result (codex-agent/handle-message!
                      service
                      bootstrap-message
                      {:on-reply! (fn [_]
                                    {:promote-external-session-id "omt_1"})})
              root (edn/read-string (slurp path))
              bootstrap-key [:feishu "bootstrap:om_1"]
              thread-key [:feishu "omt_1"]]
          (is (= :completed (:status result)))
          (is (= true (:promoted? result)))
          (is (= "omt_1" (:external-session-id result)))
          (is (= "bootstrap:om_1" (:previous-external-session-id result)))
          (is (nil? (get-in root [:sessions bootstrap-key])))
          (is (= thread-key (get-in root [:aliases bootstrap-key])))
          (is (= "remote-thread-1"
                 (get-in root [:sessions thread-key :codex-thread-id])))
          (is (= ["om_1"]
                 (get-in root [:sessions thread-key :processed-message-ids])))
          (is (= :delivered
                 (get-in root [:sessions thread-key :last-delivery-status]))))))))

(deftest promoted-bootstrap-duplicate-resolves-through-alias
  (testing "a retried top-level message does not start another turn after promotion"
    (let [path (temp-store-path)
          service (codex-agent/start! {:store-path path
                                       :codex-app-server-service ::fake})
          calls (atom 0)
          bootstrap-message {:channel :feishu
                             :external-session-id "bootstrap:om_1"
                             :external-message-id "om_1"
                             :content [{:type :text :text "hello"}]}]
      (with-redefs [app-server/run-turn!
                    (fn [_ request]
                      (swap! calls inc)
                      ((get-in request [:callbacks :on-thread-started])
                       {:codex-thread-id "remote-thread-1"})
                      {:status :completed
                       :codex-thread-id "remote-thread-1"
                       :text "reply"})]
        (codex-agent/handle-message!
         service
         bootstrap-message
         {:on-reply! (fn [_] {:promote-external-session-id "omt_1"})})
        (let [duplicate (codex-agent/handle-message! service bootstrap-message {})]
          (is (= :duplicate (:status duplicate)))
          (is (= "remote-thread-1" (:codex-thread-id duplicate)))
          (is (= 1 @calls)))))))

(deftest busy-session-returns-structured-result
  (testing "a second distinct message while a turn is active does not start another turn"
    (let [path (temp-store-path)
          service (codex-agent/start! {:store-path path
                                       :codex-app-server-service ::fake})
          started (promise)
          release (promise)
          calls (atom 0)]
      (with-redefs [app-server/run-turn!
                    (fn [_ request]
                      (swap! calls inc)
                      ((get-in request [:callbacks :on-thread-started])
                       {:codex-thread-id "remote-thread-1"})
                      (deliver started true)
                      @release
                      {:status :completed
                       :codex-thread-id "remote-thread-1"
                       :text "done"})]
        (let [f (future (codex-agent/handle-message! service (message "m1" "first") {}))]
          @started
          (let [busy (codex-agent/handle-message! service (message "m2" "second") {})]
            (is (= :busy (:status busy)))
            (is (= "remote-thread-1" (:codex-thread-id busy)))
            (is (= 1 @calls)))
          (deliver release true)
          @f)))))

(deftest submit-message-queues-same-session-turns-in-order
  (testing "submitted ordinary messages for one external session run FIFO"
    (let [path (temp-store-path)
          service (codex-agent/start! {:store-path path
                                       :codex-app-server-service ::fake})
          requests (atom [])
          first-started (promise)
          release-first (promise)
          second-finished (promise)]
      (try
        (with-redefs [app-server/run-turn!
                      (fn [_ request]
                        (let [text (get-in request [:input-items 0 :text])]
                          (swap! requests conj request)
                          (case text
                            "first"
                            (do
                              (deliver first-started true)
                              @release-first)

                            "second"
                            (deliver second-finished true)

                            nil)
                          {:status :completed
                           :codex-thread-id "remote-thread-1"
                           :text (str "reply " text)}))]
          (is (= :queued
                 (:status
                  (codex-agent/submit-message! service
                                               (message "m1" "first")
                                               {}))))
          (is (= true (deref first-started 1000 nil)))
          (is (= :queued
                 (:status
                  (codex-agent/submit-message! service
                                               (message "m2" "second")
                                               {}))))
          (is (= ["first"]
                 (mapv #(get-in % [:input-items 0 :text]) @requests)))
          (deliver release-first true)
          (is (= true (deref second-finished 1000 nil)))
          (is (= ["first" "second"]
                 (mapv #(get-in % [:input-items 0 :text]) @requests)))
          (is (= "remote-thread-1"
                 (:codex-thread-id (second @requests)))))
        (finally
          (codex-agent/close! service))))))

(deftest submit-message-runs-different-sessions-in-parallel
  (testing "submitted messages for different external sessions are not globally serialized"
    (let [path (temp-store-path)
          service (codex-agent/start! {:store-path path
                                       :codex-app-server-service ::fake
                                       :submit-worker-count 2})
          chat-1-started (promise)
          chat-2-started (promise)
          release (promise)]
      (try
        (with-redefs [app-server/run-turn!
                      (fn [_ request]
                        (case (:session-key request)
                          "feishu:chat-1" (deliver chat-1-started true)
                          "feishu:chat-2" (deliver chat-2-started true)
                          nil)
                        @release
                        {:status :completed
                         :codex-thread-id (str "remote-" (:session-key request))
                         :text "done"})]
          (is (= :queued
                 (:status
                  (codex-agent/submit-message! service
                                               (message "chat-1" "m1" "first")
                                               {}))))
          (is (= :queued
                 (:status
                  (codex-agent/submit-message! service
                                               (message "chat-2" "m2" "second")
                                               {}))))
          (is (= true (deref chat-1-started 1000 nil)))
          (is (= true (deref chat-2-started 1000 nil)))
          (deliver release true))
        (finally
          (deliver release true)
          (codex-agent/close! service))))))

(deftest stop-session-discards-pending-submitted-messages
  (testing "out-of-band stop interrupts the active turn and clears queued ordinary messages"
    (let [path (temp-store-path)
          service (codex-agent/start! {:store-path path
                                       :codex-app-server-service ::fake})
          first-started (promise)
          release-first (promise)
          first-finished (promise)
          second-started (promise)
          interrupt-requests (atom [])]
      (try
        (with-redefs [app-server/run-turn!
                      (fn [_ request]
                        (let [text (get-in request [:input-items 0 :text])]
                          (case text
                            "first"
                            (do
                              (deliver first-started true)
                              @release-first
                              (deliver first-finished true))

                            "second"
                            (deliver second-started true)

                            nil)
                          {:status :completed
                           :codex-thread-id "remote-thread-1"
                           :text (str "reply " text)}))
                      app-server/interrupt-turn!
                      (fn [_ request]
                        (swap! interrupt-requests conj request)
                        {:interrupted? true
                         :codex-thread-id "remote-thread-1"
                         :codex-turn-id "turn-1"})]
          (codex-agent/submit-message! service (message "m1" "first") {})
          (is (= true (deref first-started 1000 nil)))
          (codex-agent/submit-message! service (message "m2" "second") {})
          (is (= {:channel :feishu
                  :external-session-id "chat-1"
                  :codex-thread-id "remote-thread-1"
                  :status :interrupted
                  :interrupted? true
                  :codex-turn-id "turn-1"
                  :discarded-message-count 1}
                 (codex-agent/stop-session! service (message "m3" "/stop"))))
          (is (= [{:session-key "feishu:chat-1"}]
                 @interrupt-requests))
          (deliver release-first true)
          (is (= true (deref first-finished 1000 nil)))
          (is (= ::not-started (deref second-started 100 ::not-started))))
        (finally
          (deliver release-first true)
          (codex-agent/close! service))))))

(deftest app-server-command-callbacks-are-forwarded-as-events
  (testing "Codex app-server item and command callbacks are exposed through Codex Agent events"
    (let [path (temp-store-path)
          service (codex-agent/start! {:store-path path
                                       :codex-app-server-service ::fake})
          events (atom [])]
      (with-redefs [app-server/run-turn!
                    (fn [_ request]
                      (let [callbacks (:callbacks request)]
                        ((:on-item-started callbacks)
                         {:item-type "commandExecution"
                          :item-id "cmd-1"
                          :command "ls"})
                        ((:on-command-delta callbacks) "apps\n")
                        ((:on-item-completed callbacks)
                         {:item-type "commandExecution"
                          :item-id "cmd-1"
                          :status "completed"
                          :command "ls"
                          :exit-code 0
                          :output "apps\nbases\n"}))
                      {:status :completed
                       :codex-thread-id "remote-thread-1"
                       :text "done"})]
        (codex-agent/handle-message!
         service
         (message "m1" "try use ls")
         {:on-event! #(swap! events conj %)})
        (is (= [{:type :codex/item-started
                 :item-type "commandExecution"
                 :item-id "cmd-1"
                 :command "ls"}
                {:type :codex/command-delta
                 :delta "apps\n"}
                {:type :codex/item-completed
                 :item-type "commandExecution"
                 :item-id "cmd-1"
                 :status "completed"
                 :command "ls"
                 :exit-code 0
                 :output "apps\nbases\n"}]
               @events))))))

(deftest app-server-dynamic-tool-options-are-forwarded
  (testing "Codex Agent can expose per-channel dynamic tools to app-server"
    (let [path (temp-store-path)
          service (codex-agent/start! {:store-path path
                                       :codex-app-server-service ::fake})
          requests (atom [])
          tool-spec {:name "send_feishu_file"
                     :description "Send a local file back to Feishu."
                     :input-schema {:type "object"}}]
      (with-redefs [app-server/run-turn!
                    (fn [_ request]
                      (swap! requests conj request)
                      {:status :completed
                       :codex-thread-id "remote-thread-1"
                       :text "done"})]
        (codex-agent/handle-message!
         service
         (message "m1" "send file")
         {:dynamic-tools [tool-spec]
          :experimental-api? true
          :on-dynamic-tool-call! (fn [_] "ok")})
        (is (= [tool-spec] (:dynamic-tools (first @requests))))
        (is (= true (:experimental-api? (first @requests))))
        (is (fn? (get-in (first @requests) [:callbacks :on-dynamic-tool-call])))))))

(deftest stop-session-interrupts-current-app-server-turn
  (testing "stop-session targets the app-server session key without entering normal message handling"
    (let [path (temp-store-path)
          service (codex-agent/start! {:store-path path
                                       :codex-app-server-service ::fake})
          interrupt-requests (atom [])]
      (with-redefs [app-server/run-turn!
                    (fn [_ request]
                      ((get-in request [:callbacks :on-thread-started])
                       {:codex-thread-id "remote-thread-1"})
                      {:status :completed
                       :codex-thread-id "remote-thread-1"
                       :text "ready"})
                    app-server/interrupt-turn!
                    (fn [_ request]
                      (swap! interrupt-requests conj request)
                      {:interrupted? true
                       :codex-thread-id "remote-thread-1"
                       :codex-turn-id "turn-1"})]
        (codex-agent/handle-message! service (message "m1" "hello") {})
        (is (= {:channel :feishu
                :external-session-id "chat-1"
                :codex-thread-id "remote-thread-1"
                :status :interrupted
                :interrupted? true
                :codex-turn-id "turn-1"}
               (codex-agent/stop-session! service (message "m2" "/stop"))))
        (is (= [{:session-key "feishu:chat-1"}]
               @interrupt-requests))))))

(deftest stop-session-resolves-promoted-bootstrap-alias
  (testing "a stop request for the original bootstrap key interrupts the promoted thread session"
    (let [path (temp-store-path)
          service (codex-agent/start! {:store-path path
                                       :codex-app-server-service ::fake})
          interrupt-requests (atom [])
          bootstrap-message {:channel :feishu
                             :external-session-id "bootstrap:om_1"
                             :external-message-id "om_1"
                             :content [{:type :text :text "hello"}]}]
      (with-redefs [app-server/run-turn!
                    (fn [_ request]
                      ((get-in request [:callbacks :on-thread-started])
                       {:codex-thread-id "remote-thread-1"})
                      {:status :completed
                       :codex-thread-id "remote-thread-1"
                       :text "ready"})
                    app-server/interrupt-turn!
                    (fn [_ request]
                      (swap! interrupt-requests conj request)
                      {:interrupted? true
                       :codex-thread-id "remote-thread-1"
                       :codex-turn-id "turn-1"})]
        (codex-agent/handle-message!
         service
         bootstrap-message
         {:on-reply! (fn [_] {:promote-external-session-id "omt_1"})})
        (is (= "omt_1"
               (:external-session-id
                (codex-agent/stop-session! service bootstrap-message))))
        (is (= [{:session-key "feishu:omt_1"}]
               @interrupt-requests))))))
