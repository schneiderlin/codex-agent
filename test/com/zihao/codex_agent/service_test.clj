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
  [message-id text]
  {:channel :feishu
   :external-session-id "chat-1"
   :external-message-id message-id
   :content [{:type :text :text text}]})

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
