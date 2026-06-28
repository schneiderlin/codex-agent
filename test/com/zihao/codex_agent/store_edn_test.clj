(ns com.zihao.codex-agent.store-edn-test
  (:require [clojure.edn :as edn]
            [clojure.java.io :as io]
            [clojure.test :refer [deftest is testing]]
            [com.zihao.codex-agent.session :as session]
            [com.zihao.codex-agent.store-edn :as sut]))

(defn- temp-store-path
  []
  (let [dir (doto (io/file "tmp" "codex-agent-store-tests" (str (java.util.UUID/randomUUID)))
              (.mkdirs))]
    (.getPath (io/file dir "sessions.edn"))))

(deftest load-root-missing-file-test
  (testing "missing stores start as an empty versioned root"
    (is (= sut/empty-root
           (sut/load-root (temp-store-path))))))

(deftest load-root-malformed-file-test
  (testing "malformed EDN is reported as a Codex Agent store error"
    (let [path (temp-store-path)]
      (spit path "{:version")
      (is (thrown-with-msg?
           clojure.lang.ExceptionInfo
           #"Failed to read Codex Agent store EDN"
           (sut/load-root path))))))

(deftest update-session-persists-atomically-test
  (testing "session upsert writes the versioned EDN root"
    (let [path (temp-store-path)
          store (sut/open! {:path path})
          key [:feishu "chat-1"]]
      (sut/update-session!
       store
       key
       (fn [_]
         (-> (session/new-session {:channel :feishu
                                   :external-session-id "chat-1"}
                                  "t0")
             (session/set-codex-thread-id "remote-thread-1" "t1"))))
      (let [root (edn/read-string (slurp path))]
        (is (= 1 (:version root)))
        (is (= "remote-thread-1"
               (get-in root [:sessions key :codex-thread-id])))))))

(deftest remember-message-id-bounds-processed-cache-test
  (testing "processed-message-ids is an idempotency cache, not an audit log"
    (let [session (reduce (fn [s id]
                            (session/remember-message-id s id 3))
                          (session/new-session {:channel :feishu
                                                :external-session-id "chat-1"}
                                               "t0")
                          ["m1" "m2" "m3" "m4"])]
      (is (= ["m2" "m3" "m4"]
             (:processed-message-ids session)))
      (is (true? (session/processed-message? session "m3")))
      (is (false? (session/processed-message? session "m1"))))))
