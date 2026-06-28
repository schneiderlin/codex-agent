(ns com.zihao.codex-agent.store-edn
  "EDN-backed Codex Agent session store."
  (:require [clojure.edn :as edn]
            [clojure.java.io :as io])
  (:import [java.nio.file AtomicMoveNotSupportedException Files StandardCopyOption]))

(def store-version 1)

(def empty-root
  {:version store-version
   :sessions {}})

(defrecord EdnStore [path !root lock])

(defn- malformed!
  [message data]
  (throw (ex-info message (assoc data :type :codex-agent/malformed-store))))

(defn validate-root
  [root]
  (when-not (map? root)
    (malformed! "Codex Agent store must be an EDN map" {:root root}))
  (when-not (= store-version (:version root))
    (malformed! "Unsupported Codex Agent store version"
                {:version (:version root)
                 :supported-version store-version}))
  (when-not (map? (:sessions root))
    (malformed! "Codex Agent store :sessions must be a map"
                {:sessions (:sessions root)}))
  root)

(defn load-root
  [path]
  (let [f (io/file path)]
    (if-not (.exists f)
      empty-root
      (try
        (validate-root (edn/read-string (slurp f)))
        (catch clojure.lang.ExceptionInfo e
          (throw e))
        (catch Throwable t
          (throw (ex-info "Failed to read Codex Agent store EDN"
                          {:type :codex-agent/malformed-store
                           :path (.getPath f)
                           :throwable t})))))))

(defn- ensure-parent-dir!
  [^java.io.File f]
  (when-let [parent (.getParentFile f)]
    (.mkdirs parent)))

(defn atomic-write-root!
  [path root]
  (validate-root root)
  (let [f (io/file path)
        _ (ensure-parent-dir! f)
        parent (or (.getParentFile f) (io/file "."))
        tmp-path (Files/createTempFile (.toPath parent)
                                       (str (.getName f) ".")
                                       ".tmp"
                                       (make-array java.nio.file.attribute.FileAttribute 0))
        body (str (pr-str root) "\n")]
    (try
      (spit (.toFile tmp-path) body)
      (try
        (Files/move tmp-path
                    (.toPath f)
                    (into-array StandardCopyOption
                                [StandardCopyOption/ATOMIC_MOVE
                                 StandardCopyOption/REPLACE_EXISTING]))
        (catch AtomicMoveNotSupportedException _
          (Files/move tmp-path
                      (.toPath f)
                      (into-array StandardCopyOption
                                  [StandardCopyOption/REPLACE_EXISTING]))))
      root
      (finally
        (try
          (Files/deleteIfExists tmp-path)
          (catch Throwable _))))))

(defn open!
  [{:keys [path]}]
  (when-not path
    (throw (ex-info "Codex Agent store path required"
                    {:type :codex-agent/missing-store-path})))
  (->EdnStore path (atom (load-root path)) (Object.)))

(defn snapshot
  [^EdnStore store]
  @(:!root store))

(defn get-session
  [store session-key]
  (get-in (snapshot store) [:sessions session-key]))

(defn write-root!
  [^EdnStore store root]
  (locking (:lock store)
    (let [root* (validate-root root)]
      (atomic-write-root! (:path store) root*)
      (reset! (:!root store) root*)
      root*)))

(defn update-session!
  [^EdnStore store session-key f]
  (locking (:lock store)
    (let [root @(:!root store)
          current (get-in root [:sessions session-key])
          session* (f current)
          root* (assoc-in root [:sessions session-key] session*)]
      (atomic-write-root! (:path store) root*)
      (reset! (:!root store) root*)
      session*)))

(defn assoc-session!
  [store session-key session]
  (update-session! store session-key (constantly session)))
