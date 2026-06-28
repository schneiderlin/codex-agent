(ns com.zihao.codex-agent.store-edn
  "EDN-backed Codex Agent session store."
  (:require [clojure.edn :as edn]
            [clojure.java.io :as io])
  (:import [java.nio.file Files StandardCopyOption]))

(def store-version 1)

(def empty-root
  {:version store-version
   :sessions {}
   :aliases {}})

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
  (when (and (contains? root :aliases)
             (not (map? (:aliases root))))
    (malformed! "Codex Agent store :aliases must be a map"
                {:aliases (:aliases root)}))
  (assoc root :aliases (or (:aliases root) {})))

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
        (catch Exception e
          (if (= "java.nio.file.AtomicMoveNotSupportedException"
                 (.getName (class e)))
            (Files/move tmp-path
                        (.toPath f)
                        (into-array StandardCopyOption
                                    [StandardCopyOption/REPLACE_EXISTING]))
            (throw e))))
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

(defn resolve-session-key
  [root session-key]
  (loop [k session-key
         seen #{}]
    (cond
      (contains? seen k)
      (malformed! "Codex Agent store aliases contain a cycle"
                  {:session-key session-key
                   :cycle-key k})

      (contains? (:aliases root) k)
      (recur (get (:aliases root) k) (conj seen k))

      :else
      k)))

(defn canonical-session-key
  [store session-key]
  (resolve-session-key (snapshot store) session-key))

(defn get-session
  [store session-key]
  (let [root (snapshot store)]
    (get-in root [:sessions (resolve-session-key root session-key)])))

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
    (let [root (validate-root @(:!root store))
          canonical-key (resolve-session-key root session-key)
          current (get-in root [:sessions canonical-key])
          session* (f current)
          root* (assoc-in root [:sessions canonical-key] session*)]
      (atomic-write-root! (:path store) root*)
      (reset! (:!root store) root*)
      session*)))

(defn assoc-session!
  [store session-key session]
  (update-session! store session-key (constantly session)))

(defn- rewrite-alias-targets
  [aliases from-key to-key]
  (reduce-kv (fn [m k v]
               (assoc m k (if (= from-key v) to-key v)))
             {}
             aliases))

(defn promote-session!
  ([store from-key to-key]
   (promote-session! store from-key to-key identity))
  ([^EdnStore store from-key to-key f]
   (locking (:lock store)
     (let [root (validate-root @(:!root store))
           from-canonical (resolve-session-key root from-key)
           to-canonical (resolve-session-key root to-key)
           source-session (get-in root [:sessions from-canonical])
           target-session (get-in root [:sessions to-canonical])]
       (when-not source-session
         (throw (ex-info "Cannot promote missing Codex Agent session"
                         {:type :codex-agent/missing-session
                          :from-key from-key
                          :canonical-from-key from-canonical})))
       (when (and target-session
                  (not= from-canonical to-canonical)
                  (not= (:codex-thread-id source-session)
                        (:codex-thread-id target-session)))
         (throw (ex-info "Cannot promote Codex Agent session over a different Codex thread"
                         {:type :codex-agent/session-promotion-conflict
                          :from-key from-key
                          :to-key to-key
                          :from-codex-thread-id (:codex-thread-id source-session)
                          :to-codex-thread-id (:codex-thread-id target-session)})))
       (let [session* (f (or target-session source-session))
             aliases* (-> (:aliases root)
                          (rewrite-alias-targets from-canonical to-key)
                          (assoc from-key to-key
                                 from-canonical to-key)
                          (dissoc to-key))
             sessions* (cond-> (:sessions root)
                         (not= from-canonical to-key) (dissoc from-canonical)
                         true (assoc to-key session*))
             root* (assoc root
                          :sessions sessions*
                          :aliases aliases*)]
         (atomic-write-root! (:path store) root*)
         (reset! (:!root store) root*)
         session*)))))
