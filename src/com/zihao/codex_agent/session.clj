(ns com.zihao.codex-agent.session
  "Pure session and message helpers for Codex Agent."
  (:require [clojure.string :as str]))

(def default-processed-message-limit 200)

(defn session-key
  [{:keys [channel external-session-id]}]
  (when-not (keyword? channel)
    (throw (ex-info "channel must be a keyword"
                    {:type :codex-agent/invalid-message
                     :field :channel
                     :value channel})))
  (when (str/blank? (str (or external-session-id "")))
    (throw (ex-info "external-session-id required"
                    {:type :codex-agent/invalid-message
                     :field :external-session-id})))
  [channel (str external-session-id)])

(defn now-iso []
  (str (java.time.Instant/now)))

(defn new-session
  [{:keys [channel external-session-id]} now]
  {:channel channel
   :external-session-id (str external-session-id)
   :codex-thread-id nil
   :status :idle
   :created-at now
   :updated-at now
   :last-external-message-id nil
   :last-delivery-status nil
   :processed-message-ids []})

(defn processed-message?
  [session external-message-id]
  (and (some? external-message-id)
       (contains? (set (:processed-message-ids session))
                  (str external-message-id))))

(defn remember-message-id
  ([session external-message-id]
   (remember-message-id session external-message-id default-processed-message-limit))
  ([session external-message-id limit]
   (if (str/blank? (str (or external-message-id "")))
     session
     (let [message-id (str external-message-id)
           existing (vec (remove #{message-id} (:processed-message-ids session)))
           ids (conj existing message-id)
           n (count ids)
           bounded (if (> n limit)
                     (vec (subvec ids (- n limit)))
                     ids)]
       (assoc session
              :last-external-message-id message-id
              :processed-message-ids bounded)))))

(defn mark-status
  [session status now]
  (assoc session
         :status status
         :updated-at now))

(defn set-codex-thread-id
  [session codex-thread-id now]
  (cond-> (assoc session :updated-at now)
    (not (str/blank? (str (or codex-thread-id ""))))
    (assoc :codex-thread-id (str codex-thread-id))))

(defn set-delivery-status
  [session status now]
  (assoc session
         :last-delivery-status status
         :updated-at now))

(defn content->input-items
  [content]
  (let [items (->> (or content [])
                   (keep (fn [item]
                           (let [item-type (:type item)]
                             (cond
                               (#{:text "text"} item-type)
                               {:type "text"
                                :text (str (or (:text item) ""))
                                :text_elements []}

                               (#{:image "image"} item-type)
                               (when-not (str/blank? (str (or (:url item) "")))
                                 {:type "image"
                                  :url (:url item)})

                               :else nil))))
                   vec)]
    (when-not (seq items)
      (throw (ex-info "Codex Agent message content must include at least one supported item"
                      {:type :codex-agent/invalid-message
                       :field :content})))
    items))
