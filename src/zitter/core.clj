(ns zitter.core
  (:require [twitter-streaming-client.core :as client]
            [twitter.oauth :as oauth]
            [clojure.core.async :refer [thread]]
            [org.httpkit.client :as http]
            [clojure.data.json :as json]))


(def consumer-key "REPLACE-ME")
(def consumer-secret "REPLACE-ME")
(def user-access-token "REPLACE-ME")
(def user-access-token-secret "REPLACE-ME")


(def creds (oauth/make-oauth-creds consumer-key consumer-secret
                                   user-access-token user-access-token-secret))

;; create the client with a twitter.api streaming method and params of your choice
(def stream  (client/create-twitter-stream twitter.api.streaming/statuses-filter
                                           :oauth-creds creds :params {:track "#GE2015"}))

;; fire up the client and start collecting tweets and other messages
(client/start-twitter-stream stream)

;; returns a hash of all collected message queues. empties the queues
;; keys are message-types: :tweet, :delete, :scrub_geo, :limit, :unkown
;; values are vectors of JSON decoded messages
(def q (client/retrieve-queues stream))

;; cancel the client : stop collecting tweets
(client/cancel-twitter-stream stream)



(def process? (atom false))
(reset! process? true)
(reset! process? false)
(thread
  (while @process?
    (let [q (client/retrieve-queues stream)
          tweets (:tweet q)
          t-format "EEE MMM dd HH:mm:ss ZZZZZ yyyy"
          df (doto (java.text.SimpleDateFormat. t-format java.util.Locale/ENGLISH)
               (.setLenient true))
          f (fn [m]
              {:timestamp (->> (:created_at m) (.parse df) (.getTime))
               :sourceId (get-in m [:user :name])
               :eventName "tweet"
               :twitter-user (get-in m [:user :name])
               :location (get-in m [:user :location])
               :tweet (:text m)})
          extracted-tweet-info (map f tweets)]
       (println extracted-tweet-info)
       (when (first extracted-tweet-info)
         (http/post "http://localhost:9000/v1/events"
                    {:headers {"Content-Type" "application/json"}
                     :body         (json/write-str extracted-tweet-info)})))
    (Thread/sleep 1000)))


(keys q)

(-> q
    :tweet
    first
    :user
    :name)

(defn extract [m]
  {:twitter-user (get-in m [:user :name] ) :location (get-in m [:user :location] ) :tweet (:text m)})
(map extract (:tweet q))
