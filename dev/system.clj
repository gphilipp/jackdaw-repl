(ns system
  "Functions to start and stop the system, used for interactive
  development.

  These functions are required by the `user` namespace and should not
  be called directly."
  (:require [clojure.string :as str]
            [clojure.tools.logging :as log]
            [clojure.java.shell :refer [sh]]
            [jackdaw.serdes.resolver :as resolver]
            [jackdaw.admin :as ja]
            [poc.core :as poc]))

(def system nil)

(defn kafka-admin-client-config
  []
  {"bootstrap.servers" "localhost:9092"})

(defn create-topics
  "Takes a list of topics and creates these using the names given."
  [topic-config-list]
  (with-open [client (ja/->AdminClient (kafka-admin-client-config))]
    (ja/create-topics! client topic-config-list)))

(defn re-delete-topics
  "Takes an instance of java.util.regex.Pattern and deletes any Kafka
  topics that match."
  [re]
  (with-open [client (ja/->AdminClient (kafka-admin-client-config))]
    (let [topics-to-delete (->> (ja/list-topics client)
                             (filter #(re-find re (:topic-name %))))]
      (ja/delete-topics! client topics-to-delete))))

(defn application-id
  "Takes an application config and returns an `application.id`."
  [app-config]
  (get app-config "application.id"))

(defn destroy-state-stores
  "Takes an application config and deletes local files associated with
  internal state."
  [app-config]
  (sh "rm" "-rf" (str "/tmp/kafka-streams/" (application-id app-config)))
  (log/info "internal state is deleted"))

(defn stop
  "Stops the app, and deletes topics and internal state.
  This functions is required by the `user` namespace and should not
  be called directly."
  []
  (log/info "Stopping the app")
  (when system
    (poc/stop-app (:app system)))
  (log/info "Delete the topics")
  (re-delete-topics (re-pattern (str "("
                                  (->> poc/topic-metadata
                                    keys
                                    (map name)
                                    (str/join "|"))
                                  "|"
                                  (application-id poc/app-config)
                                  ".*)")))
  (log/info "Destroy state stores")
  (destroy-state-stores poc/app-config))

(defn start
  "Creates topics, and starts the app.
  This functions is required by the `user` namespace and should not
  be called directly."
  []
  (with-out-str (stop))
  (log/info "Create topics")
  (create-topics (vals poc/topic-metadata))
  (log/info "Start app")
  {:app (poc/start-app poc/topic-metadata poc/app-config)})
