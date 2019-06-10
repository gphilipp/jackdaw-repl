(ns jackdaw.repl
  "Functions to start and stop the system, used for interactive
  development.

  These functions are required by the `user` namespace and should not
  be called directly."
  (:require [clojure.string :as str]
            [clojure.tools.logging :as log]
            [clojure.java.shell :refer [sh]]
            [jackdaw.serdes.resolver :as resolver]
            [jackdaw.admin :as ja]
            [jackdaw.client.log :as jcl]
            [integrant.core :as ig]
            [clojure.java.io :as io]
            [jackdaw.streams :as j]
            [jackdaw.client :as jc]
            [jackdaw.streams :as js]
            [hazard.core :as hazard])
  (:import (clojure.lang ILookup)))


(def kafka-broker-config {"bootstrap.servers" "localhost:9092"})


(defn kafka-admin-client-config []
  kafka-broker-config)


(defn kafka-producer-config []
  kafka-broker-config)


(defn kafka-consumer-config
  [group-id]
  (merge kafka-broker-config
         {"group.id" group-id
          "auto.offset.reset" "earliest"
          "enable.auto.commit" "false"}))


(defn publish
  "Takes a topic config and record value, and (optionally) a key and
  parition number, and produces to a Kafka topic."
  ([topic-config value]
   (with-open [client (jc/producer (kafka-producer-config) topic-config)]
     @(jc/produce! client topic-config value))
   nil)

  ([topic-config key value]
   (with-open [client (jc/producer (kafka-producer-config) topic-config)]
     @(jc/produce! client topic-config key value))
   nil)

  ([topic-config partition key value]
   (with-open [client (jc/producer (kafka-producer-config) topic-config)]
     @(jc/produce! client topic-config partition key value))
   nil))


(defn get-records
  "Takes a topic config, consumes from a Kafka topic, and returns a
  seq of maps."
  ([topic-config]
   (get-records topic-config 200))

  ([topic-config polling-interval-ms]
   (let [client-config (kafka-consumer-config
                         (str/join "-" (hazard/words 3)))]
     (with-open [client (jc/subscribed-consumer client-config
                                                [topic-config])]
       (doall (jcl/log client 100 seq))))))


(defn get-keyvals
  "Takes a topic config, consumes from a Kafka topic, and returns a
  seq of key-value pairs."
  ([topic-config]
   (get-keyvals topic-config 20))

  ([topic-config polling-interval-ms]
   (map (juxt :key :value) (get-records topic-config polling-interval-ms))))


(defn create-topic
  "Takes a topic config and creates a Kafka topic."
  [topic-config]
  (with-open [client (ja/->AdminClient (kafka-admin-client-config))]
    (ja/create-topics! client [topic-config])))


(defn create-topics
  "Takes a list of topics and creates these using the names given."
  [kafka-config topic-list]
  (log/infof "Creating topics %s" (clojure.string/join "," (mapv :topic-name topic-list)))
  (with-open [client (ja/->AdminClient kafka-config)]
    (ja/create-topics! client topic-list)))


(defn topic-exists?
  "Takes a topic name and returns true if the topic exists."
  [topic-config]
  (with-open [client (ja/->AdminClient (kafka-admin-client-config))]
    (ja/topic-exists? client topic-config)))


(defn list-topics
  "Returns a list of Kafka topics."
  []
  (with-open [client (ja/->AdminClient (kafka-admin-client-config))]
    (ja/list-topics client)))


(defn re-delete-topics
  "Takes an instance of java.util.regex.Pattern and deletes any Kafka
  topics that match."
  [kafka-config re]
  (log/infof "Deleting topics matching %s" re)
  (with-open [client (ja/->AdminClient kafka-config)]
    (let [topics-to-delete (->> (ja/list-topics client)
                             (filter #(re-find re (:topic-name %))))]
      (ja/delete-topics! client topics-to-delete))))


(defn destroy-state-stores
  "Takes an application config and deletes local files associated with
  internal state."
  [application-id]
  (log/infof "Deleting internal state for %s" application-id)
  (sh "rm" "-rf" (str "/tmp/kafka-streams/" application-id))
  (log/info "internal state is deleted"))


(defn make-application-id [key]
  (-> key second name))


(defn make-topic-metadata [topics]
  (reduce #(assoc %1 (-> %2 :topic-name keyword) %2) {} topics))


(defn create-and-resolve-topic [topic-key]
  (let [metadata {:topic-name (name topic-key)
                  :partition-count 1
                  :replication-factor 1
                  :key-serde {:serde-keyword :jackdaw.serdes.edn/serde}
                  :value-serde {:serde-keyword :jackdaw.serdes.edn/serde}}]
    (let [resolve-serde (resolver/serde-resolver)
          topic-config  (assoc metadata
                               :key-serde (resolve-serde (:key-serde metadata))
                               :value-serde (resolve-serde (:value-serde metadata)))]
      (do
        (if (not (topic-exists? topic-config))
          (create-topic topic-config))
        topic-config))))


(defn- make-exception-handler! []
  (reify Thread$UncaughtExceptionHandler
    (uncaughtException [_ t e]
      (log/fatalf e "topology threw an uncaught exception on thread %s" t))))


(defn start-app! [builder kafka-config]
  (doto (js/kafka-streams builder kafka-config)
    (.setUncaughtExceptionHandler (make-exception-handler!))
    (.start)))


(defmethod ig/init-key :kafka/streams-app [[_ app-name]
                                           {:keys [app-config
                                                   builder-fn
                                                   topology-fn
                                                   topology-opts]
                                            :or {builder-fn 'jackdaw.streams/streams-builder}}]
  (log/infof "starting %s streams app" app-name)
  (let [builder-fun    (resolve builder-fn)
        topology       (resolve topology-fn)
        builder        (builder-fun)
        application-id (str/join "-" (hazard/words 3))
        app-config     (assoc app-config "application.id" application-id)]
    {:application-id application-id
     :app-config app-config
     :streams-app (-> (topology builder topology-opts)
                      (start-app! app-config))}))


(defmethod ig/halt-key! :kafka/streams-app [_ {:keys [application-id streams-app app-config]}]
  (log/infof "class of streamsapp is %s" (class streams-app))
  (j/close streams-app)
  (.cleanUp streams-app)
  #_(destroy-state-stores application-id)
  (re-delete-topics app-config (re-pattern (str application-id ".*"))))


(defprotocol TopicRegistry
  (stop [this]))


(deftype MockTopicRegistry [kafka-config topics]
  ILookup
  (valAt [this topic-key]
    (let [topic (create-and-resolve-topic topic-key)]
      (swap! topics assoc topic-key topic)
      topic))

  TopicRegistry
  (stop [this]
    (log/infof "Topics recorded so far %s" @topics)
    (doseq [topic (vals @topics)]
      (re-delete-topics kafka-config
                        (re-pattern (str (->> topic :topic-name)))))))


(defmethod ig/init-key :topic-registry
  [_ {:keys [kafka-config]}]
  (->MockTopicRegistry kafka-config (atom {})))


(defmethod ig/init-key :kafka-config
  [_ opts]
  opts)


(defmethod ig/halt-key! :topic-registry
  [_ topic-registry]
  (stop topic-registry))

