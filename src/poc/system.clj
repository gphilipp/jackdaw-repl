(ns poc.system
  "Functions to start and stop the system, used for interactive
  development.

  These functions are required by the `user` namespace and should not
  be called directly."
  (:require [clojure.string :as str]
            [clojure.tools.logging :as log]
            [clojure.java.shell :refer [sh]]
            [jackdaw.serdes.resolver :as resolver]
            [jackdaw.admin :as ja]
            [poc.tracker :as poc]
            [integrant.core :as ig]
            [clojure.java.io :as io]
            [jackdaw.streams :as j]))


(def resolve-serde
  (resolver/serde-resolver))


(defn create-topics
  "Takes a list of topics and creates these using the names given."
  [kafka-config topic-list]
  (log/infof "Creating topics %s" (clojure.string/join "," (mapv :topic-name topic-list)))
  (with-open [client (ja/->AdminClient kafka-config)]
    (ja/create-topics! client topic-list)))


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


(defmethod ig/init-key :kafka/streams-app [key {:keys [topic-registry topology-fn app-config]}]
  (let [
        application-id (make-application-id key)
        app            (j/kafka-streams
                         ((resolve topology-fn)
                          (:topic-metadata topic-registry))
                         (assoc app-config "application.id" application-id))]
    (j/start app)
    (log/infof "%s app is up" key)
    {:application-id application-id
     :streams-app app
     :app-config app-config}))


(defmethod ig/halt-key! :kafka/streams-app [_ {:keys [application-id streams-app app-config]}]
  (re-delete-topics app-config (re-pattern (str application-id ".*")))
  (destroy-state-stores application-id)
  (j/close streams-app))


(defmethod ig/init-key :kafka/topic [_ opts]
  (assoc opts
    :key-serde (resolve-serde (:key-serde opts))
    :value-serde (resolve-serde (:value-serde opts))))


(defmethod ig/init-key :kafka [_ opts] opts)


(defmethod ig/init-key :topic-registry [_ {:keys [kafka-config topics]}]
  (let [topic-metadata (make-topic-metadata topics)]
    (create-topics kafka-config (vals topic-metadata))
    {:topic-metadata topic-metadata
     :kafka-config kafka-config}))


(defmethod ig/halt-key! :topic-registry [key {:keys [topic-metadata kafka-config]}]
  (doseq [topic (vals topic-metadata)]
    (re-delete-topics kafka-config (re-pattern (str (->> topic :topic-name))))))

(defn read-config []
  (-> "config.edn"
    (io/resource)
    (slurp)
    (ig/read-string)))

(comment

  (require '[user :refer :all])
  ;(require '[sc.api :refer :all])

  (list-topics)

  (require '[integrant.repl :refer [clear go halt prep init reset reset-all]])
  (integrant.repl/set-prep! read-config)

  (go)

  (def system integrant.repl.state/system)

  (halt)
  (reset)


  (def data-acquired-topic (get-in system [:topic-registry :topic-metadata :data-acquired]))
  (def loan-application-topic (get-in system [:topic-registry :topic-metadata :loan-application]))

  (let [loan-application-id (clj-uuid/v4)]

    (publish data-acquired-topic loan-application-id {:id (clj-uuid/v4)
                                                      :loan-application-id loan-application-id
                                                      :data {"company-house-number" "1234567"}
                                                      :form-id "kyc-1"})

    ;; As soon as possible
    ;; Within the next few weeks
    ;; Just checking for the future
    (publish data-acquired-topic loan-application-id {:id (clj-uuid/v4)
                                                      :loan-application-id loan-application-id
                                                      :data {"When are you looking to take funds?" "As soon as possible"}
                                                      :form-id "risk-splitter-1"}))

  (get-keyvals data-acquired-topic)
  (get-keyvals loan-application-topic)


  (let [kafka-config {"bootstrap.servers" "localhost:9092"}]
    (doseq [topic-name ["data-acquired" "loan-application"]]
      (re-delete-topics kafka-config (re-pattern topic-name))))

  )
