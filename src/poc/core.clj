(ns poc.core
  (:require [clojure.spec.alpha :as s]
            [clojure.tools.logging :as log]
            [clj-uuid :as uuid]
            [jackdaw.serdes.resolver :as resolver]
            [clojure.spec.alpha :as s]
            [jackdaw.streams :as streams]
            [clojure.spec.gen.alpha :as sgen]
            [clojure.test.check.generators :as gen]
            [jackdaw.streams :as j]))


(defn get-env [k default]
  (get (System/getenv) k default))


(def bootstrap-servers (get-env "BOOTSTRAP_SERVERS" "localhost:9092"))


(def ^{:const true
       :doc "A topic metadata map.

  Provides all the information needed to create the topics used by the
  application. It also describes the serdes used to read and write to
  the topics."}

  +topic-metadata+

  {:data-acquired
   {:topic-name "data-acquired"
    :partition-count 1
    :replication-factor 1
    :key-serde {:serde-keyword :jackdaw.serdes.edn/serde}
    :value-serde {:serde-keyword :jackdaw.serdes.edn/serde}}

   :loan-application
   {:topic-name "loan-application"
    :partition-count 1
    :replication-factor 1
    :key-serde {:serde-keyword :jackdaw.serdes.edn/serde}
    :value-serde {:serde-keyword :jackdaw.serdes.edn/serde}}})

(def resolve-serde
  (resolver/serde-resolver))

(def topic-metadata
  (reduce-kv (fn [m k v]
               (assoc m k
                 (assoc v
                   :key-serde (resolve-serde (:key-serde v))
                   :value-serde (resolve-serde (:value-serde v)))))
    {}
    +topic-metadata+))


(def app-config
  "Returns the application config."
  {"application.id" "poc"
   "bootstrap.servers" bootstrap-servers
   "default.key.serde" "jackdaw.serdes.EdnSerde"
   "default.value.serde" "jackdaw.serdes.EdnSerde"
   "cache.max.bytes.buffering" "0"})


(defn- process-customer-data
  [{:keys [base-name]}]
  (fn [{:keys [id loan-application-id unique-company-identifier parent-records]
        :as v}]
    {:pre [(s/valid? :identifier/originations-company-identifier-entered v)]}
    (let [application-ns "3b00f477-ba8f-489b-9d06-42d04e7ebcf2"]
      {:id (uuid/v5 application-ns id)
       :published-at (System/currentTimeMillis)
       :published-by base-name
       :loan-application-id loan-application-id
       :unique-company-identifier unique-company-identifier
       :parent-records (conj parent-records
                         {:parent-record-type "originations-company-identifier-entered"
                          :parent-record-id id})})))


(defn- merge-state
  [aggregate [_ v]]
  (println "AGGR " v "into" aggregate)
  (update aggregate :data merge (:data v)))

(defn printer [kstream]
  (j/peek kstream (fn [[k v]] (log/info (str {:key k :value v})))))


(defn topology
  [builder topic-metadata]
  (do
    (-> (j/kstream builder (:data-acquired topic-metadata))
      (j/group-by-key)
      (streams/aggregate hash-map merge-state)
      (streams/to-kstream)
      (j/to (:loan-application topic-metadata)))
    builder))


(defn start-app
  "Starts the stream processing application."
  [topic-metadata app-config]
  (let [app (-> (j/streams-builder)
              (topology topic-metadata)
              (j/kafka-streams app-config))]
    (j/start app)
    (log/info "app is up")
    app))



(defn stop-app
  "Stops the stream processing application."
  [app]
  (j/close app)
  (log/info "app is down."))



(s/def ::id uuid?)
(s/def ::loan-application-id uuid?)
(s/def ::data-key string?)
(s/def ::data-entered (s/keys :req-un [::id
                                       ::loan-application-id]))




(comment

  (gen/sample (gen/elements #{"When are you looking to take funds?" "Company House Number" "How old are you?"}) 1)

  (require '[user :refer :all])

  (reset)

  (def data-acquired-topic (:data-acquired topic-metadata))
  (def loan-application-topic (:loan-application topic-metadata))

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

  (s/exercise ::data-entered)

  )

