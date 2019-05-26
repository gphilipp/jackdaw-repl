(ns poc.tracker
  (:require [clojure.spec.alpha :as s]
            [clojure.tools.logging :as log]
            [clj-uuid :as uuid]
            [jackdaw.serdes.resolver :as resolver]
            [clojure.spec.alpha :as s]
            [jackdaw.streams :as streams]
            [clojure.spec.gen.alpha :as sgen]
            [clojure.test.check.generators :as gen]
            [integrant.core :as ig]
            [jackdaw.streams :as j]
            [clojure.java.io :as io]))


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


(defn- merge-state
  [aggregate [_ v]]
  (println "AGGR " v "into" aggregate)
  (update aggregate :data merge (:data v)))

(defn printer [kstream]
  (j/peek kstream (fn [[k v]] (log/info (str {:key k :value v})))))


(defn topology-builder
  [topic-metadata]
  (let [builder (j/streams-builder)]
    (do
      (-> (j/kstream builder (:data-acquired topic-metadata))
        (j/group-by-key)
        (streams/aggregate hash-map merge-state)
        (streams/to-kstream)
        (j/to (:loan-application topic-metadata)))
      builder)))





(s/def ::id uuid?)
(s/def ::loan-application-id uuid?)
(s/def ::data-key string?)
(s/def ::data-entered (s/keys :req-un [::id
                                       ::loan-application-id]))
