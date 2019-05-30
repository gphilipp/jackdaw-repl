(ns scratch
  (:require [user :refer :all]
            [sc.api :refer :all]
            [poc.tracker]
            [poc.decisioner]
            [poc.system :as system]
            [clj-uuid]
            [jackdaw.dev :as jd]
            [integrant.repl :refer [clear go halt prep init reset reset-all]]
            [integrant.core :as ig]))

(list-topics)

;; this topic-metadata works in a wishful thinking way.
;; `get` a topic keyword from it will create the topic and return
;; a proper jackdaw config map for it.
(def topic-metadata (jd/dynamic-topic-metadata))

(def kafka {"bootstrap.servers" "localhost:9092"
            "default.key.serde" "jackdaw.serdes.EdnSerde"
            "default.value.serde" "jackdaw.serdes.EdnSerde"
            "cache.max.bytes.buffering" "0"})

(def config {[:kafka/streams-app :app/tracker]
             {:app-config kafka
              :topic-metadata topic-metadata
              :topology-fn 'poc.tracker/topology-builder}

             [:kafka/streams-app :app/decisioning]
             {:app-config kafka
              :topic-metadata topic-metadata
              :topology-fn 'poc.decisioner/topology-builder}})

(integrant.repl/set-prep! (constantly config))

(let [{:keys [data-acquired-topic data-validated-topic foo]} dynamic-topic-metadata]
  [data-validated-topic data-acquired-topic foo])

(go)
(halt)

(def system integrant.repl.state/system)




(def data-acquired-topic (:data-acquired dynamic-topic-metadata))
(def data-validated-topic (:data-validated-topic dynamic-topic-metadata))
(def loan-application-topic (:loan-application-topic dynamic-topic-metadata))
(def decision-made-topic (:decision-made-topic dynamic-topic-metadata))

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
(get-keyvals data-validated-topic)
(get-keyvals decision-made-topic)


(let [kafka-config {"bootstrap.servers" "localhost:9092"}]
  (doseq [topic-name ["data-acquired" "data-validated" "loan-application" "decision-made"]]
    (system/re-delete-topics kafka-config (re-pattern topic-name))))