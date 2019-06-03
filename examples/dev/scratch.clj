(ns scratch
  (:require [jackdaw.repl :refer :all]
            [app.tracker]
            [app.decisioning]
            [clj-uuid]
            [jackdaw.repl :as jr]
            [integrant.repl :refer [clear go halt prep init reset reset-all]]
            [integrant.core :as ig]))

(jr/list-topics)

(def config {:kafka-config {"bootstrap.servers" "localhost:9092"
                            "default.key.serde" "jackdaw.serdes.EdnSerde"
                            "default.value.serde" "jackdaw.serdes.EdnSerde"
                            "cache.max.bytes.buffering" "0"}

             :topic-registry {:kafka-config (ig/ref :kafka-config)}

             [:kafka/streams-app :app/tracker]
             {:app-config (ig/ref :kafka-config)
              :topology-fn 'app.tracker/build-topology!
              :topology-opts {:topic-registry (ig/ref :topic-registry)}}

             [:kafka/streams-app :app/decisioning]
             {:app-config (ig/ref :kafka-config)
              :topology-fn 'app.decisioning/build-topology!
              :topology-opts {:topic-registry (ig/ref :topic-registry)}}})


(integrant.repl/set-prep! (constantly config))
(go)
(def system integrant.repl.state/system)

(def topic-registry (:topic-registry system))

(let [{:keys [data-acquired-topic data-validated-topic foo]} topic-registry]
  [data-validated-topic data-acquired-topic foo])

(halt)

(def data-acquired-topic (:data-acquired topic-registry))
(def data-validated-topic (:data-validated topic-registry))
(def loan-application-topic (:loan-application topic-registry))
(def decision-made-topic (:decision-made topic-registry))

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
(get-keyvals (:tracker-KSTREAM-AGGREGATE-STATE-STORE-0000000001-changelog topic-registry))


(let [kafka-config {"bootstrap.servers" "localhost:9092"}]
  (doseq [topic-name ["data-acquired"
                      "data-validated"
                      "loan-application"
                      "decision-made"]]
    (jr/re-delete-topics kafka-config (re-pattern topic-name))))