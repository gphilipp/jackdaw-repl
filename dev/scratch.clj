(ns scratch
  (:require [user :refer :all]
            [sc.api :refer :all]
            [poc.tracker]
            [poc.decisioning]
            [poc.system :as system]
            [clj-uuid]
            [integrant.repl :refer [clear go halt prep init reset reset-all]]
            [integrant.core :as ig]))

(list-topics)

(defn create-and-resolve-topic [topic-key]
  (let [metadata {:topic-name (name topic-key)
                  :partition-count 1
                  :replication-factor 1
                  :key-serde {:serde-keyword :jackdaw.serdes.edn/serde}
                  :value-serde {:serde-keyword :jackdaw.serdes.edn/serde}}]
    (assoc metadata
           :key-serde (poc.system/resolve-serde (:key-serde metadata))
           :value-serde (poc.system/resolve-serde (:value-serde metadata)))))

(def topics (atom {}))

(def p (proxy [clojure.lang.ILookup] []
         (valAt [x]
           (let [new-topic (create-and-resolve-topic x)]
             (swap! topics assoc x new-topic)
             new-topic))))

(get p :data-acquired)

(def kafka {"bootstrap.servers" "localhost:9092"
            "default.key.serde" "jackdaw.serdes.EdnSerde"
            "default.value.serde" "jackdaw.serdes.EdnSerde"
            "cache.max.bytes.buffering" "0"})

(def config {[:kafka/streams-app :app/tracker]
             {:app-config kafka
              :topic-metadata p
              :topology-fn 'poc.tracker/topology-builder}

             [:kafka/streams-app :app/decisioning]
             {:app-config kafka
              :topic-metadata p
              :topology-fn 'poc.decisioning/topology-builder}})

(integrant.repl/set-prep! (constantly config))

(let [{:keys [data-acquired-topic data-validated-topic foo]} p]
  [data-validated-topic data-acquired-topic  foo])

(go)
(halt)

(def system integrant.repl.state/system)




(def data-acquired-topic (:data-acquired p))
(def data-validated-topic (:data-validated-topic p))
(def loan-application-topic (:loan-application-topic p))
(def decision-made-topic (:decision-made-topic p))

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