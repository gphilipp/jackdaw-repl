(ns scratch
  (:require [user :refer :all]
            [sc.api :refer :all]
            [poc.tracker]
            [poc.decisioning]
            [poc.system :as system]
            [clj-uuid]))

(list-topics)

(require '[integrant.repl :refer [clear go halt prep init reset reset-all]])
(integrant.repl/set-prep! system/read-config)

(go)
(halt)

(def system integrant.repl.state/system)

(def data-acquired-topic (get-in system [:topic-registry :topic-metadata :data-acquired]))
(def data-validated-topic (get-in system [:topic-registry :topic-metadata :data-validated]))
(def loan-application-topic (get-in system [:topic-registry :topic-metadata :loan-application]))
(def decision-made-topic (get-in system [:topic-registry :topic-metadata :decision-made]))

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