(ns app.tracker
  (:require [clojure.spec.alpha :as s]
            [clojure.spec.alpha :as s]
            [jackdaw.streams :as j]))


(defn- merge-state
  [aggregate [_ v]]
  (update aggregate :loan-application-state merge (:data v)))


(defn topology-builder
  [{:keys [data-acquired
           data-validated
           loan-application]}]
  (let [builder (j/streams-builder)]
    (do
      (let [data-acquired (j/kstream builder data-acquired)]
        (-> data-acquired
            (j/group-by-key)
            (j/aggregate hash-map merge-state)
            (j/to-kstream)
            (j/to loan-application))
        (-> data-acquired
            (j/map-values (fn [[k v]] [k (assoc v :status :validated)]))
            (j/to data-validated)))
      builder)))


(s/def ::id uuid?)
(s/def ::loan-application-id uuid?)
(s/def ::data-key string?)
(s/def ::data-entered (s/keys :req-un [::id
                                       ::loan-application-id]))
