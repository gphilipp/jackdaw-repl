(ns example.tracker
  (:require [clojure.spec.alpha :as s]
            [clojure.tools.logging :as log]
            [clojure.spec.alpha :as s]
            [jackdaw.streams :as streams]
            [jackdaw.streams :as j]))


(defn- merge-state
  [aggregate [_ v]]
  (update aggregate :loan-application-state merge (:data v)))

(defn printer [kstream]
  (j/peek kstream println))


(defn topology-builder
  [topic-metadata]
  (let [builder (j/streams-builder)]
    (do
      (let [data-acquired (j/kstream builder (:data-acquired topic-metadata))]
        (-> data-acquired
          (j/group-by-key)
          (streams/aggregate hash-map merge-state)
          (streams/to-kstream)
          (j/to (:loan-application topic-metadata)))
        (-> data-acquired
          printer
          (j/map-values (fn [[k v]] [k (assoc v :status :validated)]))
          (j/to (:data-validated topic-metadata))))
      builder)))


(s/def ::id uuid?)
(s/def ::loan-application-id uuid?)
(s/def ::data-key string?)
(s/def ::data-entered (s/keys :req-un [::id
                                       ::loan-application-id]))
