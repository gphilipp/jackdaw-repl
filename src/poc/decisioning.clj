(ns poc.decisioning
  (:require [clojure.spec.alpha :as s]
            [clojure.tools.logging :as log]
            [clojure.spec.alpha :as s]

            [jackdaw.streams :as streams]
            [jackdaw.streams :as j]
            [sc.api]
            ))

(defn- merge-state
  [aggregate [_ v]]
  (update aggregate :data merge (:data v)))

(defn printer [kstream]
  (j/peek kstream #(log/infof "k: %s v: %s" %1 %2)))


(defn transform [record]
  (sc.api/spy)
  {:decision-made "OK"})

(defn topology-builder
  [topic-metadata]
  (let [builder (j/streams-builder)]
    (-> (j/kstream builder (:data-validated topic-metadata))
      (j/map-values transform)
      (j/to (:decision-made topic-metadata)))
    builder))
