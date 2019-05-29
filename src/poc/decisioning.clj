(ns poc.decisioning
  (:require [clojure.spec.alpha :as s]
            [clojure.tools.logging :as log]
            [clojure.spec.alpha :as s]

            [jackdaw.streams :as streams]
            [jackdaw.streams :as j]
            [sc.api]
            ))


(defn transform [record]
  (sc.api/spy)
  {:decision-made "OK"})

(defn topology-builder
  [topic-metadata]
  (sc.api/spy)
  (let [builder (j/streams-builder)]
    (-> (j/kstream builder (:data-validated topic-metadata))
      (j/map-values transform)
      (j/to (:decision-made topic-metadata)))
    builder))
