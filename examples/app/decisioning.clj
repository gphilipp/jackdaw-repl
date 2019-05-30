(ns app.decisioning
  (:require [jackdaw.streams :as j]))


(defn make-decision [[k v]]
  {:decision-made "OK"})


(defn topology-builder
  [{:keys [data-validated decision-made]}]
  (let [builder (j/streams-builder)]
    (-> (j/kstream builder data-validated)
        (j/map-values make-decision)
        (j/to decision-made))
    builder))
