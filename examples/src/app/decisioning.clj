(ns app.decisioning
  (:require [jackdaw.streams :as j]))


(defn make-decision [[k v]]
  {:decision-made "OK"})


(defn build-topology!
  [builder {:keys [topic-registry]}]
  (let [{:keys [data-validated decision-made]} topic-registry]
    (-> (j/kstream builder data-validated)
        (j/map-values make-decision)
        (j/to decision-made))
    builder))
