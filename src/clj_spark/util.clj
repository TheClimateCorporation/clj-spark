(ns clj-spark.util
  (:require
    [clojure.string :as s]))

(defn truthy?
  [x]
  (if x (Boolean. true) (Boolean. false)))

(defn as-integer
  [s]
  (Integer. s))

(defn as-long
  [s]
  (Long. s))

(defn as-double
  [s]
  (Double. s))

(defn csv-split
  [s]
  (s/split s #","))

(defn rcompare
  "Comparator. The reverse of clojure.core/compare, i.e.
  compare in DESCending order."
  [x y] (- (clojure.lang.Util/compare x y)))

(defn choose
  [coll indices]
  (reduce #(conj %1 (nth coll %2)) [] indices))

(defn avg
  [values]
  (if (empty? values)
    0.0
    (/ (reduce + values) (count values))))
