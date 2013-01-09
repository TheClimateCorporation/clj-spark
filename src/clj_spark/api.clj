(ns clj-spark.api
  (:use clj-spark.spark.functions)
  (:refer-clojure :exclude [map reduce count filter first take distinct])
  (:require
    [serializable.fn :as sfn]
    [clojure.string :as s]
    [clj-spark.util :as util])
  (:import
    java.util.Comparator
    spark.api.java.JavaSparkContext))

; Helpers

(defn spark-context
  [& {:keys [master job-name]}]
  ;JavaSparkContext(master: String, jobName: String, sparkHome: String, jars: Array[String], environment: Map[String, String])
  (JavaSparkContext. master job-name))

(defn- untuple
  [t]
  [(._1 t) (._2 t)])

(defn- double-untuple
  "Convert (k, (v, w)) to [k [v w]]."
  [t]
  (let [[x t2] (untuple t)]
    (vector x (untuple t2))))

(def csv-split util/csv-split)

(defn ftopn
  "Return a fn that takes (key, values), sorts the values in DESC order,
  and takes the top N values.  Returns (k, top-values)."
  [n]
  (fn [[k values]]
    (vector k (->> values (sort util/rcompare) (clojure.core/take n)))))

(defn fchoose
  [& indices]
  (fn [coll]
    (util/choose coll indices)))

(defn ftruthy?
  [f]
  (sfn/fn [x] (util/truthy? (f x))))

(defn feach
  "Mostly useful for parsing a seq of Strings to their respective types.  Example
  (k/map (k/feach as-integer as-long identity identity as-integer as-double))
  Implies that each entry in the RDD is a sequence of 6 things.  The first element should be
  parsed as an Integer, the second as a Long, etc.  The actual functions supplied here can be
  any arbitray transformation (e.g. identity)."
  [& fs]
  (fn [coll]
    (clojure.core/map (fn [f x] (f x)) fs coll)))

; RDD construction

(defn text-file
  [spark-context filename]
  (.textFile spark-context filename))

(defn parallelize
  [spark-context lst]
  (.parallelize spark-context lst))

; Transformations

(defn echo-types
  ; TODO make this recursive
  [c]
  (if (coll? c)
    (println "TYPES" (clojure.core/map type c))
    (println "TYPES" (type c)))
  c)

(defn trace
  [msg]
  (fn [x]
    (prn "TRACE" msg x)
    x))

(defn map
  [rdd f]
  (.map rdd (function f)))

(defn reduce
  [rdd f]
  (.reduce rdd (function2 f)))

(defn flat-map
  [rdd f]
  (.map rdd (flat-map-function f)))

(defn filter
  [rdd f]
  (.filter rdd (function (ftruthy? f))))

(defn foreach
  [rdd f]
  (.foreach rdd (void-function f)))

(defn aggregate
  [rdd zero-value seq-op comb-op]
  (.aggregate rdd zero-value (function2 seq-op) (function2 comb-op)))

(defn fold
  [rdd zero-value f]
  (.fold rdd zero-value (function2 f)))

(defn reduce-by-key
  [rdd f]
  (-> rdd
      (.map (pair-function identity))
      (.reduceByKey (function2 f))
      (.map (function untuple))))

(defn group-by-key
  [rdd]
  (-> rdd
      (.map (pair-function identity))
      .groupByKey
      (.map (function untuple))))

(defn sort-by-key
  ([rdd]
   (sort-by-key rdd compare true))
  ([rdd x]
    ; Note: RDD has a .sortByKey signature with just a Boolean arg, but it doesn't
    ; seem to work when I try it, bool is ignored.
    (if (instance? Boolean x)
      (sort-by-key rdd compare x)
      (sort-by-key rdd x true)))
  ([rdd compare-fn asc?]
   (-> rdd
       (.map (pair-function identity))
       (.sortByKey
         (if (instance? Comparator compare-fn)
           compare-fn
           (comparator compare-fn))
         (util/truthy? asc?))
       (.map (function untuple)))))

(defn join
  [rdd other]
  (-> rdd
      (.map (pair-function identity))
      (.join (.map other (pair-function identity)))
      (.map (function double-untuple))))

; Actions

(def first (memfn first))

(def count (memfn count))

(def glom (memfn glom))

(def cache (memfn cache))

(def collect (memfn collect))

; take defined with memfn fails with an ArityException, so doing this instead:
(defn take
  [rdd cnt]
  (.take rdd cnt))

(def distinct (memfn distinct))
