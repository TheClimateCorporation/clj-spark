(ns clj-spark.examples.query
  (:refer-clojure :exclude [fn])
  (:use
    [clojure.tools.cli :only [cli]]
    serializable.fn
    clj-spark.util)
  (:require
    [clojure.string :as s]
    [clj-spark.api :as k]
    [clj-spark.examples.extra :as extra]))

(defn -main
  [& args]
  (let [[{:keys [input master spark-home jars env]} remaining banner]
          (cli args
            ["--input" "the input file, a path in local mode, or full hdfs: url for a cluster"
                       :default "test/resources/input.csv"]
            ["--master" "The master url (local, local[N], spark://..., mesos://..." :default "local"]
            ["--spark-home" "Path to the Spark home directory."
                            :default (or (System/getenv "SPARK_HOME")
                                         (throw (RuntimeException. "SPARK_HOME or --spark-home must be set")))]
            ["--jars" (str "A comma separated list of JARs to send to the cluster. These can be paths "
                            "on the local file system or HDFS, HTTP, HTTPS, or FTP URLs.")
                       :parse-fn #(s/split % #",") :default []]
            ["--env" "Environment variables to set on worker nodes (e.g. \"k=v,k=v\")"
                     :parse-fn #(->> (s/split % #",") (map (fn [s] (s/split s #"="))) (into {}))
                     :default {}])

        sc
          (k/spark-context :master master :job-name "Simple Job" :jars jars
                           :environment env :spark-home spark-home)

        ; Warning: Make sure you use fn and not # for anonymous functions, so that the function
        ; will be cerated with serializable.fn

        extra-rdd
          (->> (extra/get-data)
               list*
               (k/parallelize sc)
               k/cache)

        input-rdd
          (-> (.textFile sc input)
              (k/map k/csv-split)
              ; _,policy-id,field-id,_,_,element-id,element-value
              (k/map (k/feach identity as-integer as-long identity identity as-integer as-double))
              (k/map (juxt (k/fchoose 1 2) (k/fchoose 5 6)))  ; [ [policy-id field-id] [element-id element-value] ]
              k/cache)

        premium-per-state
          (-> input-rdd
              (k/map first)       ; [policy-id field-id]
              k/distinct

              ; Need the constant 1, b/c there must be some data values to join
              (k/map (fn [pf] (vector pf 1)))  ; [[policy-id field-id] 1]

              (k/join (-> extra-rdd
                          (k/map (fn [{:keys [policy_id field_id state policy_premium acres]}]
                                   [[policy_id field_id] [state (* policy_premium acres)]]))))
                                            ; [[policy-id field-id] [1 [state field-premium]]]

              (k/map (comp second second))  ; [state field-premium]
              (k/reduce-by-key +))

        top100-element-ids-overall
          (-> input-rdd
              (k/map second)  ; [element-id element-value]
              (k/reduce-by-key +)      ; [element-id total]
              (k/map (k/fchoose 1 0))  ; [total element-id]
              (k/sort-by-key false)    ; desc
              (k/map second)           ; element-id
              (k/take 2)               ; TODO n=100
              set)

        element-id-state-value-rdd
          (-> input-rdd
              (k/join
                (-> extra-rdd
                    (k/map (fn [{:keys [policy_id field_id state]}] [[policy_id field_id] state]))))
                                        ; [ [policy-id field-id] [ [element-id element-value] state ]]
              (k/map
                (fn [[_ [[element-id element-value] state]]] [element-id [state element-value]]))
                                        ; [element-id [state element-value]]*
              k/cache)

        ;CTE per state (based on common Top 100)
        cte-per-state
          (-> element-id-state-value-rdd
              (k/filter (fn [[element-id _]] (top100-element-ids-overall element-id)))
              (k/map second)            ; [state element-value]
              (k/group-by-key)          ; [state element-values]
              (k/map (fn [[state element-values]] [state (avg element-values)])))   ; [state cte99]

        ; Standalone CTE per state (based on each state's Top 100)
        top100-element-ids-per-state
          (-> element-id-state-value-rdd
              (k/map (fn [[element-id [state element-value]]]
                       [[state element-id] element-value]))    ; [[state element-id] element-value]
              (k/reduce-by-key +)
              (k/map (fn [[[state element-id] element-value]]
                       [state [element-value element-id]]))    ; [ state [element-value element-id] ]
              k/group-by-key                                   ; [ state [element-value element-id]* ]
              ; TODO n=100
              (k/map (k/ftopn 2))                              ; [ state [element-value element-id]* ] for the TOP values
              (k/map (fn [[state elements]]
                       (vector state (set (map second elements)))))   ; [ state top-element-ids ]
              k/collect
              (->> (into {})))

        standalone-cte-per-state
          (-> element-id-state-value-rdd                ; [element-id [state element-value]]*
              (k/map (fn [[element-id [state element-value]]]
                       [[state element-id] element-value]))
              k/group-by-key                            ; [ [state element-id] element-values ]
              (k/filter (fn [[[state element-id] element-values]]
                          ((get top100-element-ids-per-state state) element-id)))
              (k/map (fn [[[state _] element-values]]
                       [state element-values]))         ; [state element-values]*
              k/group-by-key                            ; [state element-values-seq-of-seqs]

              (k/map (fn [[state element-values-seq-of-seqs]]
                       ; NOTE clojure.core/flatten does not work on scala SeqWrapper
                       [state (avg (reduce concat element-values-seq-of-seqs))])))  ; [state cte99]
          ]

    (println "==============")
    (println "Premium Per State")
    (doseq [[state premium] (k/collect premium-per-state)]
      (println state premium))

    (println)
    (println "==============")
    (println "TOP100")
    (println top100-element-ids-overall)

    (println)
    (println "==============")
    (println "CTE Per State")
    (doseq [[state cte] (k/collect cte-per-state)]
      (println state cte))

    (println)
    (println "==============")
    (println "TOP100PERSTATE")
    (println top100-element-ids-per-state)

    (println)
    (println "==============")
    (println "Standalone CTE Per State")
    (doseq [[state cte] (k/collect standalone-cte-per-state)]
      (println state cte))

    (println "==============")))

(defn command-line? [] (.isAbsolute (java.io.File. *file*)))

(if (command-line?) (apply -main *command-line-args*))
