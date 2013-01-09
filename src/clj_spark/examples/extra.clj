(ns clj-spark.examples.extra
  "This namespace simulates getting a dataset from a distinct source.  This could be
  a SQL query, for example.")

(defn get-data
  "Returns a result set as a seq of Maps.  Similar to a result set acquired by clojure.data.jdbc."
  []
  (map (partial zipmap [:policy_id :field_id :state :policy_premium :acres])
       [[(int 1) 10 "NY" 100.0 2]
        [(int 1) 20 "NY" 200.0 2]
        [(int 2) 10 "CT" 300.0 2]
        [(int 2) 11 "CT" 400.0 2]]))
