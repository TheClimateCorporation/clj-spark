clj-spark
=========

A Clojure api for the Spark Project.  It is useable, but not complete.  I
provide it as a starting point.  It should be simple enough to add the
additional wrappers as you need them.  This is the result of about two weeks of
work.

It handles many of the initial problems like serializing anonymous functions,
converting back and forth between Scala Tuples and Clojure seqs, and
converting RDDs to PairRDDs.

# What is Spark

From http://spark-project.org/docs/latest/index.html

Spark is a MapReduce-like cluster computing framework designed for low-latency iterative 
jobs and interactive use from an interpreter. It provides clean, language-integrated APIs
in Scala and Java, with a rich array of parallel operators. Spark can run on top of the
Apache Mesos cluster manager, Hadoop YARN, Amazon EC2, or without an independent resource
manager (“standalone mode”).

# Example usage

There is a complete sample program in src/clj_spark/examples/query.clj.  To run
it, clone this repo and cd into it.  You will need Leiningen 2 installed
(assuming this is available on your PATH as lein2):

```bash
$ git clone https://github.com/TheClimateCorporation/clj-spark.git
$ cd clj-spark
$ lein2 deps
$ lein2 compile
$ lein2 run
```
```
Compiling clj-spark.api
2013-01-02 13:18:41.477 java[65466:1903] Unable to load realm mapping info from
SCDynamicStore
==============
Premium Per State
NY 600.0

==============
TOP100
#{1 2}

==============
CTE Per State
NY 70.0

==============
TOP100PERSTATE
{NY #{1 2}}

==============
Standalone CTE Per State
NY 70.0
==============
```

The following are subsections copied from query.clj:

Here is a sample of creating an RDD:
```clojure
          (-> (.textFile sc testfile)
              (k/map k/csv-split)
              ; _,policy-id,field-id,_,_,element-id,element-value
              (k/map (k/feach identity as-integer as-long identity identity as-integer as-double))
              (k/map (juxt (k/fchoose 1 2) (k/fchoose 5 6)))  ; [ [policy-id field-id] [element-id element-value] ]
              k/cache)
```
And a sample query on that data:
```clojure
          (-> input-rdd
              (k/map second)  ; [element-id element-value]
              (k/reduce-by-key +)      ; [element-id total]
              (k/map (k/fchoose 1 0))  ; [total element-id]
              (k/sort-by-key false)    ; desc
              (k/map second)           ; element-id
              (k/take 2)               ; TODO n=100
              set)
```

# Running queries from the REPL

You can also start a repl and play around:

```bash
# assuming you already did deps and compile above...
$ lein2 repl
```

```clojure
; deleted results to be more concise
user=> (use 'serializable.fn 'clj-spark.util)
user=> (require '[clj-spark.api :as k])
user=> (def sc (k/spark-context :master "local" :job-name "Simple Job"))
user=> (def r1 (k/parallelize sc [10 20 25 30 35]))
user=> (def r2 (k/text-file sc "test/resources/input.csv"))

user=> (k/count r2)
5
user=> (def result (-> r1 (k/map inc) (k/map (fn [t] [(even? t) t])) (k/reduce-by-key +)))
#'clj-spark.examples.query/result
user=> (k/collect result)
#<ArrayList [[false 63], [true 62]]>
; or, all in one step:
user=> (-> r1 (k/map inc) (k/map (fn [t] [(even? t) t])) (k/reduce-by-key +) k/collect)
#<ArrayList [[false 63], [true 62]]>
```

# Other clojure apis

After working on this, I found another Clojure API for Spark project: 
https://github.com/markhamstra/spark/tree/master/cljspark

It's a bit more complete, but no examples.  You might find good ideas in both projects.

# Known Issues

## Function serialization

You must create your anonymous functions using serializable.fn/fn, as in:
```clojure
(ns ...
  (:require \[serializable.fn :as sfn\]))

(sfn/fn my-inc \[x\] (+ x 1))
```
Do not use clojure.core/fn or #().  This is necessary because the anonymous function 
must be serialized so it can be passed around to distributed tasks.

## AOT compilation

Generally speaking, any functions that are used in the Spark calls will need to be part
of AOT compiled namespaces.  I.e. they need to be compiled or the distributed Spark
tasks will not be able to find them.  In some cases, compiling on the fly might work
also:

```clojure
  (compile 'your-namespace)
```

But you need to do this somewhere where it will be executed for each task.

NOTE: This should be avoidable using the serializable.fn as above, but I did not get
that to work in my initial attempts.

## None of the Double* method are implemented

The Spark Java API provides versions of some methods that accept or return Doubles.  
E.g. (copied from Spark docs, using Scala syntax):
```scala
def map[R](f: DoubleFunction[T]): JavaDoubleRDD
```
So class DoubleFunction<T> has a function type of T => Double

Compare this to the standard:
```scala
def map[R](f: Function[T, R]): JavaRDD[R]
```
Where Function<T, R> has type T => R

I didn't wrap any of these.  To be honest, I don't see why they are needed.  Maybe
I'm missing something or maybe it just doesn't matter when called from a dynamically
typed language like Clojure.  Instead of DoubleFunction[T], just use Function<T, Double>
which has type T => Double.  I don't see why this wouldn't work, but interested to know 
if there is a case where this fails or is sub-optimal.
