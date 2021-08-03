(ns async-sql.core
  (:import
    (io.vertx.core
      Future)
    (io.vertx.sqlclient
      Pool
      PoolOptions
      Row
      RowSet
      SqlClient
      Tuple)
    (java.util.concurrent
      CompletionStage)
    (java.util.function
      Function)))

(defprotocol ReadableColumn
  (read-column-by-label [val label]
    "Function for transforming values after reading them via a column label.")
  (read-column-by-index [val rsmeta idx]
    "Function for transforming values after reading them via a column index."))

(extend-protocol ReadableColumn
  Object
  (read-column-by-label [x _] x)
  (read-column-by-index [x _2 _3] x)

  Boolean
  (read-column-by-label [x _] (if (= true x) true false))
  (read-column-by-index [x _2 _3] (if (= true x) true false))

  nil
  (read-column-by-label [_1 _2] nil)
  (read-column-by-index [_1 _2 _3] nil))


(defn ^PoolOptions map->pool-options
  [{:keys [max-size max-wait-queue-size
           idle-timeout-unit idle-timeout
           pool-cleaner-period
           connection-timeout-unit connection-timeout]}]

  (cond-> (PoolOptions.)
    max-size (.setMaxSize max-size)
    max-wait-queue-size (.setMaxWaitQueueSize max-wait-queue-size)
    idle-timeout-unit (.setConnectionTimeoutUnit idle-timeout-unit)
    idle-timeout (.setIdleTimeout idle-timeout)
    pool-cleaner-period (.setPoolCleanerPeriod pool-cleaner-period)
    connection-timeout-unit (.setConnectionTimeoutUnit connection-timeout-unit)
    connection-timeout (.setConnectionTimeout connection-timeout)))


(defn close
  [^SqlClient client]
  (.close client))


(defn jfn
  [f]
  (reify Function
    (apply [this args] (f args))))


(defn rowset->seq
  [^RowSet rowset]
  (when rowset
    (iterator-seq (.iterator rowset))))


(defn as-unqualifed-maps
  [^Row row]
  (loop [i (.size row)
         m (transient {})]
    (if (nat-int? i)
      (recur
       (dec i)
       (assoc! m
               (keyword (.getColumnName row i))
               (read-column-by-index (.getValue row i) row i)))
      (-> (dissoc! m nil)
          (persistent!)))))


(defn ^Future execute!*
  ([^SqlClient client query]
   (execute!* client query {}))
  ([^SqlClient client [query & sql-params] opts]
   (-> client
       (.preparedQuery query)
       (cond->
         (:mapper opts) (.mapping (let [f (:mapper opts)]
                                    (if (instance? Function f)
                                      f
                                      (jfn f)))))
       (.execute (Tuple/tuple (vec sql-params))))))


(defn ^CompletionStage execute!
  ([^SqlClient client query]
   (execute! client query {}))
  ([^SqlClient client query opts]
   (-> (execute!* client query opts)
       (.toCompletionStage)
       (.thenApply (jfn rowset->seq)))))


(defn ^Future transact*
  ([^Pool pool tx-fn]
   (-> pool
       (.withTransaction (jfn tx-fn)))))


(defn ^CompletionStage transact
  ([^Pool pool tx-fn]
   (-> (transact* pool tx-fn)
       (.toCompletionStage)
       (.thenApply (jfn rowset->seq)))))


(defmacro with-transaction
  [[sym pool] & body]
  `(transact ~pool (^{:once true} fn* [~sym] ~@body)))
