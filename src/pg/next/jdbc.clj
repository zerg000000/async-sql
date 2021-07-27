(ns pg.next.jdbc
  (:require
    [next.jdbc.protocols :as p])
  (:import
    (io.vertx.sqlclient
      SqlClient)))


(extend-type SqlClient
  p/Executeable
  (-execute ^clojure.lang.IReduceInit [this sql-params opts])
  (-execute-one [this sql-params opts])
  (-execute-all [this sql-params opts]))
