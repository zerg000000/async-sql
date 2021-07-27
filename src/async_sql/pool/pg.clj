(ns async-sql.pool.pg
  (:require
    [async-sql.core :as core])
  (:import
    (io.vertx.core
      Vertx)
    (io.vertx.pgclient
      PgPool)
    (io.vertx.sqlclient
      Pool)))


(defn ^Pool create
  [^Vertx vertx options]
  (PgPool/pool ^Vertx vertx
               ^String (:jdbc-url options)
               (core/map->pool-options options)))
