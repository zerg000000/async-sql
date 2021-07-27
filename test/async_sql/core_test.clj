(ns async-sql.core-test
  (:require
    [async-sql.core :as sql]
    [async-sql.pool.pg :as pg]
    [clj-test-containers.core :as tc]
    [clojure.test :refer :all])
  (:import
    (io.vertx.core
      Vertx)))


(deftest db-integration-test
  (testing "A simple PostgreSQL integration test"
    (let [pw "db-pass"
          postgres (-> (tc/create {:image-name "postgres:12.1"
                                   :exposed-ports [5432]
                                   :env-vars {"POSTGRES_PASSWORD" pw}}))
          {:keys [mapped-ports]} (tc/start! postgres)]
      (with-open [vertx (Vertx/vertx)
                  pg (pg/create vertx {:jdbc-url (str "postgresql://postgres:db-pass@localhost:" (get mapped-ports 5432)  "/postgres")})]
        (is (= [{:one 1 :two 2}] @(sql/execute! pg
                                                ["SELECT 1 ONE, 2 TWO"]
                                                {:mapper    sql/as-unqualifed-maps}))))
      (with-open [vertx (Vertx/vertx)
                  pg (pg/create vertx {:jdbc-url (str "postgresql://postgres:db-pass@localhost:" (get mapped-ports 5432)  "/postgres")})]
        (is (= [{:one 1 :two 2}] @(sql/with-transaction [sql pg]
                                                        (sql/execute!* sql
                                                                       ["SELECT 1 ONE, 2 TWO"]
                                                                       {:mapper    sql/as-unqualifed-maps})))))
      (tc/stop! postgres))))
