(ns pg.core-test
  (:require
    [clj-test-containers.core :as tc]
    [clojure.test :refer :all]
    [pg.core :as pg])
  (:import
    (io.vertx.core
      Vertx)
    (io.vertx.sqlclient
      Row)
    (java.util.stream
      Collectors)))


(deftest db-integration-test
  (testing "A simple PostgreSQL integration test"
    (let [pw "db-pass"
          postgres (-> (tc/create {:image-name "postgres:12.1"
                                   :exposed-ports [5432]
                                   :env-vars {"POSTGRES_PASSWORD" pw}}))
          {:keys [mapped-ports]} (tc/start! postgres)]
      (with-open [vertx (Vertx/vertx)
                  pg (pg/create vertx {:jdbc-url (str "postgresql://postgres:db-pass@localhost:" (get mapped-ports 5432)  "/postgres")})]
        (is (= [{:one 1 :two 2}] @(pg/execute! pg
                                               ["SELECT 1 ONE, 2 TWO"]
                                               {:mapper    pg/as-unqualifed-maps
                                                :collector (Collectors/toList)}))))
      (with-open [vertx (Vertx/vertx)
                  pg (pg/create vertx {:jdbc-url (str "postgresql://postgres:db-pass@localhost:" (get mapped-ports 5432)  "/postgres")})]
        (is (= [{:one 1 :two 2}] @(pg/with-transaction [sql pg]
                                                       (pg/execute!* sql
                                                                     ["SELECT 1 ONE, 2 TWO"]
                                                                     {:mapper    pg/as-unqualifed-maps
                                                                      :collector (Collectors/toList)})))))
      (tc/stop! postgres))))
