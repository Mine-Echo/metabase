(ns metabase.driver.dolphindb
  "Driver for DolphinDB. "
  (:require
  ;;  [clojure.string :as str]
  ;;  [clojure.java.jdbc :as jdbc]
   [metabase.driver :as driver] 
  ;;  [metabase.driver.sql :as driver.sql]
  ;;  [metabase.driver.sql-jdbc.common :as sql-jdbc.common] 
   [metabase.driver.sql-jdbc.connection :as sql-jdbc.conn]
   [metabase.driver.sql-jdbc.execute :as sql-jdbc.execute] 
   [metabase.driver.sql-jdbc.sync :as sql-jdbc.sync]
   [metabase.driver.sql-jdbc.sync.common :as sql-jdbc.sync.common]
   [metabase.driver.sql-jdbc.sync.describe-database :as sql-jdbc.describe-database]
   [metabase.driver.sql.query-processor :as sql.qp]
   [metabase.driver.sync :as driver.s]
   [metabase.driver.util :as driver.u] 
  ;;  [metabase.driver.sql-jdbc.sync.interface :as sql-jdbc.sync.interface]
  ;;  [metabase.lib.schema.common :as lib.schema.common]
   [metabase.util.log :as log]
   [metabase.util.malli :as mu])
  (:import 
   (java.sql Connection DatabaseMetaData ResultSet)
   (java.sql Connection DatabaseMetaData ResultSet))
)

(set! *warn-on-reflection* true)

(driver/register! :dolphindb, :parent :sql-jdbc)


;; 以下在plugin manifest中指定
;; driver/display-name
;; driver/connection-properties
;; driver/initialize!


(doseq [[feature supported?] {:metadata/key-constraints false
                              :nested-fields false
                              :nested-field-columns false
                              :set-timezone false
                              :basic-aggregations true
                              :standard-deviation-aggregations true
                              :expressions true
                              :native-parameters true 
                              :expression-aggregations true 
                              :nested-queries true
                              :native-parameter-card-reference false
                              :persist-models false 
                              :persist-models-enabled false 
                              :binning false
                              :case-sensitivity-string-filter-options true
                              :left-join true
                              :right-join true
                              :inner-join true
                              :full-join true
                              :regex true
                              :advanced-math-expressions true
                              :percentile-aggregations true
                              :temporal-extract true
                              :date-arithmetics true
                              :now true
                              :convert-timezone false
                              :datetime-diff true 
                              :actions false
                              :table-privileges false  ;;表级权限
                              :uploads false
                              :schemas true
                              :actions/custom false
                              :test/jvm-timezone-setting false 
                              :connection-impersonation false 
                              :connection-impersonation-requires-role false 
                              :native-requires-specified-collection false 
                              :index-info false
                              :describe-fks false
                              :describe-fields false 
                              :upload-with-auto-pk false
                              :fingerprint true 
                              :connection/multiple-databases true
                              :identifiers-with-spaces true 
                              :uuid-type true
                              :temporal/requires-default-unit false
                              :window-functions/cumulative true
                              :window-functions/offset true
                              :parameterized-sql true
                              :test/dynamic-dataset-loading false 
                              }]
  (defmethod driver/database-supports? [:dolphindb feature] [_driver _feature _db] supported?))


(defmethod sql-jdbc.conn/connection-details->spec :dolphindb
  [_ {:keys [user password dbname host port]
    ;;   :or   {user "dbuser", password "dbpassword", dbname "", host "localhost", port 8848}
      :as   _details}]
  {
   :classname   "com.dolphindb.jdbc.Driver"
   :subprotocol "dolphindb"
   :subname     (str "//" host ":" port)
   :user user
   :password password
   :host host
   :port port
   :dbname dbname
  })


(defmethod sql-jdbc.sync/database-type->base-type :dolphindb
  [_ column-type]
  ({:DT_BOOL           :type/Boolean
    :DT_CHAR           :type/Byte
    :DT_DATE           :type/Date
    :DT_DATEHOUR       :type/DateTime
    :DT_DATETIME       :type/DateTime
    :DT_MINUTE         :type/DateTime
    :DT_TIME           :type/Time
    :DT_TIMESTAMP      :type/DateTime ;
    :DT_NANOTIME       :type/DateTime
    :DT_NANOTIMESTAMP  :type/DateTime
    :DT_DECIMAL32      :type/Decimal
    :DT_DECIMAL64      :type/Decimal
    :DT_DECIMAL128     :type/Decimal
    :DT_DOUBLE         :type/Float ;
    :DT_DURATION       :type/* ;java中无对应数据类型
    :DT_FLOAT          :type/Float
    :DT_INT            :type/Integer ;
    :DT_INT128         :type/Integer
    :DT_IPADDR         :type/* ;java无
    :DT_LONG           :type/Integer ; 
    :DT_MONTH          :type/*
    :DT_SECOND         :type/Time
    :DT_POINT          :type/* ;java无
    :DT_SHORT          :type/Integer
    :DT_STRING         :type/Text
    :DT_BLOB           :type/* ;java无
    :DT_UUID           :type/UUID

    :DT_SYMBOL         :type/Text ;
    } column-type))


;; driver/describe-table
(defmethod sql-jdbc.sync/active-tables :dolphindb
  [driver connection schema-inclusion-filters schema-exclusion-filters]
  (log/info "----------active-tables:dolphindb----------")
  (sql-jdbc.sync/fast-active-tables driver connection (.getCatalog connection) schema-inclusion-filters schema-exclusion-filters))

(mu/defn describe-database*
  "Implementation of [[metabase.driver/describe-database]] for DolphinDB drivers. Uses JDBC DatabaseMetaData."
  [driver           :- :keyword
   db-or-id-or-spec :- [:or :int :map]]
  {:tables
        (sql-jdbc.execute/do-with-connection-with-options
         driver
         db-or-id-or-spec
         nil
         (fn [^Connection conn]
           (let [schema-filter-prop   (driver.u/find-schema-filters-prop driver)
                 database             (sql-jdbc.describe-database/db-or-id-or-spec->database db-or-id-or-spec)
                 [inclusion-patterns
                  exclusion-patterns] (when (some? schema-filter-prop)
                                        (driver.s/db-details->schema-filter-patterns (:name schema-filter-prop) database))]
             (log/info "---------DDB:describe-database:schema-filter-prop--------" schema-filter-prop)
             (log/info "---------DDB:describe-database:database--------" database)
             (log/info "---------DDB:describe-database:inclusion-patterns--------" inclusion-patterns)
             (log/info "---------DDB:describe-database:exclusion-patterns--------" exclusion-patterns)
             (.setCatalog  conn (:dbname (:details database)))
             (into #{} (sql-jdbc.sync/active-tables driver conn inclusion-patterns exclusion-patterns)))))})

(defmethod driver/describe-database :dolphindb
  [driver database]
  (describe-database* driver database))

;; driver/describe-table
(defmethod driver/describe-table :dolphindb
  [driver database table]
  (sql-jdbc.execute/do-with-connection-with-options
     driver
     database
     nil
     (fn [^Connection conn]
       (log/info "----------describe-table:dolphindb-----------" (.getCatalog conn))
       (->> (assoc (select-keys table [:name :schema])
                     :fields (sql-jdbc.sync/describe-table-fields driver conn table (.getCatalog conn)))))))

;; sql-jdbc.sync/filtered-syncable-schemas
(defn- catalog-schemas
  "Get a *reducible* sequence of all string schema names for the current database from its JDBC database metadata."
  [^DatabaseMetaData metadata catalog]
  (sql-jdbc.sync.common/reducible-results
   #(.getSchemas metadata catalog "%")
   (fn [^ResultSet rs]
     #(.getString rs "TABLE_SCHEM"))))

(defmethod sql-jdbc.sync/filtered-syncable-schemas :dolphindb
  [driver conn metadata schema-inclusion-patterns schema-exclusion-patterns]
  (eduction (remove (set (sql-jdbc.sync/excluded-schemas driver)))
            (filter (partial driver.s/include-schema? schema-inclusion-patterns schema-exclusion-patterns))
            (catalog-schemas metadata (.getCatalog conn))))

(defmethod sql.qp/->honeysql [:dolphindb Boolean]
  [_ bool]
  (if bool 1 0))

(defmethod sql.qp/quote-style :dolphindb [_driver] :dolphindb2)






;; substring method
(defmethod sql.qp/->honeysql [:dolphindb :substring]
  [driver [_ arg start length]]
  (if length
    [:substr (sql.qp/->honeysql driver arg) (sql.qp/->honeysql driver (- start 1)) (sql.qp/->honeysql driver length)]
    [:substr (sql.qp/->honeysql driver arg) (sql.qp/->honeysql driver (- start 1))]))

;; remark style
(defmethod sql-jdbc.execute/inject-remark :dolphindb
  [_ sql remark]
  (str "// " remark "\n" sql))


(defmethod sql.qp/date [:dolphindb :minute]
  [_driver _unit expr]
  [:minute expr])


