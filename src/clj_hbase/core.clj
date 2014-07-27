(ns clj-hbase.core
  (:refer-clojure :rename {get map-get})
  (:require [clj-hbase.interop :as interop]
            [clj-hbase.util :as util]))

(defprotocol HBaseProto
  (create-table [this table-name family-names])
  (delete-table [this table-name])
  (disable-table [this table-name])
  (enable-table [this table-name])
  (table-exists? [this table-name])
  (delete [this table-name row spec])
  (get [this table-name row spec result-mapping])
  (put [this table-name row values]))

(deftype HBase [connection]
  HBaseProto
  (create-table [this table-name family-names]
                (let [table-desc (interop/create-table-descriptor-obj table-name)]
                  (doall
                   (for [family-name family-names]
                     (.addFamily
                      table-desc
                      (interop/create-column-descriprot-obj family-name))))
                  (.createTable
                   (interop/create-admin-obj connection)
                   table-desc)))
  (delete-table [this table-name]
                (let [admin (interop/create-admin-obj connection)
                      table (util/->bytes table-name)]
                  (.disableTable admin table)
                  (.deleteTable admin table)))
  (disable-table [this table-name] (.disableTable (interop/create-admin-obj connection)))
  (enable-table [this table-name] (.enableTable (interop/create-admin-obj connection)))
  (table-exists? [this table-name]
                 (interop/table-exists?
                  (interop/create-admin-obj connection)
                  table-name))
  (delete [this table-name row spec]
       (.delete
         (interop/create-table-obj connection (util/->string table-name))
         (interop/create-delete-obj row spec)))
  (get [this table-name row spec result-mapping]
       (interop/unwrap-result
        (.get
         (interop/create-table-obj connection (util/->string table-name))
         (interop/create-get-obj row spec))
        result-mapping))
  (put [this table-name row values]
       (.put
         (interop/create-table-obj connection (util/->string table-name))
         (interop/create-put-obj row values)))
  )

(defn create-database
  ([] (create-database {}))
  ([config-map] (HBase.
                 (interop/create-connection-obj
                  (interop/create-config-obj config-map)))))
