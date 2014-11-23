(ns clj-hbase.interop
  (:refer-clojure :rename {get map-get})
  (:require [clj-hbase.util :as util])
  (:import [org.apache.hadoop.hbase.client
            HConnection HConnectionManager
            Delete Get Put Result
            HBaseAdmin]
           [org.apache.hadoop.hbase
            HBaseConfiguration
            TableName
            HColumnDescriptor
            HTableDescriptor
            TableNotFoundException]
           [org.apache.hadoop.fs Path]))

(defn unwrap-result [^Result result result-mapping]
  (let [family-mapper (map-get result-mapping :family #(String. %))
        timestamp-mapper (map-get result-mapping :timestamp #(java.util.Date. %))
        column-mapper (map-get result-mapping :column {})
        default-column-mapper identity
        value-mapper (map-get result-mapping :value {})
        default-value-mapper identity
        result-map (.getMap result)
        value-reducer (fn [family column acc [timestamp value]]
                        (let [mapper (get-in value-mapper [family column]
                                             (get-in value-mapper [family :*]
                                                     (get-in value-mapper [:* :*] default-value-mapper)))]
                          (assoc acc (timestamp-mapper timestamp) (mapper value))))
        column-reducer (fn [family acc [column values]]
                         (let [mapper (map-get column-mapper family
                                               (get-in column-mapper [:*] default-column-mapper))
                               mapped-column (mapper column)]
                           (assoc acc mapped-column
                             (reduce (partial value-reducer family mapped-column) {} values))))
        family-reducer (fn [acc [family columns]]
                         (let [mapped-family (family-mapper family)]
                           (assoc
                             acc
                             mapped-family
                             (reduce (partial column-reducer mapped-family) {} columns))))]
    (reduce family-reducer {} result-map)))

(defn create-config-obj [config-map]
  (let [config (HBaseConfiguration/create)]
    (doall (for [[k v] config-map]
             (cond
              (= k :resources) (doall (map #(.addResource config (Path. %)) v))
              (string? v) (.set config (name k) v)
              (instance? Boolean v) (.setBoolean config (name k) v)
              (instance? Double v) (.setDouble config (name k) v)
              (instance? Long v) (.setLong config (name k) v)
              (instance? Float v) (.setFloat config (name k) v)
              (instance? Integer v) (.setInteger config (name k) v)
              (seq? v) (.setStrings config (name k) v))))
    config))

(defn create-connection-obj [^HBaseConfiguration config]
  (HConnectionManager/createConnection config))

(defn create-table-obj [^HConnection connection table-name]
  (.getTable connection table-name))

(defn create-admin-obj [^HConnection connection]
  (HBaseAdmin. connection))

(defn create-delete-obj [row spec]
  (let [d (Delete. (util/->bytes row))]
    (when-let [family (:family spec)]
      (if-let [column (:column spec)]
        (let [cols (if (vector? column) column [column])]
          (doall (for [col cols]
                   (.deleteColumns d (util/->bytes family) (util/->bytes col)))))
        (.deleteFamily d (util/->bytes family))))
    d))

(defn create-get-obj [row spec]
  (let [g (Get. (util/->bytes row))]
    (when-let [family (:family spec)]
      (if-let [column (:column spec)]
        (let [cols (if (vector? column) column [column])]
          (doall (for [col cols]
                   (.addColumn g (util/->bytes family) (util/->bytes col)))))
        (.addFamily g (util/->bytes family))))
    g))

(defn create-put-obj [row values]
  (let [p (Put. (util/->bytes row))]
    (doall
     (for [family-key (keys values)]
       (let [family (map-get values family-key)]
         (if (not (map? family))
           (.add
            p
            (util/->bytes family-key)
            (util/->bytes "")
            (util/->bytes family))
           (doall
            (for [column-key (keys family)]
              (.add
               p
               (util/->bytes family-key)
               (util/->bytes column-key)
               (util/->bytes (map-get family column-key)))))))))
    p))

(defn create-table-name-obj [table-name]
  (TableName/valueOf (util/->bytes table-name)))

(defn create-table-descriptor-obj [table-name]
  (HTableDescriptor. (create-table-name-obj table-name)))

(defn create-column-descriprot-obj [family-name]
  (HColumnDescriptor. (util/->bytes family-name)))

(defn table-exists? [admin table-name]
  (try
    (when (nil? table-name)
      (throw (Exception. "table-name is nil")))
    (.getTableDescriptor admin (util/->bytes table-name))
    true
    (catch TableNotFoundException e false)))



