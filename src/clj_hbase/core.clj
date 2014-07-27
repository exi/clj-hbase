(ns clj-hbase.core
  (:refer-clojure :rename {get map-get})
  (:import [org.apache.hadoop.hbase.client HConnectionManager Get Put Result]
           [org.apache.hadoop.hbase HBaseConfiguration]
           [org.apache.hadoop.fs Path]))

(defprotocol HBaseProto
  (get [this table-name row spec result-mapping])
  (put [this table-name row values]))

(defn create-config [config-map]
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

(defn create-connection [^HBaseConfiguration config] (HConnectionManager/createConnection config))

(defn create-table [connection table-name] (.getTable connection table-name))

(defn ->bytes [item] (cond (string? item) (.getBytes item)
                           (instance? (type (byte-array 0)) item) item
                           (keyword? item) (.getBytes (name item))
                           :else (throw (Exception. (str "Unable to put type " (class item))))))

(defn ->string [item] (cond (keyword? item) (name item)
                            (string? item) item
                            :else (throw (Exception. (str "Unable to put type " (class item))))))

(defn create-get [row spec]
  (let [g (Get. (->bytes row))]
    (when-let [family (:family spec)]
      (if-let [column (:column spec)]
        (let [cols (if (vector? column) column [column])]
          (doall (for [col cols]
                   (.addColumn g (->bytes family) (->bytes col)))))
        (.addFamily g (->bytes family))))
    g))

(defn create-put [row values]
  (let [p (Put. (->bytes row))]
    (doall
     (for [family-key (keys values)]
       (let [family (map-get values family-key)]
         (if (not (map? family))
           (.add
            p
            (->bytes family-key)
            (->bytes "")
            (->bytes family))
           (doall
            (for [column-key (keys family)]
              (.add
               p
               (->bytes family-key)
               (->bytes column-key)
               (->bytes (map-get family column-key)))))))))
    p))

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

(deftype HBase [connection]
  HBaseProto
  (get [this table-name row spec result-mapping]
       (unwrap-result
        (.get
         (create-table connection (->string table-name))
         (create-get row spec))
        result-mapping))
  (put [this table-name row values]
       (.put
         (create-table connection (->string table-name))
         (create-put row values))))

(defn create-database
  ([] (create-database {}))
  ([config-map] (HBase.
                 (create-connection
                  (create-config config-map)))))
