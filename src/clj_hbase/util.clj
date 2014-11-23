(ns clj-hbase.util)

(defn ->bytes [item] (cond (string? item) (.getBytes item)
                           (instance? (type (byte-array 0)) item) item
                           (keyword? item) (.getBytes (name item))
                           :else (throw (Exception. (str "Unable to convert type " (class item) " to bytes")))))

(defn ->string [item] (cond (keyword? item) (name item)
                            (string? item) item
                            :else (throw (Exception. (str "Unable to convert type " (class item) " to string")))))
