# clj-hbase

This is a small and still incomplete clojure wrapper for hbase 0.98.3

## Usage
Suppose we have a table "files" with column families "byte-data" and "content-type".
Our hbase configuration is placed somewhere and loaded upon database configuration.

    user=> (require '[clj-hbase.core :as hb])
    nil
    user=> (def db (hb/create-database {:resources ["/mnt/btrfs/hadoop/local/hbase-0.98.3-hadoop2/conf/hbase-site.xml"]}))
    log4j:WARN No appenders could be found for logger (org.apache.hadoop.metrics2.lib.MutableMetricsFactory).
    log4j:WARN Please initialize the log4j system properly.
    log4j:WARN See http://logging.apache.org/log4j/1.2/faq.html#noconfig for more info.
    #'user/db
    user=> (hb/put db :files "myfile.txt" {:byte-data "how neat" :content-type "text/plain"})
    nil
    user=> (hb/get db :files "myfile.txt" {} {})
    {"content-type" {#<byte[] [B@533266ff> {#inst "2014-07-27T03:12:10.639-00:00" #<byte[] [B@172c0b49>}},
     "byte-data" {#<byte[] [B@510bf556> {#inst "2014-07-27T03:12:10.639-00:00" #<byte[] [B@2f5a7d38>}}}

To be able to convert the `get` results back to usable data, it is possible to specify mappers in the last argument. The
mappers are specified in a clojure map and it follows the format:

    {:column {"myfamily" <fn to convert the column names for columnFamily "myfamily">
              :* <fn to convert the column names for ALL column families>}
     :values {"myfamily" {:* <fn that converts ALL value for all columns in "myfamily">}
              "secondfamily" {"mycolumnname" <fn that converts the values of family "secondfamily" in the column "mycolumn". The column name is matched AFTER it was converted by the column name mapper.>
              :* {:* <fn that converts all values>}

It works like this:

    user=> (hb/get db :files "myfile.txt" {} {:column {"content-type" #(String. %)}
                                              :value {"content-type" {:* #(String. %)}}})
    {"content-type" {"" {#inst "2014-07-27T03:12:10.639-00:00" "text/plain"}},
     "byte-data" {#<byte[] [B@38ed8225> {#inst "2014-07-27T03:12:10.639-00:00" #<byte[] [B@78c53110>}}}

    user=> (hb/get db :files "myfile.txt" {} {:value {:* {:* #(String. %)}}})
    {"content-type" {#<byte[] [B@63f9fdd3> {#inst "2014-07-27T03:12:10.639-00:00" "text/plain"}},
     "byte-data" {#<byte[] [B@6f4218e7> {#inst "2014-07-27T03:12:10.639-00:00" "how neat"}}}

    user=> (hb/get db :files "myfile.txt" {} {:column {:* #(String. %)}
                                              :value {:* {:* #(String. %)}}})
    {"content-type" {"" {#inst "2014-07-27T03:12:10.639-00:00" "text/plain"}},
     "byte-data" {"" {#inst "2014-07-27T03:12:10.639-00:00" "how neat"}}}

It is also possible to `put` several columns at once:

    user=> (hb/put db :files "myfile.txt" {:byte-data {:first "first file"
                                                       "second" "second file"}
                                           :content-type "text/plain"})
    nil
    user=> (hb/get db :files "myfile.txt" {} {:column {:* #(String. %)}
                                              :value {:* {:* #(String. %)}}})
    {"content-type" {"" {#inst "2014-07-27T03:39:27.389-00:00" "text/plain"}},
     "byte-data" {"second" {#inst "2014-07-27T03:39:27.389-00:00" "second file"},
                  "first" {#inst "2014-07-27T03:39:27.389-00:00" "first file"}}}

And to `get` only specific families and columns:

    user=> (hb/get db :files "myfile.txt" {:family :byte-data} {:column {:* #(String. %)}
                                                                :value {:* {:* #(String. %)}}})
    {"byte-data" {"second" {#inst "2014-07-27T03:39:27.389-00:00" "second file"},
                  "first" {#inst "2014-07-27T03:39:27.389-00:00" "first file"}}}
    user=> (hb/get db :files "myfile.txt" {:family :byte-data
                                           :column :first}
                                          {:column {:* #(String. %)}
                                           :value {:* {:* #(String. %)}}})
    {"byte-data" {"first" {#inst "2014-07-27T03:39:27.389-00:00" "first file"}}}
    user=> (hb/get db :files "myfile.txt" {:family :byte-data
                                           :column [:first :second]}
                                          {:column {:* #(String. %)}
                                           :value {:* {:* #(String. %)}}})
    {"byte-data" {"second" {#inst "2014-07-27T03:39:27.389-00:00" "second file"}, "first" {#inst "2014-07-27T03:39:27.389-00:00" "first file"}}}

## License

Copyright © 2014 FIXME

Distributed under the Eclipse Public License either version 1.0 or (at
your option) any later version.
