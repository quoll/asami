(ns ^{:doc "An connection wrapper for existing graphs with memory-only updates"
      :author "Paula Gearon"}
    asami.durable.memwrap
  (:require [asami.graph :as gr :refer [Graph graph-add graph-delete graph-diff resolve-triple count-triple]]
            [asami.storage :as storage]
            [asami.memory :refer [->MemoryWrapConnection ->MemoryConnection]]
            [schema.core :as s :include-macros true]))

(defn wrapped-db
  "Creates a DB based on a wrapgraph"
  [db]
  )

(defn wrap-connection
  "Returns a new wrapped connection. This is almost the same as a normal
  in-memory database, but the connection is no longer attached to the session."
  [connection]
  ;; create a wrapped with a copy of the state
  (let [nm (storage/get-name connection)
        db (wrapped-db (storage/db connection))
        internal-connection (->MemoryConnection nm (atom {:db db :history []}))]
    (->MemoryWrapConnection internal-connection
                            nm
                            (storage/next-tx connection)
                            wrapped-db)))
