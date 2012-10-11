;; MongerDB interop

; CAPITALS signify particularly important methods

; Entry methods: count-db, ADD, RETRIEVE, DELETE, FULL-SEARCH, find-not-tag,
;                get-info, delete-dupes, any?, EDIT, TAG, RM-TAG, FIND-TAG,
;                update-matches,

; Try not to delete anything from playlist "all", as this is where all objects will
; exist after conversion. If you want to delete a sample from ease's knowledge,
; use method erase-sample from namespace ease.storage. This will remove the sample's
; binary from ease's data-dir, and the metadata from playlist "all", but not from
; other playlists which may confuse ease. So be careful.

(ns transitions.mongo
  (:use [monger.core :only [connect! set-db! get-db]]
        [monger.collection
         :only [update insert-batch find-maps remove save]
         :rename {update update-db remove remove-db save save-db
                  find-maps find-maps-db}]))

(def doc-name "transitions")

(defn activate-db
  "Initialize this db with given name."
  [name]
  (do (connect!)
      (set-db! (get-db name))))

(activate-db doc-name)

(defn _retrieve
  "Defaults the document to doc-name for consistency and abstraction."
  [& args]
  (apply find-maps-db (cons doc-name args)))

(defn _update
  "Defaults the document to doc-name for consistency and abstraction."
  [& args]
  (apply update-db (cons doc-name args)))

(defn _remove
  "Defaults the document to doc-name for consistency and abstraction."
  [& args]
  (apply remove-db (cons doc-name args)))
  
(defn _save
  "Defaults the document to doc-name for consistency and abstraction."
  [& args]
  (apply save-db (cons doc-name args)))

(defn _save-batch
  "Defaults the document to doc-name for consistency and abstraction."
  [& args]
  (apply insert-batch (cons doc-name args)))

(defn _id-map
  "Extracts the id of given map and returns it in the map form of:
   {:_id extracted-id}."
  [map]
  {:_id (:_id map)})

(defn find-maps
  "Returns a list of all objects with given criteria map, or nil. (find-maps)
   returns all objects."
  ([] (_retrieve))
  ([criteria] (_retrieve criteria)))

(defn handle-coll-or-map
  "Higher order function which executes and announces given function based on
   whether the input is a map or a coll, and also handles type errors."
  [fxn-name map-fxn coll-fxn map-or-coll & args]
  (cond (map? map-or-coll)
        (apply map-fxn (cons map-or-coll args))
        (coll? map-or-coll)
        (apply coll-fxn (cons map-or-coll args))
        :else
        (println "Method" fxn-name "given unknown type for map-or-coll.")))

(defn _edit-fxn-for-map
  "Functions to be called when performing edit on an object map."
  [object criteria]
  (if-let [id (_id-map object)]  ; if object has an :_id
    (do (_update id criteria)    ; make the changes
        (find-maps {:_id id}))
    (if-let [updated-object (find-maps object)]      ; if object had no :_id, update
      (_edit-fxn-for-map updated-object criteria)))) ; then try again

(defn make-coll-recursive-fn
  "Returns a function which maps given function with args over coll."
  [function]
  (fn [coll & args]
    (doall (map #(apply function (cons % args)) coll))))

(defn add
  "Adds (and returns) given object(s)'s metadata to ease's metadatabase.
   Returns list if given a list, object if given a object."
  [objects]
  (handle-coll-or-map "add"
                      #(do (_save %) (first (find-maps %)))
                      #(_save-batch (vec %))
                      objects))

(defn edit
  "Edits given object's or list of objects' info with given criteria in the form
   of a map. If given object does not have an :_id field, will attempt to find the
   object which matches given object's info. CAUTION: edit WILL replace the stored
   object with given criteria if given no operators.
   Returns the updated object or list of objects."
  [objects criteria]
  (handle-coll-or-map "edit"
                      _edit-fxn-for-map
                      (make-coll-recursive-fn edit)
                      objects
                      criteria))

(defn delete
  "Removes given object(s)'s metadata from ease's metadatabase. Does NOT remove
   the corresponding folder from ease's data-dir."
  [objects]
  (handle-coll-or-map "delete"
                      #(_remove (_id-map %))
                      (make-coll-recursive-fn delete)
                      objects))