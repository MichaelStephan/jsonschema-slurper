(ns jsonschema-slurper.core
  (:require [clojure.data.json :as json]
            [clojure.pprint :as pp]
            [datomic.api :as d]
            [com.rpl.specter :as sp]))

(defn datomic-schema [id type cardinality doc]
  (cond->
    {:db/ident id
     :db/valueType type
     :db/cardinality cardinality
     :db/doc (or doc "")
     :db/isComponent (or (= cardinality :db.cardinality/many)
                         (contains? #{:technicalAsset/description
                                      :technicalAsset/provider
                                      :technicalAsset/composedId
                                      :environment/region
                                      :environment/infrastructure
                                      :environment/platform
                                      :entitlementRule/technicalAssetId
                                      :technicalAssetId/composedId
                                      :product/provider
                                      :product/composedId
                                      :technicalAsset/prerequisites
                                      :prerequisites/technicalAssetPrerequisites
                                      :prerequisites/textualPrerequisites}
                                    id))}))

(defn make-singular [nsk]
  (let [ns (namespace nsk) k (name nsk)]
    (keyword ns (if (.endsWith k "s") (clojure.string/join "" (drop-last k)) k))))

(defn make-namespaced [prefix k]
  (keyword (name prefix) (name k)))

(defn traverse-schema
  ([resolve-path f prefix]
   (traverse-schema resolve-path f prefix
                    (-> f (resolve-path) (slurp) (json/read-str :key-fn keyword))))
  ([resolve-path f prefix {:keys [properties]}]
   (flatten
     (for [[k {:keys [type items $ref properties title] :or {type "object"}}] properties
             :let [nsk (make-namespaced prefix k)]]
       (condp = type
         "array" (conj (if-let [ref (:$ref items)]
                         (when-not (.startsWith ref "#/definitions/")
                           (traverse-schema resolve-path ref (make-singular k)))
                         (traverse-schema resolve-path f (make-singular k) items))
                       (datomic-schema nsk :db.type/ref :db.cardinality/many title))
         "object" (conj (if (nil? $ref) ; TODO deal with nested schemas
                          (traverse-schema resolve-path f k properties)
                          (when-not (.startsWith $ref "#/definitions/")
                            (traverse-schema resolve-path $ref k)))
                        (datomic-schema nsk :db.type/ref :db.cardinality/one title))
         "string" (datomic-schema nsk :db.type/string :db.cardinality/one title)
         "number" (datomic-schema nsk :db.type/bigdec :db.cardinality/one title)
         "boolean" (datomic-schema nsk :db.type/boolean :db.cardinality/one title)
         (throw (ex-info "unknown type")))))))

(defn traverse-catalog [prefix catalog]
  (reduce
      (fn [ret [k v]]
        (let [nsk (make-namespaced prefix k)]
          (cond
            (or #_(= k :prerequisites) ; TODO deal with full structure 
                  (= k :ratePlans)) ret
            (= k :id) (assoc ret
                             :db/id v
                             nsk v) 
            (= k :composedId) (assoc ret
                                     :db/id (str (:id v) "/" (:version v) "/" (:catalog v))
                                     nsk (traverse-catalog k v)) 
            :else (cond
                    (map? v) (let [x (traverse-catalog k v)] (if (empty? x) ret (assoc ret nsk x)))
                    (coll? v) (let [x (remove empty? (map (partial traverse-catalog (make-singular k)) v))] (if (empty? x) ret (assoc ret nsk x))) 
                    :else (assoc ret nsk v)))))
      {}
      catalog))

(comment
  (pp/pprint (traverse-schema #(str "/Users/i303874/dev/data-exchange/json/schemas/" %) "catalog.json" :venezia))
 
  (pp/pprint (traverse-catalog "venezia"
                               (-> "/Users/i303874/dev/cmt-prod/cmt-cis-htp740461067-2018-03-01-18-23-30.json"
                                   (slurp)
                                   (json/read-str :key-fn keyword)))))

(def schema (-> (traverse-schema #(str "/Users/i303874/dev/data-exchange/json/schemas/" %) "catalog.json" "venezia")
                (concat [{:db/ident :media/title
                          :db/valueType :db.type/string
                          :db/cardinality :db.cardinality/one}
                         {:db/ident :media/url
                          :db/valueType :db.type/string
                          :db/cardinality :db.cardinality/one}
                         {:db/ident :media/format
                          :db/valueType :db.type/string
                          :db/cardinality :db.cardinality/one}
                         {:db/ident :region/id
                          :db/valueType :db.type/string
                          :db/cardinality :db.cardinality/one}
                         {:db/ident :region/name
                          :db/valueType :db.type/string
                          :db/cardinality :db.cardinality/one}
                         {:db/ident :region/technicalName
                          :db/valueType :db.type/string
                          :db/cardinality :db.cardinality/one}
                         {:db/ident :infrastructure/name
                          :db/valueType :db.type/string
                          :db/cardinality :db.cardinality/one}
                         {:db/ident :infrastructure/id
                          :db/valueType :db.type/string
                          :db/cardinality :db.cardinality/one}
                         {:db/ident :platform/id
                          :db/valueType :db.type/string
                          :db/cardinality :db.cardinality/one}
                         {:db/ident :platform/name
                          :db/valueType :db.type/string
                          :db/cardinality :db.cardinality/one}
                         {:db/ident :ratePlan/revenueCloudRatePlanId 
                          :db/valueType :db.type/string
                          :db/cardinality :db.cardinality/one}
                         {:db/ident :group/id 
                          :db/valueType :db.type/string
                          :db/cardinality :db.cardinality/one}
                         {:db/ident :group/validFrom
                          :db/valueType :db.type/string
                          :db/cardinality :db.cardinality/one}
                         {:db/ident :entitlementRule/materialNumber 
                          :db/valueType :db.type/string
                          :db/cardinality :db.cardinality/one}
                         {:db/ident :entitlementRule/revenueCloudProductId
                          :db/valueType :db.type/string
                          :db/cardinality :db.cardinality/one}
                         {:db/ident :product/contractTypes
                          :db/valueType :db.type/ref
                          :db/cardinality :db.cardinality/many
                          :db/isComponent true}
                         {:db/ident :technicalAssetPrerequisite/technicalAssetId
                          :db/valueType :db.type/ref
                          :db/cardinality :db.cardinality/one}
                         ])))

(def catalog (traverse-catalog "venezia"
                               (-> "/Users/i303874/dev/cmt-prod/cmt-cis-htp740461067-2018-03-01-18-23-30.json"
                                   (slurp)
                                   (json/read-str :key-fn keyword))))


#_[:find ?technical-asset-technical-name
 :where
 [_ :venezia/products ?product]
 [?product :product/entitlementRules ?entitlement-rule]
 [?entitlement-rule :entitlementRule/technicalAssetId ?technical-asset]
 [?technical-asset :technicalAsset/technicalName ?technical-asset-technical-name]]

#_(spit "/Users/i303874/Desktop/schema.edn" (with-out-str (pp/pprint schema)))

#_(spit "/Users/i303874/Desktop/catalog.edn" (with-out-str (pp/pprint catalog)))

(comment
  (d/create-database "datomic:dev://localhost:4334/venezia")
  (let [uri "datomic:dev://localhost:4334/venezia"
        conn (d/connect uri)]
    (d/transact conn schema))
  
  (let [uri "datomic:dev://localhost:4334/venezia"
        conn (d/connect uri)]
    (d/transact conn [catalog]))
 
  (let [uri "datomic:dev://localhost:4334/venezia"
        conn (d/connect uri)]
    (d/q '[:find ?id
           :where
           [?tire :my/tire.id ?id]
           [_ :my/car.tires ?tire]]
         (d/db conn))))

(def myschema [{:db/ident :my/car.numberplate
                :db/cardinality :db.cardinality/one
                :db/valueType :db.type/string}
               
               {:db/ident :my/car.brand
                :db/cardinality :db.cardinality/one
                :db/valueType :db.type/string}
               
               {:db/ident :my/car.tires
                :db/cardinality :db.cardinality/many
                :db/isComponent true
                :db/valueType :db.type/ref}
               
               {:db/ident :my/tire.id
                :db/cardinality :db.cardinality/one
                :db/valueType :db.type/string}])

(def mycars [{:my/car.numberplate "RI-145Z"
              :my/car.brand "BMW"
              :my/car.tires [{:my/tire.id "123"}
                             {:my/tire.id "456"}
                             {:my/tire.id "789"}
                             {:my/tire.id "012"}]}])

(comment
  (d/create-database "datomic:dev://localhost:4334/my")
  (let [uri "datomic:dev://localhost:4334/my"
        conn (d/connect uri)]
    (d/transact conn myschema))
 (let [uri "datomic:dev://localhost:4334/my"
        conn (d/connect uri)]
    (d/transact conn mycars)))




