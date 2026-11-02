;; Clojure over native BEEP using the pure-Java library (native/java). Run by tests/run_all.py.
;;   clojure -Sdeps '{:deps {io.minta/minta-beep {:local/root "../../native/java/target/minta-beep-0.1.0.jar"}}}' -M clojure.clj
(import '[io.minta.beep RpcClient RpcServer Rpc$Reply Rpc$Handler Headers])

(def server
  (.start (RpcServer. (reify Rpc$Handler
                        (handle [_ req]
                          (Rpc$Reply/of 200 (Headers/of (into-array String ["Content-Type" "text/plain"]))
                                        (str "clojure saw " (.method req) " " (.target req))))))
          "127.0.0.1" 0))

(with-open [c (RpcClient. "127.0.0.1" (.port server))]
  (let [r (.call c "GET" "/clj")]
    (println (.status r) (.text r))))
(.close server)
