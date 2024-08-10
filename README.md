## Fubsub
A publish/subscribe library build on FoundationDB
[![Test and lint](https://github.com/p14n/fubsub/actions/workflows/main-check.yaml/badge.svg)](https://github.com/p14n/fubsub/actions/workflows/main-check.yaml)

### Usage

```clojure
(defn minimal-handler
  [ctx msg]
  (println "received" msg))


(p14n.fubsub.core/start-consumer {:handlers {"mytopic" [handler]}
                                  :consumer-name "myconsumer"
                                  :subspace ["fubsub" "test"]
                                  :handler-context {}})

```

```clojure
(p14n.fubsub.producer/put-message "mytopic" "Hi there!" "dean" {:subspace ["fubsub" "test"]})

```

```shell
received {:subject "dean", :id "00-00-06-20-1D-5D-96-8E-00-00", :data "Hi there!", :time "2024-08-10T22:54:07.217Z"}
```

