
## Jackdaw Repl

The purpose of this library is to improve the developer experience when developing Kafka Streams applications.

When we’re developing a set of kafka topologies, we don’t want to have to deal with Avro Schemas as we’re still experimenting and trying to flesh out the shapes of the messages flowing across the topologies and the various aggregates. Instead, we want to stay in the REPL and focus on the Clojure code to implement the topologies and have everything else be dealt automatically for us.

Sources of inspiration:
- Charles Reese's [demo of Jackdaw at the Conj 2018](https://youtu.be/eJFBbwCB6v4)
- [integrant-repl](https://github.com/weavejester/integrant-repl) the companion lib to [integrant](https://github.com/weavejester/integrant).

## Approach

This lib contains bit extracted from the Jackdaw [user.clj](https://github.com/FundingCircle/jackdaw/blob/master/examples/dev/user.clj) namespace in the example directory folder and adds the following:

- The concept of a topic registry which is just a map of topic config, exactly the same shape than topics-metadata. The particular topic registry implementation we have in jackdaw-repl is a clojure Record which automatically creates a topic (ideally in the running Docker Confluent stack) and adds an entry in the topic registry with EDN serdes whenever you `get` a topic from it. It also works when destructuring any topic name.
- Support for Reloaded workflow via [integrant-repl](https://github.com/weavejester/integrant-repl)
- Startup and cleanup of the system (topics created and destroyed).


## How to use

1. Start a fresh confluent stack:

```bash
git clone https://github.com/confluentinc/cp-docker-images && cd cp-docker-images/examples/cp-all-in-one
;; we don't need the full stack
docker-compose up -d rest-proxy 
```

2. Fire up a REPL and create a scratch.clj similar from the [one in the example](jackdaw-repl/blob/master/examples/dev/scratch.clj). This namespace resets the whole system each time we evaluate it (including deleting/recreating the topics), this usually takes only 1 or 2 seconds. 

3. Execute the `do` block in the `(comment)` section of `scratch.clj` to run a particular scenario to flow some data into the freshly reset system and put it into action. Most of the time we can do this multiple times without having the kill and restart the REPL, this is basically the famous Stuart Sierra's reloaded workflow, but instead of Component we use Integrant here.

4. When things go wrong (really bad state) we just have to:
- restart the confluent stack with `docker-compose down && docker-compose up -d rest-proxy`
- sometimes clear up your local `/tmp/kafka-streams` directory. Normally, that should not be necessary as the kafka-streams app names are randomly generated when we start the topologies up.

Note that lib was developed at the same time as https://github.com/FundingCircle/kafka-productionised which is a production-grade quality lib which extracted commonly used bits from production repos, which also has this notion of Topic Registry but its implementation uses `topics-metadata` instead.
