# Observer [WIP]

Obvserver will send in messages to the Brokers connected to it, for example on creation of a new Topic, Observer is responsible for distributing the pratitions of the topic on the Brokers for maximum fault-tolerence.

### A typical flow of starting Imp:

1. User launches for example 3 Brokers, Brokers in a state where they are looking for the observer with a specified host passed as param, on dev we can start the broker using `cargo run --bin broker localhost:5555` we assume that Observer runs on port 5555 locally. While the Observer is shut down, broker is just trying to connect to the Observer in intervals (which can be customized)

2. Observer is launched and the Brokers connecting to it, once connected, Observer prints a message that state so (TBD), Observer is now ready to receive commands and execute them.

### Available commands

Create topic

```bash
CREATE TOPIC [TOPIC_NAME]
```

Create partition for topic

```
CREATE PARTITION [TOPIC_NAME] [AMOUNT]
```

Delete topic (will delete all partitions)

```
DELETE TOPIC [TOPIC_NAME]
```

Delete paritition from a topic (will delete a partition with its given number)

```
DELETE PARTITION [TOPIC_NAME] [PARTITION_NUM]
```

Connects a new spawned broker to the Observer

```
CONNECT [ADDRESS]
```

Disconnect broker partition rebalancing will take place

```
DISCONNECT [BROKER_ID]
```

Lists an entity of a choice, see examples below, Entity ID can be a broker, topic, or a partition id

```
LIST [ENTITY_ID]
```

Commands will return either `OK` after each execution or `ERR: [ERROR_DESCRIPTION]` to the terminal.
