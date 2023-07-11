# Observer [WIP]

Observer is a program that sends messages to brokers connected to it. For example, when a new topic is created, Observer is responsible for distributing the partitions of the topic across the brokers for maximum fault tolerance.

### Typical flow of starting Nyx

1. The user launches three brokers.
   The brokers are in a state where they are looking for the observer with a specified host passed as a parameter. On development, we can start the broker using `cargo run --bin broker localhost:5555`. We assume that Observer runs on port 5555 locally. While Observer is shut down, the broker is just trying to connect to Observer in intervals (which can be customized).

   You can use as much Observers as you want, but for a good fault-tolerence we recommend odd number of instances, no more then 5 for most of projects.

   #### Start the Observer as Leader

   ```
   cargo run --bin observer
   ```

   #### Starting the Observers as a follower

   ```
   cargo run --bin observer -- -f <HOST>
   ```

   You should have 1 Leader + even number of followers as a result for the recommened fault-tolerence.

2. Once the Leader Observer is launched, brokers will find the Observer and will connect to it to star exchanging metadata,
   be aware of different states through the Observer, receive the partitions and elect leaders. Once connected, Observer prints a message stating that it is ready to receive commands and execute them.

### Available commands

#### CREATE

Create topic

```bash
CREATE TOPIC [TOPIC_NAME]
```

Create partition for topic

```
CREATE PARTITION [TOPIC_NAME]
```

#### DELETE

Delete topic (will delete all partitions)

```
DELETE TOPIC [TOPIC_NAME]
```

Delete paritition from a topic (will delete a partition with its given number)

```
DELETE PARTITION [TOPIC_NAME] [PARTITION_NUM]
```

#### CONNECT/DISCONNECT

Connects a new spawned broker to the Observer

```
CONNECT [ADDRESS]
```

Disconnect broker partition rebalancing will take place

```
DISCONNECT [BROKER_ID]
```

#### LIST

List all entities

```
LIST ALL
```

List an entity of a choice

```
LIST [ENTITY_ID]
```

#### Exit

Will exit the program

```
EXIT
```

Commands will return either `OK` after each execution or `ERR: [ERROR_DESCRIPTION]` to the terminal.
