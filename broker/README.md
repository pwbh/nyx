# Broker

Broker is one of the core components of nyx, this is the unit which maintaince the integrity of your partitions, manges and stores the actual data of the partition, and talks with the consumers which consume the data from it in their own pace, advancing an index one by one whenever they are ready to consume more data.

Multiple brokers can run now on the same machine by specifiying a unique name when running a new broker instance.

## Usage

```
cargo run --bin broker -- <HOST>
```

### Running a broker with custom name

```
cargo run --bin broker -- <HOST> --name <NAME>
```
