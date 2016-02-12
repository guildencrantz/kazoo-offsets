# Monitor Kafka Consumer Partition Offsets in Zookeeper

Simple golang based (kazoo-go and sarama) application to monitor Kafka offsets stored in Zookeeper, and calculate lag based on current Kafka partition sizes. Output is modeled after the official `kafka.tools.ConsumerOffsetChecker`.

# Install

    go install github.com/guildencrantz/kazoo-offsets

# Run

For current execution flags see `kazoo-offsets -h`

In general if you're using CLI flags the pattern is:
    kazoo-offsets -zookeeper $(ZK_HOST):2181 -group-id $(KAFKA_GROUP_ID) -topic $(KAFKA_TOPIC)

If you want the values to update I highly recommeend using [watch](http://linux.die.net/man/1/watch)
    watch -n 5 kazoo-offsets -zookeeper $(ZK_HOST):2181 -group-id $(KAFKA_GROUP_ID) -topic $(KAFKA_TOPIC)

