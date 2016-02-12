package main

import (
	"flag"
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"text/tabwriter"
	"time"

	"github.com/Shopify/sarama"
	"github.com/cznic/sortutil"
	"github.com/wvanbergen/kazoo-go"
)

var (
	zookeeper        = flag.String("zookeeper", os.Getenv("ZOOKEEPER_PEERS"), "Zookeeper connection string. It can include a chroot.")
	zookeeperTimeout = flag.Int("zookeeper-timeout", 1000, "Zookeeper timeout in milliseconds.")
	topic            = flag.String("topic", os.Getenv("KAFKA_TOPIC"), "Kafka topic")
	group            = flag.String("group-id", os.Getenv("KAFKA_GROUP_ID"), "Kafka Consumer Group ID")
)

type ConsumerOffsets struct {
	Group      string
	Topic      string
	Partitions map[int32]*PartitionOffsets
}

func (co ConsumerOffsets) Print() {
	var ps sortutil.Int32Slice
	for k, _ := range co.Partitions {
		ps = append(ps, k)
	}
	ps.Sort()

	w := new(tabwriter.Writer)
	w.Init(os.Stdout, 0, 8, 4, '\t', 0)

	fmt.Fprintf(w, "Group ID\tTopic\tPartition\tOffset\tLog Size\tLag\tOwner\n")
	for _, p := range ps {
		s := co.Partitions[p].Newest - co.Partitions[p].Oldest
		l := co.Partitions[p].Newest - co.Partitions[p].Current
		fmt.Fprintf(w, "%s\t%s\t%d\t%d\t%d\t%d\t%s\n", co.Group, co.Topic, p, co.Partitions[p].Current, s, l, co.Partitions[p].Owner)
	}

	w.Flush()
}

type PartitionOffsets struct {
	Owner   string
	Oldest  int64
	Newest  int64
	Current int64
}

func main() {
	flag.Parse()

	checkFlags()

	// Prevent zk library froom outputing garbage
	log.SetOutput(ioutil.Discard)

	conf := kazoo.NewConfig()
	conf.Timeout = time.Duration(*zookeeperTimeout) * time.Millisecond

	zk, err := kazoo.NewKazooFromConnectionString(*zookeeper, conf)
	if err != nil {
		printErrorAndExit(69, "Failed to connect to Zookeeper: %v", err)
	}
	defer zk.Close()

	groups, err := zk.Consumergroups()
	if err != nil {
		printErrorAndExit(69, "Failed to get Kafka Consumer Groups from Zookeeper: %v", err)
	}

	g := groups.Find(*group)

	offsets, err := g.FetchAllOffsets()
	if err != nil {
		printErrorAndExit(69, "Failed to get offsets for Kafka Consumer Group %s: %v", group, err)
	}

	co := ConsumerOffsets{
		Group:      *group,
		Topic:      *topic,
		Partitions: make(map[int32]*PartitionOffsets),
	}

	for p, o := range offsets[*topic] {
		owner, err := g.PartitionOwner(*topic, p)
		if err != nil {
			printErrorAndExit(69, "Failed to get partition owner for Consumer Group %s topic %s partition %d: err", group, topic, p, err)
		}
		co.Partitions[p] = &PartitionOffsets{
			Owner:   owner.ID,
			Current: o,
		}
	}

	b, err := zk.BrokerList()
	if err != nil {
		printErrorAndExit(69, "Failed to get kafka broker list from zookeeper: %v", err)
	}

	k, err := sarama.NewClient(b, sarama.NewConfig())
	if err != nil {
		printErrorAndExit(69, "Failed to connect to kafka: %v", err)
	}
	defer k.Close()

	ps, err := k.Partitions(*topic)
	if err != nil {
		printErrorAndExit(69, "Failed to get partitions for topic %s from kafka: %v", topic, err)
	}

	for _, p := range ps {
		oldest, err := k.GetOffset(*topic, p, sarama.OffsetOldest)
		if err != nil {
			printErrorAndExit(69, "Failed to get oldest offset for topic %s, partition %d, from kafka: %v", topic, p, err)
		}
		co.Partitions[p].Oldest = oldest

		newest, err := k.GetOffset(*topic, p, sarama.OffsetNewest)
		if err != nil {
			printErrorAndExit(69, "Failed to get newest offset for topic %s, partition %d, from kafka: %v", topic, p, err)
		}
		co.Partitions[p].Newest = newest
	}

	co.Print()
}

func printUsageErrorAndExit(format string, values ...interface{}) {
	fmt.Fprintf(os.Stderr, "ERROR: %s\n", fmt.Sprintf(format, values...))
	fmt.Fprintln(os.Stderr)
	fmt.Fprintln(os.Stderr, "Available command line options:")
	flag.PrintDefaults()
	os.Exit(64)
}

func printErrorAndExit(code int, format string, values ...interface{}) {
	fmt.Fprintf(os.Stderr, "ERROR: %s\n", fmt.Sprintf(format, values...))
	fmt.Fprintln(os.Stderr)
	os.Exit(code)
}

func checkFlags() {
	var err string

	if *zookeeper == "" {
		err += "A zookeeper host string is required\n"
	}

	if *group == "" {
		err += "A consumer group id is required.\n"
	}

	if *topic == "" {
		err += "A topic name is required.\n"
	}

	if err != "" {
		printUsageErrorAndExit(err)
	}
}
