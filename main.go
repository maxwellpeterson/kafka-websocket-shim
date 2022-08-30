package main

import (
	"context"
	"flag"
	"log"
	"os"

	"github.com/maxwellpeterson/kafka-websocket-shim/shim"
	"github.com/pkg/errors"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kversion"
)

var broker = flag.String("broker", "", "the hostname plus port of the broker, such as localhost:8787")
var tls = flag.Bool("tls", false, "whether to use tls for the websocket connection")

func main() {
	flag.Parse()
	if *broker == "" {
		log.Fatal("broker flag required")
	}

	dialer := shim.NewDialer(shim.DialerConfig{TLS: *tls})
	logger := kgo.BasicLogger(
		os.Stdout,
		kgo.LogLevelDebug,
		func() string { return "[kgo] " },
	)

	cl, err := kgo.NewClient(
		kgo.SeedBrokers(*broker),
		kgo.MaxVersions(kversion.V0_8_0()),
		kgo.ConsumeTopics("test-topic"),
		kgo.Dialer(dialer.DialContext),
		kgo.WithLogger(logger),
		kgo.ProducerBatchCompression(kgo.NoCompression()),
	)
	if err != nil {
		log.Fatal(errors.Wrap(err, "client create failed"))
	}
	defer cl.Close()
	log.Println("Created client!")

	record := &kgo.Record{Topic: "test-topic", Value: []byte("bar")}
	if err := cl.ProduceSync(context.Background(), record).FirstErr(); err != nil {
		log.Fatal(errors.Wrap(err, "produce failed"))
	}
	log.Println("Produced record!")
}
