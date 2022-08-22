package main

import (
	"flag"
	"log"
	"os"

	"github.com/Shopify/sarama"
	"github.com/maxwellpeterson/kafka-websocket-shim/shim"
	"github.com/pkg/errors"
)

var broker = flag.String("broker", "", "the hostname plus port of the broker, such as localhost:8787")
var tls = flag.Bool("tls", false, "whether to use tls for the websocket connection")

func main() {
	flag.Parse()
	if *broker == "" {
		log.Fatal("broker flag required")
	}

	sarama.Logger = log.New(os.Stdout, "[sarama] ", log.LstdFlags)

	cfg := sarama.NewConfig()
	cfg.Net.Proxy.Enable = true
	cfg.Net.Proxy.Dialer = shim.NewDialer(shim.DialerConfig{Tls: *tls})
	cfg.Version = sarama.V0_8_2_0

	_, err := sarama.NewClient([]string{*broker}, cfg)
	if err != nil {
		log.Fatal(errors.Wrap(err, "create client failed"))
	}

	log.Println("Created client!")
}
