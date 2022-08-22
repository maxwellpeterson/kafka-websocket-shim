package main

import (
	"log"
	"os"

	"github.com/Shopify/sarama"
	"github.com/maxwellpeterson/kafka-websocket-shim/shim"
	"github.com/pkg/errors"
)

const workerAddr = "kafka-worker.archmap.workers.dev:443"

func main() {
	sarama.Logger = log.New(os.Stdout, "[sarama] ", log.LstdFlags)

	cfg := sarama.NewConfig()
	cfg.Net.Proxy.Enable = true
	cfg.Net.Proxy.Dialer = shim.NewDialer(true)
	cfg.Version = sarama.V0_8_2_0

	_, err := sarama.NewClient([]string{workerAddr}, cfg)
	if err != nil {
		log.Fatal(errors.Wrap(err, "create client failed"))
	}

	log.Println("Created client!")
}
