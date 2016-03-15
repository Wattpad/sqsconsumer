// sqsdrain reads messages until interrupted off of an SQS queue and writes them to a file.
package main

import (
	"flag"
	"fmt"
	"io"
	"log"
	"os"
	"os/signal"

	"github.com/Wattpad/sqsconsumer"
	"github.com/Wattpad/sqsconsumer/middleware"
	"golang.org/x/net/context"
)

func main() {
	var outputFilename, queueName string
	flag.StringVar(&outputFilename, "out", "", "File for output (default: STDOUT)")
	flag.StringVar(&queueName, "queue", "", "Queue name to drain")
	flag.Parse()

	if queueName == "" {
		flag.Usage()
		log.Fatalln("Error: -queue is required")
	}

	// write to a file if specified, or standard out
	var output io.WriteCloser
	if outputFilename != "" {
		if of, err := os.Create(outputFilename); err != nil {
			log.Fatalf("Error creating output file: %s", err)
		} else {
			output = of
		}
	} else {
		output = os.Stdout
	}

	// set up a context for graceful shutdown on interrupt/kill
	ctx, cancel := context.WithCancel(context.Background())
	term := make(chan os.Signal, 1)
	signal.Notify(term, os.Interrupt, os.Kill)
	go func() {
		<-term
		log.Printf("Starting graceful shutdown")
		cancel()
	}()

	// configure the cleaner worker and start it
	w := &worker{
		Queue:  queueName,
		Output: output,
	}
	w.Run(ctx)

	output.Close()
}

type worker struct {
	Queue  string
	Output io.Writer
}

func (w *worker) Run(ctx context.Context) {
	sqs, err := sqsconsumer.SQSServiceForQueue(w.Queue)
	if err != nil {
		log.Fatalf("ERROR Could not set up queue '%s': %s\n", w.Queue, err)
	}

	// deleter context should only be cancelled after the consumer is really done
	delCtx, cancelDelete := context.WithCancel(context.Background())
	stack := middleware.DefaultStack(delCtx, sqs)

	// wrap the worker
	handler := middleware.ApplyDecoratorsToHandler(w.HandleMessage, stack...)

	// create the consumer bound to a queue and processor function and start running it with a context that will be cancelled when a graceful shutdown is requested
	log.Printf("Starting consumer for SQS queue: %s", w.Queue)
	sc := sqsconsumer.NewConsumer(sqs, handler)
	sc.Run(ctx)

	// only after the consumer is done should the deleter stop
	cancelDelete()
	log.Println("Shutdown complete")
}

func (w *worker) HandleMessage(ctx context.Context, msg string) error {
	if _, err := fmt.Fprintln(w.Output, msg); err != nil {
		return err
	}
	return nil
}
