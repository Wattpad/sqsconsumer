package main

import (
	"log"
	"os"
	"os/signal"
	"time"

	"fmt"
	"math/rand"

	goexpvar "expvar"
	"net/http"
	"runtime"

	"sync"

	"github.com/Wattpad/sqsconsumer"
	"github.com/Wattpad/sqsconsumer/middleware"
	"github.com/go-kit/kit/metrics/expvar"
	"golang.org/x/net/context"
)

// build with -ldflags "-X main.revision a123"
var revision = "UNKNOWN"

func init() {
	goexpvar.NewString("version").Set(revision)
}

func main() {
	region := "us-east-1"
	queueName := "example_queue"
	numFetchers := 3

	// set up an SQS service instance
	// note that you can modify the AWS config used - make your own sqsconsumer.AWSConfigOption
	// or just depend on ~/.aws/... or environment variables and don't pass any opts at all
	s, err := sqsconsumer.SQSServiceForQueue(queueName, sqsconsumer.OptAWSRegion(region))
	if err != nil {
		log.Fatalf("Could not set up queue '%s': %s", queueName, err)
	}

	// set up a context which will gracefully cancel the worker on interrupt
	fetchCtx, cancelFetch := context.WithCancel(context.Background())
	term := make(chan os.Signal, 1)
	signal.Notify(term, os.Interrupt, os.Kill)
	go func() {
		<-term
		log.Println("Starting graceful shutdown")
		cancelFetch()
	}()

	// set up metrics - note TrackMetrics does not run the http server, and uses expvar
	exposeMetrics()
	ms := fmt.Sprintf("%s.success", queueName)
	mf := fmt.Sprintf("%s.fail", queueName)
	mt := fmt.Sprintf("%s.time", queueName)
	track := middleware.TrackMetrics(ms, mf, mt)

	// set up middleware stack for each consumer
	delCtx, cancelDelete := context.WithCancel(context.Background())
	stack := middleware.DefaultStack(delCtx, s)

	// wrap the handler
	stack = append(stack, track)
	handler := middleware.ApplyDecoratorsToHandler(processMessage, stack...)

	// start the consumers
	log.Println("Starting queue consumers")

	wg := &sync.WaitGroup{}
	wg.Add(numFetchers)
	for i := 0; i < numFetchers; i++ {
		go func() {
			// create the consumer and bind it to a queue and processor function
			c := sqsconsumer.NewConsumer(s, handler)

			// start running the consumer with a context that will be cancelled when a graceful shutdown is requested
			c.Run(fetchCtx)

			wg.Done()
		}()
	}

	// wait for all the consumers to exit cleanly
	wg.Wait()

	// and only then shut down the deleter
	cancelDelete()
	log.Println("Shutdown complete")
}

// processMessage is an example processor function which randomly errors or delays processing and demonstrates using the context.
func processMessage(ctx context.Context, msg string) error {
	log.Printf("Starting processMessage for msg %s", msg)

	// simulate random errors and random delays in message processing
	r := rand.Intn(10)
	if r < 3 {
		return fmt.Errorf("a random error processing msg: %s", msg)
		//	} else if r < 6 {
		//		log.Printf("Sleeping for msg %s", msg)
		//		time.Sleep(45 * time.Second)
	}

	// handle cancel requests
	select {
	case <-ctx.Done():
		log.Println("Context done so aborting processing message:", msg)
		return ctx.Err()
	default:
	}

	// do the "work"
	log.Printf("MSG: '%s'", msg)
	return nil
}

// exposeMetrics adds expvar metrics updated every 5 seconds and runs the HTTP server to expose them.
func exposeMetrics() {
	goroutines := expvar.NewGauge("total_goroutines")
	uptime := expvar.NewGauge("process_uptime_seconds")

	start := time.Now()

	go func() {
		for range time.Tick(5 * time.Second) {
			goroutines.Set(float64(runtime.NumGoroutine()))
			uptime.Set(time.Since(start).Seconds())
		}
	}()

	log.Println("Expvars at http://localhost:8123/debug/vars")
	go http.ListenAndServe(":8123", nil)
}
