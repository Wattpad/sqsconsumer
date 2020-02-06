package main

import (
	"expvar"
	"fmt"
	"log"
	"math/rand"
	"net/http"
	"os"
	"os/signal"
	"runtime"
	"sync"
	"time"

	"github.com/Wattpad/sqsconsumer"
	"github.com/Wattpad/sqsconsumer/middleware"
	"golang.org/x/net/context"
)

// build with -ldflags "-X main.revision a123"
var revision = "UNKNOWN"

func init() {
	expvar.NewString("version").Set(revision)
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

	shutDown := make(chan struct{})
	term := make(chan os.Signal, 1)
	signal.Notify(term, os.Interrupt, os.Kill)

	go func() {
		<-term
		log.Println("Starting graceful shutdown")
		close(shutDown)
	}()

	// set up metrics - note TrackMetrics does not run the http server, and uses expvar
	exposeMetrics()
	ms := expvar.NewInt(fmt.Sprintf("%s.success", queueName))
	mf := expvar.NewInt(fmt.Sprintf("%s.fail", queueName))
	mt := expvar.NewFloat(fmt.Sprintf("%s.time", queueName))
	track := middleware.TrackMetrics(ms, mf, mt)

	// wrap the handler
	handler := middleware.ApplyDecoratorsToHandler(processMessage, track)

	// start the consumers
	log.Println("Starting queue consumers")

	// create the consumer and bind it to a queue and processor function
	c := sqsconsumer.NewConsumer(s, handler)
	c.SetLogger(log.Printf)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	wg := &sync.WaitGroup{}
	wg.Add(numFetchers)
	for i := 0; i < numFetchers; i++ {
		go func() {
			c.Run(ctx, sqsconsumer.WithShutdownChan(shutDown))
			wg.Done()
		}()
	}

	<-shutDown
	time.AfterFunc(30*time.Second, cancel)

	// wait for all the consumers to exit cleanly
	wg.Wait()
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
	goroutines := expvar.NewInt("total_goroutines")
	uptime := expvar.NewFloat("process_uptime_seconds")

	start := time.Now()

	go func() {
		for range time.Tick(5 * time.Second) {
			goroutines.Set(int64(runtime.NumGoroutine()))
			uptime.Set(time.Since(start).Seconds())
		}
	}()

	log.Println("Expvars at http://localhost:8123/debug/vars")
	go http.ListenAndServe(":8123", nil)
}
