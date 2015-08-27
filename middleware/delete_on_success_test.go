package middleware

import (
	"runtime"
	"testing"
	"time"

	"errors"

	"fmt"
	"io/ioutil"
	"log"
	"os"

	"github.com/Wattpad/sqsconsumer"
	"github.com/Wattpad/sqsconsumer/mock"
	"github.com/Wattpad/sqsconsumer/sqsmessage"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/sqs"
	"github.com/golang/mock/gomock"
	. "github.com/smartystreets/goconvey/convey"
	"github.com/stretchr/testify/assert"
	"golang.org/x/net/context"
)

func TestDeleteQueueDeleteBatch(t *testing.T) {
	// log to /dev/null because the deleter is chatty
	log.SetOutput(ioutil.Discard)
	defer func() {
		log.SetOutput(os.Stderr)
	}()

	Convey("deleteQueue.deleteBatch()", t, func() {
		ctl := gomock.NewController(t)
		defer ctl.Finish()

		Convey("Given the expectation that SQS DeleteMessageBatch will be invoked once and succeed", func() {
			mSQS := mock.NewMockSQSAPI(ctl)
			mSQS.EXPECT().DeleteMessageBatch(gomock.Any()).Return(&sqs.DeleteMessageBatchOutput{}, nil)

			q := testMakeDeleteQueueWithTimeout(mSQS, time.Millisecond)

			Convey("When deleteBatch is invoked with a pair of delete request entries", func() {
				_, err := q.deleteBatch([]*sqs.DeleteMessageBatchRequestEntry{
					&sqs.DeleteMessageBatchRequestEntry{ReceiptHandle: aws.String("r1"), Id: aws.String("i1")},
					&sqs.DeleteMessageBatchRequestEntry{ReceiptHandle: aws.String("r2"), Id: aws.String("i2")},
				})

				Convey("Then the error should be nil", func() {
					So(err, ShouldBeNil)
				})
			})
		})

		Convey("Given the expectation that SQS DeleteMessageBatch will return a failure", func() {
			mSQS := mock.NewMockSQSAPI(ctl)
			mSQS.EXPECT().DeleteMessageBatch(gomock.Any()).Return(&sqs.DeleteMessageBatchOutput{
				Failed: []*sqs.BatchResultErrorEntry{
					&sqs.BatchResultErrorEntry{Id: aws.String("i1")},
				},
			}, nil)

			q := testMakeDeleteQueueWithTimeout(mSQS, time.Millisecond)

			Convey("When deleteBatch is invoked with a pair of delete request entries", func() {
				batch := []*sqs.DeleteMessageBatchRequestEntry{
					&sqs.DeleteMessageBatchRequestEntry{ReceiptHandle: aws.String("r1"), Id: aws.String("i1")},
					&sqs.DeleteMessageBatchRequestEntry{ReceiptHandle: aws.String("r2"), Id: aws.String("i2")},
				}
				fails, err := q.deleteBatch(batch)

				Convey("Then the error should be nil (because there was a valid response)", func() {
					assert.Nil(t, err, "Err is nil because there was a valid response")
				})

				Convey("And the failure should be returned", func() {
					assert.Equal(t, 1, len(fails), "1 failure")
					assert.Equal(t, batch[0], fails[0])
				})
			})
		})

		Convey("Given the expectation that SQS DeleteMessageBatch will return an error", func() {
			mSQS := mock.NewMockSQSAPI(ctl)
			mSQS.EXPECT().DeleteMessageBatch(gomock.Any()).Return(nil, assert.AnError)

			q := testMakeDeleteQueueWithTimeout(mSQS, time.Millisecond)

			Convey("When deleteBatch is invoked with a pair of delete request entries", func() {
				_, err := q.deleteBatch([]*sqs.DeleteMessageBatchRequestEntry{
					&sqs.DeleteMessageBatchRequestEntry{ReceiptHandle: aws.String("r1"), Id: aws.String("i1")},
					&sqs.DeleteMessageBatchRequestEntry{ReceiptHandle: aws.String("r2"), Id: aws.String("i2")},
				})

				Convey("Then an error should be returned", func() {
					So(err, ShouldNotBeNil)
				})
			})
		})
	})
}

func TestDeleteQueueAddToPendingDeletes(t *testing.T) {
	Convey("deleteQueue.addToPendingDeletes()", t, func() {
		ctl := gomock.NewController(t)
		defer ctl.Finish()

		Convey("Unique messages are queued", func() {
			Convey("Given a delete queue", func() {
				mSQS := mock.NewMockSQSAPI(ctl)
				q := testMakeDeleteQueueWithTimeout(mSQS, time.Millisecond)

				Convey("When invoked with 2 messages with unique MessageIDs", func() {
					q.addToPendingDeletes(&sqs.Message{MessageId: aws.String("1")})
					q.addToPendingDeletes(&sqs.Message{MessageId: aws.String("2")})

					Convey("Then the queue should contain 2 messages", func() {
						q.Lock()
						defer q.Unlock()
						So(len(q.entries), ShouldEqual, 2)
					})
				})
			})
		})

		Convey("Duplicate messages are ignored", func() {
			Convey("Given a delete queue", func() {
				mSQS := mock.NewMockSQSAPI(ctl)
				q := testMakeDeleteQueueWithTimeout(mSQS, time.Millisecond)

				Convey("When invoked with 2 messages with the same MessageID", func() {
					q.addToPendingDeletes(&sqs.Message{MessageId: aws.String("1")})
					q.addToPendingDeletes(&sqs.Message{MessageId: aws.String("1")})

					Convey("Then the queue should contain only 1 message", func() {
						q.Lock()
						defer q.Unlock()
						So(len(q.entries), ShouldEqual, 1)
					})
				})
			})
		})
	})
}

func TestDeleteQueueDeleteFromPending(t *testing.T) {
	// log to /dev/null because the deleter is chatty
	log.SetOutput(ioutil.Discard)
	defer func() {
		log.SetOutput(os.Stderr)
	}()

	Convey("deleteQueue.deleteFromPending()", t, func() {
		ctl := gomock.NewController(t)
		defer ctl.Finish()

		Convey("Given a delete queue with 1 entry in it", func() {
			mSQS := mock.NewMockSQSAPI(ctl)
			mSQS.EXPECT().DeleteMessageBatch(gomock.Any()).Return(&sqs.DeleteMessageBatchOutput{}, nil)

			q := testMakeDeleteQueueWithTimeout(mSQS, time.Millisecond)
			q.entries = []*sqs.DeleteMessageBatchRequestEntry{
				&sqs.DeleteMessageBatchRequestEntry{ReceiptHandle: aws.String("1")},
			}

			Convey("When invoked", func() {
				n := q.deleteFromPending()

				Convey("Then the queue should be empty", func() {
					q.Lock()
					defer q.Unlock()
					So(len(q.entries), ShouldEqual, 0)
				})

				Convey("And 1 message should have been deleted", func() {
					So(n, ShouldEqual, 1)
				})
			})
		})

		Convey("Given a delete queue with 11 entries in it (noting the batch size is 10)", func() {
			mSQS := mock.NewMockSQSAPI(ctl)
			mSQS.EXPECT().DeleteMessageBatch(gomock.Any()).Return(&sqs.DeleteMessageBatchOutput{}, nil)

			q := testMakeDeleteQueueWithTimeout(mSQS, time.Millisecond)
			q.entries = []*sqs.DeleteMessageBatchRequestEntry{
				&sqs.DeleteMessageBatchRequestEntry{ReceiptHandle: aws.String("1")},
				&sqs.DeleteMessageBatchRequestEntry{ReceiptHandle: aws.String("2")},
				&sqs.DeleteMessageBatchRequestEntry{ReceiptHandle: aws.String("3")},
				&sqs.DeleteMessageBatchRequestEntry{ReceiptHandle: aws.String("4")},
				&sqs.DeleteMessageBatchRequestEntry{ReceiptHandle: aws.String("5")},
				&sqs.DeleteMessageBatchRequestEntry{ReceiptHandle: aws.String("6")},
				&sqs.DeleteMessageBatchRequestEntry{ReceiptHandle: aws.String("7")},
				&sqs.DeleteMessageBatchRequestEntry{ReceiptHandle: aws.String("8")},
				&sqs.DeleteMessageBatchRequestEntry{ReceiptHandle: aws.String("9")},
				&sqs.DeleteMessageBatchRequestEntry{ReceiptHandle: aws.String("10")},
				&sqs.DeleteMessageBatchRequestEntry{ReceiptHandle: aws.String("11")},
			}

			Convey("When invoked", func() {
				n := q.deleteFromPending()

				Convey("Then the queue should be reduced by a batch of 10", func() {
					q.Lock()
					defer q.Unlock()
					So(len(q.entries), ShouldEqual, 1)
				})

				Convey("And 10 messages should have been deleted", func() {
					So(n, ShouldEqual, 10)
				})
			})
		})

		Convey("Given SQS will return 1 failure when a 2 message delete batch is requested", func() {
			mSQS := mock.NewMockSQSAPI(ctl)
			mSQS.EXPECT().DeleteMessageBatch(gomock.Any()).Return(&sqs.DeleteMessageBatchOutput{
				Failed: []*sqs.BatchResultErrorEntry{
					&sqs.BatchResultErrorEntry{Id: aws.String("i1")},
				},
			}, nil)

			Convey("And a delete queue with 2 entries in it", func() {
				q := testMakeDeleteQueueWithTimeout(mSQS, time.Millisecond)
				q.entries = []*sqs.DeleteMessageBatchRequestEntry{
					&sqs.DeleteMessageBatchRequestEntry{ReceiptHandle: aws.String("r1"), Id: aws.String("i1")},
					&sqs.DeleteMessageBatchRequestEntry{ReceiptHandle: aws.String("r2"), Id: aws.String("i2")},
				}

				Convey("When invoked", func() {
					n := q.deleteFromPending()

					Convey("Then the queue should be empty", func() {
						q.Lock()
						defer q.Unlock()
						So(len(q.entries), ShouldEqual, 0)
					})

					Convey("And only 1 message should have been deleted", func() {
						So(n, ShouldEqual, 1)
					})
				})
			})
		})

		Convey("Given SQS will return an error", func() {
			mSQS := mock.NewMockSQSAPI(ctl)
			mSQS.EXPECT().DeleteMessageBatch(gomock.Any()).Return(nil, errors.New("an error"))

			Convey("And a delete queue with 2 entries in it", func() {
				q := testMakeDeleteQueueWithTimeout(mSQS, time.Millisecond)
				q.entries = []*sqs.DeleteMessageBatchRequestEntry{
					&sqs.DeleteMessageBatchRequestEntry{ReceiptHandle: aws.String("r1"), Id: aws.String("i1")},
					&sqs.DeleteMessageBatchRequestEntry{ReceiptHandle: aws.String("r2"), Id: aws.String("i2")},
				}

				Convey("When invoked", func() {
					n := q.deleteFromPending()

					Convey("Then the queue should be unchanged", func() {
						q.Lock()
						defer q.Unlock()
						So(len(q.entries), ShouldEqual, 2)
					})

					Convey("And the deleted count should be 0", func() {
						So(n, ShouldEqual, 0)
					})
				})
			})
		})
	})
}

func TestDeleteQueueStart(t *testing.T) {
	// log to /dev/null because the deleter is chatty
	log.SetOutput(ioutil.Discard)
	defer func() {
		log.SetOutput(os.Stderr)
	}()

	Convey("deleteQueue.start()", t, func() {
		ctl := gomock.NewController(t)
		defer ctl.Finish()

		Convey("Accumulates from a channel", func() {
			Convey("Given an empty queue", func() {
				mSQS := mock.NewMockSQSAPI(ctl)
				q := testMakeDeleteQueueWithTimeout(mSQS, 10*time.Millisecond)

				Convey("When the queue is started", func() {
					ctx, cancel := context.WithCancel(context.Background())
					defer cancel()
					go q.start(ctx)

					Convey("And a message is written to the queue channel", func() {
						q.queue <- &sqs.Message{MessageId: aws.String("1")}

						Convey("Then the queue has one entry", func() {
							q.Lock()
							defer q.Unlock()
							So(len(q.entries), ShouldEqual, 1)
						})
					})
				})
			})
		})

		Convey("Does not leak goroutines", func() {
			Convey("Given a running queue", func() {
				mSQS := mock.NewMockSQSAPI(ctl)

				// convey uses goroutines, so must do everything in the leaf node because we care about routine count
				Convey("When the queue context is cancelled", func() {
					Convey("And the queue is given time to clean up", func() {
						Convey("Then the number of goroutines has not changed relative to the number running before starting", func() {
							// pause for Convey's goroutine mess to stabilize
							time.Sleep(time.Millisecond)

							// note the number of goroutines before starting
							n := runtime.NumGoroutine()

							// create the queue and run it
							q := testMakeDeleteQueueWithTimeout(mSQS, 10*time.Millisecond)
							ctx, cancel := context.WithCancel(context.Background())
							go q.start(ctx)

							// cancel the queue context and pause to let the number of routines to stabilize
							cancel()
							time.Sleep(time.Millisecond)

							// verify no routines leaked
							So(runtime.NumGoroutine(), ShouldEqual, n)
						})
					})
				})
			})
		})

		Convey("Deletes batches after a timeout", func() {
			Convey("Given an SQS expectation that the delete function will be invoked once", func() {
				mSQS := mock.NewMockSQSAPI(ctl)
				mSQS.EXPECT().DeleteMessageBatch(gomock.Any()).Return(&sqs.DeleteMessageBatchOutput{}, nil)

				Convey("And a running queue with a timeout of 10ms", func() {
					q := testMakeDeleteQueueWithTimeout(mSQS, 10*time.Millisecond)
					ctx, cancel := context.WithCancel(context.Background())
					defer cancel()
					go q.start(ctx)

					Convey("When a message is added to the queue (knowing the batch size is more than 1 item)", func() {
						q.queue <- &sqs.Message{MessageId: aws.String("1")}

						Convey("And more than 10ms passes with no more messages", func() {
							time.Sleep(15 * time.Millisecond)

							Convey("Then the expectation is satisfied", func() {
								ctl.Finish()
							})
						})
					})
				})
			})
		})

		Convey("Deletes batches when 10 accumulate", func() {
			Convey("Given a running queue with a timeout of 10ms", func() {
				mSQS := mock.NewMockSQSAPI(ctl)
				q := testMakeDeleteQueueWithTimeout(mSQS, 10*time.Millisecond)
				ctx, cancel := context.WithCancel(context.Background())
				defer cancel()
				go q.start(ctx)

				Convey("When 9 messages are added to the queue (knowing the batch size is 10)", func() {
					q.queue <- &sqs.Message{MessageId: aws.String("1")}
					q.queue <- &sqs.Message{MessageId: aws.String("2")}
					q.queue <- &sqs.Message{MessageId: aws.String("3")}
					q.queue <- &sqs.Message{MessageId: aws.String("4")}
					q.queue <- &sqs.Message{MessageId: aws.String("5")}
					q.queue <- &sqs.Message{MessageId: aws.String("6")}
					q.queue <- &sqs.Message{MessageId: aws.String("7")}
					q.queue <- &sqs.Message{MessageId: aws.String("8")}
					q.queue <- &sqs.Message{MessageId: aws.String("9")}

					Convey("After a short wait to give the queue time to run", func() {
						time.Sleep(2 * time.Millisecond)

						Convey("Then SQS DeleteMessageBatch was not invoked", func() {
							// finish verifies the expectation, which is empty
							ctl.Finish()

							Convey("When the expectation is configured on the SQS mock", func() {
								mSQS.EXPECT().DeleteMessageBatch(gomock.Any()).Return(&sqs.DeleteMessageBatchOutput{}, nil)

								Convey("And a 10th message is added to the queue", func() {
									q.queue <- &sqs.Message{MessageId: aws.String("10")}

									Convey("After a short wait to give the queue time to run", func() {
										time.Sleep(2 * time.Millisecond)

										Convey("Then the expectation is satisfied", func() {
											ctl.Finish()
										})
									})
								})
							})
						})
					})
				})
			})
		})
	})
}

func TestSQSBatchDeleteOnSuccessWithTimeout(t *testing.T) {
	// log to /dev/null because the deleter is chatty
	log.SetOutput(ioutil.Discard)
	defer func() {
		log.SetOutput(os.Stderr)
	}()

	Convey("SQSBatchDeleteOnSuccessWithTimeout()", t, func() {
		ctl := gomock.NewController(t)
		defer ctl.Finish()

		Convey("Deletes batches after a timeout", func() {
			Convey("Given an SQS expectation that the delete function will be invoked once", func() {
				mSQS := mock.NewMockSQSAPI(ctl)
				mSQS.EXPECT().DeleteMessageBatch(gomock.Any()).Return(&sqs.DeleteMessageBatchOutput{}, nil)

				Convey("And a running queue with a timeout of 10ms", func() {
					ctx, cancel := context.WithCancel(context.Background())
					defer cancel()

					u := "a/url"
					srv := &sqsconsumer.SQSService{Svc: mSQS, URL: &u}

					deleter := SQSBatchDeleteOnSuccessWithTimeout(ctx, srv, 10*time.Millisecond)

					Convey("And a handler that always returns success", func() {
						handler := func(_ context.Context, _ string) error { return nil }
						handler = deleter(handler)

						Convey("When a message is successfully processed", func() {
							msg := &sqs.Message{MessageId: aws.String("1"), Body: aws.String("message")}
							msgCtx := sqsmessage.NewContext(ctx, msg)
							handler(msgCtx, *msg.Body)

							Convey("And more than 10ms passes with no more messages", func() {
								time.Sleep(15 * time.Millisecond)

								Convey("Then the expectation is satisfied (deletion was invoked)", func() {
									ctl.Finish()
								})
							})
						})
					})
				})
			})
		})

		Convey("Deletes batches when 10 accumulate", func() {
			Convey("Given an SQS expectation that the delete function will not be invoked", func() {
				mSQS := mock.NewMockSQSAPI(ctl)

				Convey("And a running queue with a timeout of 10ms", func() {
					ctx, cancel := context.WithCancel(context.Background())
					defer cancel()

					u := "a/url"
					srv := &sqsconsumer.SQSService{Svc: mSQS, URL: &u}

					deleter := SQSBatchDeleteOnSuccessWithTimeout(ctx, srv, 10*time.Millisecond)

					Convey("And a handler that always returns success", func() {
						handler := func(_ context.Context, _ string) error { return nil }
						handler = deleter(handler)

						Convey("When 9 messages are successfully processed", func() {
							for i := 1; i <= 9; i++ {
								msg := &sqs.Message{
									MessageId: aws.String(fmt.Sprintf("%d", i)),
									Body:      aws.String("message"),
								}
								msgCtx := sqsmessage.NewContext(ctx, msg)
								handler(msgCtx, *msg.Body)
							}

							Convey("After a short wait to give the queue time to run", func() {
								time.Sleep(2 * time.Millisecond)

								Convey("Then SQS DeleteMessageBatch was not invoked", func() {
									// finish verifies the expectation, which is empty
									ctl.Finish()

									Convey("When the expectation is configured on the SQS mock", func() {
										mSQS.EXPECT().DeleteMessageBatch(gomock.Any()).Return(&sqs.DeleteMessageBatchOutput{}, nil)

										Convey("And a 10th message is added to the queue", func() {
											msg := &sqs.Message{MessageId: aws.String("10"), Body: aws.String("message")}
											msgCtx := sqsmessage.NewContext(ctx, msg)
											handler(msgCtx, *msg.Body)

											Convey("After a short wait to give the queue time to run", func() {
												time.Sleep(2 * time.Millisecond)

												Convey("Then the expectation is satisfied (deletion was invoked)", func() {
													ctl.Finish()
												})
											})
										})
									})
								})
							})
						})
					})
				})
			})
		})

		Convey("Does not delete on error", func() {
			Convey("Given an SQS expectation that the delete function will not be invoked", func() {
				mSQS := mock.NewMockSQSAPI(ctl)

				Convey("And a running queue with a timeout of 10ms", func() {
					ctx, cancel := context.WithCancel(context.Background())
					defer cancel()

					u := "a/url"
					srv := &sqsconsumer.SQSService{Svc: mSQS, URL: &u}

					deleter := SQSBatchDeleteOnSuccessWithTimeout(ctx, srv, 10*time.Millisecond)

					Convey("And a handler that always returns an error", func() {
						handler := func(_ context.Context, _ string) error { return errors.New("an error") }
						handler = deleter(handler)

						Convey("When a message fails to process", func() {
							msg := &sqs.Message{MessageId: aws.String("1"), Body: aws.String("message")}
							msgCtx := sqsmessage.NewContext(ctx, msg)
							handler(msgCtx, *msg.Body)

							Convey("And more than 10ms passes with no more messages", func() {
								time.Sleep(15 * time.Millisecond)

								Convey("Then the expectation is satisfied (deletion was not invoked)", func() {
									ctl.Finish()
								})
							})
						})
					})
				})
			})
		})
	})
}

func testMakeDeleteQueueWithTimeout(s sqsconsumer.SQSAPI, timeout time.Duration) *deleteQueue {
	u := "url"
	return &deleteQueue{
		svc:                 s,
		url:                 &u,
		accumulationTimeout: timeout,
		queue:               make(chan *sqs.Message),
	}
}
