package app

import (
	"context"
	"errors"
	"path/filepath"
	"testing"
	"time"

	"ojj/internal/event"
	"ojj/internal/messagebuilder"
	"ojj/internal/startup"
)

func TestStartupEventPersistenceAndDelivery(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	dir := t.TempDir()
	eventConfig := event.Config{Path: filepath.Join(dir, "events.db"), PollInterval: time.Millisecond}
	builderConfig := messagebuilder.Config{Path: filepath.Join(dir, "messages.db"), QuietPeriod: time.Millisecond, PollInterval: time.Millisecond}
	var previousID string
	for start := 1; start <= 2; start++ {
		func() {
			store, err := event.Open(ctx, eventConfig)
			if err != nil {
				t.Fatal(err)
			}
			defer store.Close()
			builder, err := messagebuilder.Open(ctx, builderConfig)
			if err != nil {
				t.Fatal(err)
			}
			defer builder.Close()
			producer, err := store.BindProducer(startup.EventType)
			if err != nil {
				t.Fatal(err)
			}
			checked := make(chan string, 1)
			stop := errors.New("startup delivered")
			var eventID string
			err = runWithMessages(ctx, store, builder, func(call context.Context, msg messagebuilder.Message) error {
				events, err := builder.Events(call, msg.ID)
				if err != nil {
					return err
				}
				if len(events) != 1 || events[0].EventType != startup.EventType || events[0].ProducerID != startup.EventType || events[0].Source != nil || events[0].SchemaVersion != 1 {
					return errors.New("invalid startup snapshot")
				}
				persisted, err := store.Get(call, events[0].ID)
				if err != nil {
					return err
				}
				if persisted.Content != events[0].Content {
					return errors.New("startup was not durably stored")
				}
				select {
				case checked <- persisted.ID:
				default:
				}
				return nil
			}, func(child context.Context) error {
				return startup.Run(child, producer)
			}, func(child context.Context) error {
				select {
				case eventID = <-checked:
				case <-child.Done():
					return child.Err()
				}
				for child.Err() == nil {
					messages, err := builder.List(child, messagebuilder.MessageQuery{Status: messagebuilder.StatusSucceeded})
					if err != nil {
						return err
					}
					if len(messages) == start {
						return stop
					}
					time.Sleep(time.Millisecond)
				}
				return child.Err()
			})
			if !errors.Is(err, stop) {
				t.Fatal("startup failed to reach downstream", err)
			}
			if eventID == "" || eventID == previousID {
				t.Fatal("restart did not produce a distinct startup event")
			}
			previousID = eventID
			events, err := store.List(ctx, event.EventQuery{ProducerID: startup.EventType, EventType: startup.EventType})
			if err != nil || len(events) != start {
				t.Fatal("expected exactly one durable event per start", len(events), err)
			}
		}()
	}
}
