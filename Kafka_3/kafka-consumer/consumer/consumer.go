package consumer

import (
	"encoding/json"
	"fmt"
	"github.com/IBM/sarama"
	"log"
	"sync"
	"time"
)

type UserAction struct {
	UserID    string `json:"user_id"`
	Timestamp int64  `json:"timestamp"`
	Action    string `json:"action_type"`
	PageURL   string `json:"page_url"`
}

const topic = "user-actions"

func StartConsumer() error {
	config := sarama.NewConfig()
	config.Consumer.Group.Rebalance.Strategy = sarama.BalanceStrategyRoundRobin

	log.Printf("Starting Kafka Consumer on brokers: %v", []string{"kafka:9092"})
	consumer, err := sarama.NewConsumerGroup([]string{"kafka:9092"}, "user-group", config)
	if err != nil {
		return fmt.Errorf("failed to create consumer group: %v", err)
	}
	if consumer == nil {
		log.Fatal("Consumer is nil")
		return fmt.Errorf("consumer is nil")
	}
	defer consumer.Close()

	log.Println("Consumer group created successfully")

	var wg sync.WaitGroup
	wg.Add(1)

	go func() {
		defer wg.Done()
		for {
			log.Println("Starting to consume messages")
			err := consumer.Consume(nil, []string{topic}, &handler{})
			if err != nil {
				log.Printf("Consumer error: %v", err)
				return
			}
		}
	}()

	wg.Wait()
	return nil
}

type handler struct {
	mu sync.Mutex
}

func (h *handler) Setup(_ sarama.ConsumerGroupSession) error {
	log.Println("Initializing consumer group setup")
	return nil
}

func (h *handler) Cleanup(_ sarama.ConsumerGroupSession) error {
	log.Println("Cleaning up consumer group")
	return nil
}

func (h *handler) ConsumeClaim(session sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	if session == nil {
		log.Println("Session is nil in ConsumeClaim")
		return fmt.Errorf("session is nil")
	}

	log.Println("Processing messages in ConsumeClaim")

	userActions := sync.Map{}

	ticker := time.NewTicker(10 * time.Second)
	defer ticker.Stop()

	for message := range claim.Messages() {
		var action UserAction
		err := json.Unmarshal(message.Value, &action)
		if err != nil {
			log.Printf("Failed to unmarshal message: %v", err)
			continue
		}

		userActions.Store(action.UserID, appendAction(userActions, action.UserID, action))

		session.MarkMessage(message, "")
	}

	for range ticker.C {
		h.mu.Lock()
		userActions.Range(func(key, value interface{}) bool {
			userID := key.(string)
			actions := value.([]UserAction)

			pageViews, buttonClicks := countActions(actions)

			log.Printf("User ID: %s, Page Views: %d, Button Clicks: %d", userID, pageViews, buttonClicks)

			userActions.Delete(key)
			return true
		})
		h.mu.Unlock()
	}

	return nil
}

func appendAction(userActions sync.Map, userID string, action UserAction) []UserAction {
	var actions []UserAction
	if value, ok := userActions.Load(userID); ok {
		actions = value.([]UserAction)
	}

	actions = append(actions, action)

	return actions
}

func countActions(actions []UserAction) (int, int) {
	pageViews := 0
	buttonClicks := 0

	for _, action := range actions {
		if action.Action == "page_view" {
			pageViews++
		} else if action.Action == "button_click" {
			buttonClicks++
		}
	}

	return pageViews, buttonClicks
}
