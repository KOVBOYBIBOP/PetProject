package producer

import (
	"encoding/json"
	"github.com/IBM/sarama"
	"log"
	"math/rand"
	"time"
)

type UserAction struct {
	UserID    string `json:"user_id"`
	Timestamp int64  `json:"timestamp"`
	Action    string `json:"action_type"`
	PageURL   string `json:"page_url"`
}

const topic = "user-actions"

func StartProducer() error {
	config := sarama.NewConfig()
	config.Producer.Return.Successes = true
	producer, err := sarama.NewSyncProducer([]string{"kafka:9092"}, config)
	if err != nil {
		return err
	}
	defer producer.Close()

	for {
		action := UserAction{
			UserID:    randomUserID(),
			Timestamp: time.Now().Unix(),
			Action:    randomActionType(),
			PageURL:   randomPageURL(),
		}

		message, _ := json.Marshal(action)
		_, _, err = producer.SendMessage(&sarama.ProducerMessage{
			Topic: topic,
			Value: sarama.StringEncoder(message),
		})
		if err != nil {
			log.Printf("Failed to send message: %v", err)
		} else {
			log.Printf("Message sent: %s", message)
		}

		time.Sleep(1 * time.Second)
	}
}

func randomUserID() string {
	return string('A' + rune(rand.Intn(26)))
}

func randomActionType() string {
	actions := []string{"page_view", "button_click"}
	return actions[rand.Intn(len(actions))]
}

func randomPageURL() string {
	urls := []string{"https://example.com/home", "https://example.com/contact"}
	return urls[rand.Intn(len(urls))]
}
