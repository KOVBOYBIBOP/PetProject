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

// StartConsumer запускает Kafka consumer
func StartConsumer() error {
	// Конфигурация consumer
	config := sarama.NewConfig()
	config.Consumer.Group.Rebalance.Strategy = sarama.BalanceStrategyRoundRobin

	// Логирование при запуске consumer
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

	// Используем sync.WaitGroup для ожидания завершения работы горутины
	var wg sync.WaitGroup
	wg.Add(1)

	// Горутин для потребления сообщений
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

	// Ожидаем завершения горутины
	wg.Wait()
	return nil
}

type handler struct {
	mu sync.Mutex // Используем Mutex для синхронизации доступа к userActions
}

// Setup вызывается при инициализации группы
func (h *handler) Setup(_ sarama.ConsumerGroupSession) error {
	// Логируем при инициализации
	log.Println("Initializing consumer group setup")
	return nil
}

// Cleanup вызывается при завершении работы группы
func (h *handler) Cleanup(_ sarama.ConsumerGroupSession) error {
	// Логируем завершение работы
	log.Println("Cleaning up consumer group")
	return nil
}

// ConsumeClaim обрабатывает каждое сообщение в группе
func (h *handler) ConsumeClaim(session sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	// Проверяем, что session не nil
	if session == nil {
		log.Println("Session is nil in ConsumeClaim")
		return fmt.Errorf("session is nil")
	}

	log.Println("Processing messages in ConsumeClaim")

	// Карта для хранения действий пользователей
	userActions := sync.Map{}

	// Тикер для проверки каждые 10 секунд
	ticker := time.NewTicker(10 * time.Second)
	defer ticker.Stop()

	// Обрабатываем каждое сообщение в claim
	for message := range claim.Messages() {
		var action UserAction
		// Преобразуем сообщение в структуру UserAction
		err := json.Unmarshal(message.Value, &action)
		if err != nil {
			log.Printf("Failed to unmarshal message: %v", err)
			continue
		}

		// Добавляем действие пользователя
		userActions.Store(action.UserID, appendAction(userActions, action.UserID, action))

		// Отмечаем, что сообщение обработано
		session.MarkMessage(message, "")
	}

	// Запуск цикла обработки каждого тикера
	for range ticker.C {
		// Печатаем действия пользователей каждые 10 секунд
		h.mu.Lock()
		userActions.Range(func(key, value interface{}) bool {
			userID := key.(string)
			actions := value.([]UserAction)

			pageViews, buttonClicks := countActions(actions)

			// Печатаем результаты
			log.Printf("User ID: %s, Page Views: %d, Button Clicks: %d", userID, pageViews, buttonClicks)

			// Очищаем старые данные после обработки
			userActions.Delete(key)
			return true
		})
		h.mu.Unlock()
	}

	return nil
}

// appendAction добавляет новое действие для пользователя
func appendAction(userActions sync.Map, userID string, action UserAction) []UserAction {
	var actions []UserAction

	// Извлекаем предыдущие действия, если они есть
	if value, ok := userActions.Load(userID); ok {
		actions = value.([]UserAction)
	}

	// Добавляем новое действие
	actions = append(actions, action)

	return actions
}

// countActions подсчитывает количество page_views и button_clicks для пользователя
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
