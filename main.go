package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"sync"
	"time"

	"github.com/IBM/sarama"
	"github.com/ThreeDotsLabs/watermill"
	"github.com/ThreeDotsLabs/watermill-kafka/v3/pkg/kafka"
	"github.com/ThreeDotsLabs/watermill/components/cqrs"
	"github.com/ThreeDotsLabs/watermill/message"
	"github.com/gorilla/websocket"
)

type SendMessage struct {
	RoomID   string `json:"room_id"`
	Username string `json:"username"`
	Content  string `json:"content"`
}

type MessageSentEvent struct {
	MessageID   string      `json:"message_id"`
	MessageSent SendMessage `json:"message_sent"`
	Moment      time.Time   `json:"moment"`
}

var upgrader = websocket.Upgrader{
	CheckOrigin: func(r *http.Request) bool { return true },
}

type WSHub struct {
	mu      sync.RWMutex
	clients map[string]map[*websocket.Conn]bool
}

func NewWSHub() *WSHub {
	return &WSHub{clients: make(map[string]map[*websocket.Conn]bool)}
}

func (h *WSHub) Broadcast(event *MessageSentEvent) {
	h.mu.RLock()
	defer h.mu.RUnlock()

	roomClients := h.clients[event.MessageSent.RoomID]
	for client := range roomClients {
		err := client.WriteJSON(event)
		if err != nil {
			client.Close()
			delete(roomClients, client)
		}
	}
}

type SendMessageCommandHandler struct {
	EventBus *cqrs.EventBus
}

func (h SendMessageCommandHandler) HandlerName() string {
	return "SendMessageCommandHandler"
}

func (h SendMessageCommandHandler) NewCommand() interface{} {
	return &SendMessage{}
}

func (h SendMessageCommandHandler) Handle(ctx context.Context, c interface{}) error {
	cmd := c.(*SendMessage)

	fmt.Printf("[Escrita]Usuário: %s\n", cmd.Username)

	event := &MessageSentEvent{
		MessageID:   watermill.NewUUID(),
		MessageSent: *cmd,
		Moment:      time.Now(),
	}

	return h.EventBus.Publish(ctx, event)
}

func main() {
	logger := watermill.NewStdLogger(false, false)

	saramaPubConfig := kafka.DefaultSaramaSyncPublisherConfig()
	saramaPubConfig.Version = sarama.V3_0_0_0

	saramaSubConfig := kafka.DefaultSaramaSubscriberConfig()
	saramaSubConfig.Version = sarama.V3_0_0_0

	publisher, err := kafka.NewPublisher(kafka.PublisherConfig{
		Brokers:               []string{"127.0.0.1:9092"},
		Marshaler:             kafka.DefaultMarshaler{},
		OverwriteSaramaConfig: saramaPubConfig,
	}, logger)
	if err != nil {
		log.Fatal(err)
	}

	subscriber, err := kafka.NewSubscriber(kafka.SubscriberConfig{
		Brokers:               []string{"127.0.0.1:9092"},
		Unmarshaler:           kafka.DefaultMarshaler{},
		ConsumerGroup:         "chat_api",
		OverwriteSaramaConfig: saramaSubConfig,
	}, logger)
	if err != nil {
		log.Fatal(err)
	}

	router, err := message.NewRouter(message.RouterConfig{}, logger)
	if err != nil {
		log.Fatal(err)
	}

	commandBus, err := cqrs.NewCommandBusWithConfig(publisher, cqrs.CommandBusConfig{
		GeneratePublishTopic: func(params cqrs.CommandBusGeneratePublishTopicParams) (string, error) {
			return "commands", nil
		},
		Marshaler: cqrs.JSONMarshaler{},
	})
	if err != nil {
		log.Fatal(err)
	}

	eventBus, err := cqrs.NewEventBusWithConfig(publisher, cqrs.EventBusConfig{
		GeneratePublishTopic: func(params cqrs.GenerateEventPublishTopicParams) (string, error) {
			return "events.messages", nil
		},
		Marshaler: cqrs.JSONMarshaler{},
	})
	if err != nil {
		log.Fatal(err)
	}

	handler := SendMessageCommandHandler{
		EventBus: eventBus,
	}

	commandProcessor, err := cqrs.NewCommandProcessorWithConfig(router, cqrs.CommandProcessorConfig{
			GenerateSubscribeTopic: func(params cqrs.CommandProcessorGenerateSubscribeTopicParams) (string, error) {
				return "commands", nil
		},
			SubscriberConstructor: func(params cqrs.CommandProcessorSubscriberConstructorParams) (message.Subscriber, error) {
				return subscriber, nil
		},
		Marshaler: cqrs.JSONMarshaler{},
		Logger:    logger,
	})
	if err != nil {
		log.Fatal(err)
	}

	err = commandProcessor.AddHandlers(handler)
	if err != nil {
		log.Fatal(err)
	}

	wsHub := NewWSHub()

	router.AddNoPublisherHandler(
		"ws_broadcaster",
		"events.messages",
		subscriber,
		func(msg *message.Message) error {
			var event MessageSentEvent
			json.Unmarshal(msg.Payload, &event)

			fmt.Printf("[Leitura]Sala: %s\n", event.MessageSent.RoomID)
			wsHub.Broadcast(&event)
			return nil
		},
	)

	go func() { router.Run(context.Background()) }()
	<-router.Running()

	http.HandleFunc("/send", func(w http.ResponseWriter, r *http.Request) {

		w.Header().Set("Access-Control-Allow-Origin", "*")
		w.Header().Set("Access-Control-Allow-Methods", "POST, OPTIONS")
		w.Header().Set("Access-Control-Allow-Headers", "Content-Type")

		if r.Method == "OPTIONS" {
			w.WriteHeader(http.StatusOK)
			return
		}

		var cmd SendMessage
		if err := json.NewDecoder(r.Body).Decode(&cmd); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}

		if err := commandBus.Send(context.Background(), &cmd); err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}

		w.WriteHeader(http.StatusAccepted)
	})

	http.HandleFunc("/ws", func(w http.ResponseWriter, r *http.Request) {
		roomID := r.URL.Query().Get("room")
		conn, _ := upgrader.Upgrade(w, r, nil)

		wsHub.mu.Lock()
		if wsHub.clients[roomID] == nil {
			wsHub.clients[roomID] = make(map[*websocket.Conn]bool)
		}
		wsHub.clients[roomID][conn] = true
		wsHub.mu.Unlock()

		go func() {
			for {
				if _, _, err := conn.ReadMessage(); err != nil {
					wsHub.mu.Lock()
					delete(wsHub.clients[roomID], conn)
					wsHub.mu.Unlock()
					break
				}
			}
		}()
	})

	fmt.Println("Servidor rodando na porta 8080.")
	log.Fatal(http.ListenAndServe(":8080", nil))
}