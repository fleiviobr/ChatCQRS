package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"sync"
	"time"

	"github.com/Shopify/sarama"
	"github.com/ThreeDotsLabs/watermill"
	"github.com/ThreeDotsLabs/watermill-kafka/v2/pkg/kafka"
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

	publisher, _ := kafka.NewPublisher(kafka.PublisherConfig{
		Brokers:               []string{"127.0.0.1:9092"},
		Marshaler:             kafka.DefaultMarshaler{},
		OverwriteSaramaConfig: saramaPubConfig,
	}, logger)

	subscriber, _ := kafka.NewSubscriber(kafka.SubscriberConfig{
		Brokers:               []string{"127.0.0.1:9092"},
		Unmarshaler:           kafka.DefaultMarshaler{},
		ConsumerGroup:         "chat_api",
		OverwriteSaramaConfig: saramaSubConfig,
	}, logger)

	router, err := message.NewRouter(message.RouterConfig{}, logger)
	if err != nil {
		log.Fatal("Erro ao criar o router: ", err)
	}

	cqrsFacade, _ := cqrs.NewFacade(cqrs.FacadeConfig{
		GenerateCommandsTopic: func(c string) string { return "commands" },
		CommandHandlers: func(cb *cqrs.CommandBus, eb *cqrs.EventBus) []cqrs.CommandHandler {
			return []cqrs.CommandHandler{SendMessageCommandHandler{EventBus: eb}}
		},
		CommandsPublisher:             publisher,
		CommandsSubscriberConstructor: func(handlerName string) (message.Subscriber, error) { return subscriber, nil },

		GenerateEventsTopic:         func(e string) string { return "events.messages" },
		EventsPublisher:             publisher,
		EventsSubscriberConstructor: func(handlerName string) (message.Subscriber, error) { return subscriber, nil },

		Router: router,
		Logger: logger,

		CommandEventMarshaler: cqrs.JSONMarshaler{},
	})

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

		if err := cqrsFacade.CommandBus().Send(context.Background(), &cmd); err != nil {
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
