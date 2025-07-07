package main

import (
	"database/sql"
	"encoding/json"
	"errors"
	"log"
	"os"
	"time"

	_ "github.com/go-sql-driver/mysql"
	"github.com/joho/godotenv"
	amqp "github.com/rabbitmq/amqp091-go"
)

const (
	exchangeName = "chat.exchange"
	exchangeType  = "direct"
	queueName    = "chat.persist.queue"
	routingKey   = "chat.message"
)

type Metadata map[string]interface{}

type Message struct {
	MessageID   string     `json:"message_id"`
	RoomID      *string    `json:"room_id"`
	ThreadID    *string    `json:"thread_id"`
	SenderID    *string    `json:"sender_id"`
	ContentType string     `json:"content_type"`
	Content     string     `json:"content"`
	Metadata    *Metadata  `json:"metadata"`
	CreatedAt   time.Time  `json:"created_at"`
	UpdatedAt   *time.Time `json:"updated_at"`
	DeletedAt   *time.Time `json:"deleted_at"`
}

func failOnError(err error, msg string) {
    if err != nil {
        log.Fatalf("%s: %s", msg, err)
    }
}

func main() {
	if err := godotenv.Load(); err != nil {
        log.Println("No .env file found")
    }

	amqpURL := os.Getenv("RABBITMQ_URL")
	if amqpURL == "" {
        log.Fatal("RABBITMQ_URL is required")
    }
    dbURL := os.Getenv("DATABASE_URL")
    if dbURL == "" {
        log.Fatal("DATABASE_URL is required")
    }
	
	db, err := sql.Open("mysql", dbURL)
	failOnError(err, "failed to open database")
	defer db.Close()
	if err = db.Ping(); err != nil {
        failOnError(err, "failed to ping database")
    }

	conn, err := amqp.Dial(amqpURL)
	failOnError(err, "failed to connect to RabbitMQ")
    defer conn.Close()

	ch, err := conn.Channel()
    failOnError(err, "failed to open channel")
    defer ch.Close()

	err = ch.ExchangeDeclare(
        exchangeName, // name
        exchangeType, // kind
        true,         // durable
        false,        // auto-delete
        false,        // internal
        false,        // no-wait
        nil,          // args
    )
	failOnError(err, "failed to declare exchange")

	q, err := ch.QueueDeclare(
        queueName, // name
        true,      // durable
        false,     // delete when unused
        false,     // exclusive
        false,     // no-wait
        nil,       // args
    )
    failOnError(err, "failed to declare queue")

    err = ch.QueueBind(
        q.Name,       // queue name
        routingKey,   // routing key
        exchangeName, // exchange
        false,        // no-wait
        nil,          // args
    )
    failOnError(err, "failed to bind queue")

	queueInfo, err := ch.QueueDeclarePassive(
    queueName, // name
    true,      // durable
    false,     // delete when unused
    false,     // exclusive
    false,     // no-wait
    nil,       // args
	)
	if err != nil {
		log.Printf("Failed to inspect queue: %s", err)
	} else {
		log.Printf("Queue %s has %d messages", queueName, queueInfo.Messages)
	}


	msgs, err := ch.Consume(
		q.Name,
		"",
		false,
		false,
		false,
		false,
		nil,
	)
	failOnError(err, "failed to register consumer")

    forever := make(chan bool)

	go func() {
		for d := range msgs {
			log.Printf("Received message: %s", string(d.Body))

			var msg Message
			if err := json.Unmarshal(d.Body, &msg); err != nil {
				log.Printf("invalid JSON, nack: %s", err)
				d.Nack(false, false)
				continue
			}

			if err := storeMessage(db, &msg); err != nil {
                log.Printf("db insert failed, requeuing: %s", err)
                d.Nack(false, true) // requeue
                continue
            }
            log.Printf("Message stored successfully: %s", msg.MessageID)

            d.Ack(false)
		}
	}()
	log.Println(" [*] Waiting for messages. To exit press CTRL+C")
    <-forever
}

func storeMessage(db *sql.DB, message *Message) error {
	if message == nil {
		return errors.New("message cannot be nil")
	}

	// Validate that message has either RoomID or ThreadID
	if message.RoomID == nil && message.ThreadID == nil {
		return errors.New("message must have either room_id or thread_id")
	}

	// Validate required fields
	if message.Content == "" {
		return errors.New("message content cannot be empty")
	}

	if message.ContentType == "" {
		message.ContentType = "text" // Default to text if not specified
	}

	message.CreatedAt = time.Now().UTC()
	message.UpdatedAt = &[]time.Time{time.Now().UTC()}[0]

	// Create the message in database
	query := `
    INSERT INTO messages (message_id, room_id, thread_id, sender_id, content_type, content, metadata, created_at, updated_at, deleted_at)
    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`

	_, err := db.Exec(query, 
		message.MessageID,
		message.RoomID,
		message.ThreadID, 
		message.SenderID,
		message.ContentType,
		message.Content,
		message.Metadata,
		message.CreatedAt,
		message.UpdatedAt,
		message.DeletedAt,
	)

	return err
}