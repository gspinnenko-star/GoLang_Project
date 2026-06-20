package main

import (
	"context"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
	"github.com/redis/go-redis/v9"
)

var ctx = context.Background()

func main() {
	log.Println("=== Старт микросервиса на Go ===")

	// 1. Настройки портов и баз данных
	port := os.Getenv("PORT")
	if port == "" {
		port = "8080"
	}

	redisAddr := os.Getenv("REDIS_URL")
	if redisAddr == "" {
		redisAddr = "127.0.0.1:6379" 
	}

	rabbitmqURL := os.Getenv("RABBITMQ_URL")
	if rabbitmqURL == "" {
		rabbitmqURL = "amqp://guest:guest@127.0.0.1:5672/" 
	}

	// 2. Подключение к Redis
	rdb := redis.NewClient(&redis.Options{Addr: redisAddr})
	if _, err := rdb.Ping(ctx).Result(); err != nil {
		log.Printf("[Предупреждение] Redis недоступен: %v. Работаем без кэша.", err)
	} else {
		log.Println("[+] Успешное подключение к Redis!")
	}

	// 3. Подключение к RabbitMQ
	conn, err := amqp.Dial(rabbitmqURL)
	var msgs <-chan amqp.Delivery
	if err != nil {
		log.Printf("[Предупреждение] RabbitMQ недоступен: %v. Очередь не слушается.", err)
	} else {
		defer conn.Close()
		ch, err := conn.Channel()
		if err == nil {
			defer ch.Close()
			q, err := ch.QueueDeclare("tasks", true, false, false, false, nil)
			if err == nil {
				msgs, err = ch.Consume(q.Name, "", true, false, false, false, nil)
				if err == nil {
					log.Println("[+] Успешное подключение к RabbitMQ! Очередь 'tasks' активна.")
				}
			}
		}
	}

	// 4. Фоновая обработка сообщений (если RabbitMQ подключен)
	if msgs != nil {
		go func() {
			for d := range msgs {
				rawText := string(d.Body)
				log.Printf("[Новое событие] Извлечено: %s", rawText)
				cacheKey := "msg:" + time.Now().Format("15:04:05.000")
				rdb.Set(ctx, cacheKey, rawText, 5*time.Minute)
			}
		}()
	}

	// 5. ДОБАВЛЕНО: Веб-сервер для проверки жизнедеятельности Render (Health Check)
	http.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprintf(w, "Микросервер Go работает! Время сервера: %s", time.Now().Format("15:04:05"))
	})

	server := &http.Server{Addr: ":" + port}
	go func() {
		log.Printf("[*] Веб-сервер запущен на порту %s", port)
		if err := server.ListenAndServe(); err != http.ErrServerClosed {
			log.Fatalf("[-] Ошибка веб-сервера: %v", err)
		}
	}()

	// 6. Ожидание сигнала остановки
	stopSignal := make(chan os.Signal, 1)
	signal.Notify(stopSignal, os.Interrupt, syscall.SIGTERM)
	<-stopSignal

	log.Println("=== Микросервис аккуратно завершил работу ===")
}
