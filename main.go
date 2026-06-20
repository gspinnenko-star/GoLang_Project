package main

import (
	"context"
	"log"
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

	redisAddr := os.Getenv("REDIS_URL")
	if redisAddr == "" {
		redisAddr = "127.0.0.1:6379" 
	}

	rabbitmqURL := os.Getenv("RABBITMQ_URL")
	if rabbitmqURL == "" {
		rabbitmqURL = "amqp://guest:guest@127.0.0.1:5672/" 
	}

	rdb := redis.NewClient(&redis.Options{
		Addr: redisAddr,
	})
	if _, err := rdb.Ping(ctx).Result(); err != nil {
		log.Printf("[Предупреждение] Redis недоступен по адресу %s: %v. Работаем без кэша.", redisAddr, err)
	} else {
		log.Println("[+] Успешное подключение к Redis!")
	}

	conn, err := amqp.Dial(rabbitmqURL)
	if err != nil {
		log.Fatalf("[-] Критическая ошибка: нет связи с RabbitMQ (%v)", err)
	}
	defer conn.Close()

	ch, err := conn.Channel()
	if err != nil {
		log.Fatalf("[-] Не удалось открыть канал внутри TCP-сессии: %v", err)
	}
	defer ch.Close()

	q, err := ch.QueueDeclare("tasks", true, false, false, false, nil)
	if err != nil {
		log.Fatalf("[-] Не удалось объявить рабочую очередь: %v", err)
	}

	msgs, err := ch.Consume(q.Name, "", true, false, false, false, nil)
	if err != nil {
		log.Fatalf("[-] Ошибка регистрации обработчика очередей: %v", err)
	}

	go func() {
		for d := range msgs {
			rawText := string(d.Body)
			log.Printf("[Новое событие] Из очереди извлечено: %s", rawText)

			cacheKey := "msg:" + time.Now().Format("15:04:05.000")
			err := rdb.Set(ctx, cacheKey, rawText, 5*time.Minute).Err()
			if err != nil {
				log.Printf("[!] Ошибка кэширования в Redis: %v", err)
			} else {
				log.Printf("[Кэш] Данные продублированы в Redis. Ключ: %s (TTL: 5 мин)", cacheKey)
			}
		}
	}()

	log.Println("[*] Сервис успешно запущен и ожидает сообщений. Для остановки нажмите CTRL+C")

	stopSignal := make(chan os.Signal, 1)
	signal.Notify(stopSignal, os.Interrupt, syscall.SIGTERM)
	<-stopSignal

	log.Println("=== Микросервис аккуратно завершил работу ===")
}
