package main

import (
	"fmt"
	"html"
	"log"
	"net/http"

	"github.com/Shopify/sarama"
)

const topic = "SimpleProducerTopic"

func main() {
	producer, err := newProducer()
	if err != nil {
		fmt.Println("Could not create producer: ", err)
	}

	consumer, err := sarama.NewConsumer(brokers, nil)
	if err != nil {
		fmt.Println("Could not create consumer: ", err)
	}

	consumer2, err := sarama.NewConsumer(brokers, nil)
	if err != nil {
		fmt.Println("Could not create consumer: ", err)
	}

	subscribe(topic, consumer, producer)
	subscribeToMessages(topic, consumer2)

	http.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) { fmt.Fprint(w, "Hello Sarama!") })

	http.HandleFunc("/save", func(w http.ResponseWriter, r *http.Request) {

		switch r.Method {
		case "GET":
			// Serve the resource.
			fmt.Fprint(w, html.EscapeString(getMessage()))
			break
		case "POST":
			// Create a new record.
			defer r.Body.Close()
			r.ParseForm()
			msg := prepareMessage(topic, r.FormValue("q"))
			partition, offset, err := producer.SendMessage(msg)
			if err != nil {
				fmt.Fprintf(w, "%s error occured.", err.Error())
			} else {
				fmt.Fprintf(w, "Message was saved to partion: %d.\nMessage offset is: %d.\n", partition, offset)
			}
			break
		case "PUT":
			// Update an existing record.
		case "DELETE":
			// Remove the record.
		default:
			// Give an error message.
		}
	})

	http.HandleFunc("/retrieve", func(w http.ResponseWriter, r *http.Request) { fmt.Fprint(w, html.EscapeString(getMessage())) })

	http.HandleFunc("/consumeAndProduce", func(w http.ResponseWriter, r *http.Request) {

	})

	fmt.Println("Starting http server on 8081")
	log.Fatal(http.ListenAndServe(":8081", nil))
}
