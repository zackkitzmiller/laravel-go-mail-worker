package main

import (
	"encoding/json"
	"fmt"
	"github.com/iwanbk/gobeanstalk"
	"github.com/sendgrid/sendgrid-go"
	"log"
	"os"
)

type Message struct {
	Job  string
	Data struct {
		To      string
		ToName  string
		Subject string
		Body    string
		From    string
	}
}

func main() {

	messageChannel := make(chan Message)
	go consumeFromBeanstalk(messageChannel)
	go sendMail(messageChannel)

	log.Printf("Running: Press Enter to Exit.")
	var userInput string
	fmt.Scanln(&userInput)
}

func consumeFromBeanstalk(c chan Message) {
	beanstalkdConn := os.Getenv("BEANSTALKD")

	conn, err := gobeanstalk.Dial(beanstalkdConn)
	if err != nil {
		log.Printf("connect failed")
		log.Fatal(err)
	}

	log.Printf("Connected to Beanstalk")

	cnt, err := conn.Watch("emails")
	if err != nil {
		log.Println("Unable to watch email queue")
		log.Fatal(err)
	}

	log.Println("Current Tube Count: %d", cnt)

	for {
		j, err := conn.Reserve()
		if err != nil {
			log.Println("reserve failed")
			log.Fatal(err)
		}
		log.Printf("id:%d, body:%s\n", j.Id, string(j.Body))

		var m Message
		jerr := json.Unmarshal(j.Body, &m)

		if jerr != nil {
			log.Println("Bad Json")
			log.Fatal(jerr)
		}

		// Send the message to the channel
		c <- m

		err = conn.Delete(j.Id)
		if err != nil {
			log.Fatal(err)
		}
	}
}

func sendMail(c chan Message) {
	sendgridUser := os.Getenv("SENDGRID_USER")
	sendgridPass := os.Getenv("SENDGRID_PASS")
	sg := sendgrid.NewSendGridClient(sendgridUser, sendgridPass)

	for {
		// Block until message is received from channel
		m := <-c
		message := sendgrid.NewMail()
		message.AddTo(m.Data.To)
		message.AddToName(m.Data.ToName)
		message.AddSubject(m.Data.Subject)
		message.AddText(m.Data.Body)
		message.AddFrom(m.Data.From)
		if r := sg.Send(message); r == nil {
			log.Println("Email sent!")
		} else {
			log.Println(r)
		}
	}
}
