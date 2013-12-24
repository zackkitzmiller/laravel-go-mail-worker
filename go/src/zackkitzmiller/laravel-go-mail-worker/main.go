package main

import (
	"encoding/json"
	"fmt"
	"github.com/iwanbk/gobeanstalk"
	"github.com/sendgrid/sendgrid-go"
	"log"
	"os"
)

/*
 * Define a structure here to hold the JSON data that
 * we get from beanstalk. We need to define all of the
 * fields we're expecting to be able to access
 */
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

/*
 * The programs entry point.
 */
func main() {

	// Define a channel for the goroutines to communicate
	messageChannel := make(chan Message)

	// start a single thread listening to beanstalk
	go consumeFromBeanstalk(messageChannel)

	// start a single thread waiting to send emails
	go sendMail(messageChannel)

	// wait for user input
	log.Printf("Running: Press Enter to Exit.")
	var userInput string
	fmt.Scanln(&userInput)
}

/*
 * function to wait for beanstalk
 * takes an one directional channel of type Message
 */
func consumeFromBeanstalk(c chan<- Message) {
	// create a beanstalkd connection
	beanstalkdConn := os.Getenv("BEANSTALKD")

	conn, err := gobeanstalk.Dial(beanstalkdConn)
	if err != nil {
		log.Printf("connect failed")
		log.Fatal(err)
	}

	log.Printf("Connected to Beanstalk")

	// set the connection to only watch the 'emails' channel
	cnt, err := conn.Watch("emails")
	if err != nil {
		log.Println("Unable to watch email queue")
		log.Fatal(err)
	}

	log.Println("Current Tube Count: %d", cnt)

	// infinitely loop
	// since this is in it's own thead, it doesn't block
	// main program excution
	for {
		j, err := conn.Reserve()
		if err != nil {
			log.Println("reserve failed")
			log.Fatal(err)
		}
		log.Printf("id:%d, body:%s\n", j.Id, string(j.Body))

		// create a new Message and load in the json
		// that we get from beanstalk
		var m Message
		jerr := json.Unmarshal(j.Body, &m)

		if jerr != nil {
			log.Println("Bad Json")
			log.Fatal(jerr)
		}

		// Send the Message to the channel
		c <- m

		// delete the job from the tube
		err = conn.Delete(j.Id)
		if err != nil {
			log.Fatal(err)
		}
	}
}

/*
 * function listening for messages to send
 * takes a one directional channel of type Message
 */
func sendMail(c <-chan Message) {

	// create sendgrid client
	sendgridUser := os.Getenv("SENDGRID_USER")
	sendgridPass := os.Getenv("SENDGRID_PASS")
	sg := sendgrid.NewSendGridClient(sendgridUser, sendgridPass)

	// infinite loop
	// see not above. Will not block program execution
	for {
		// Block until Message is received from channel
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
