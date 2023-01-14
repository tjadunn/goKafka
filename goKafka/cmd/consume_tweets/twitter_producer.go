// Poll the Twitter FilteredStream API  using a goroutine and feed the result into Kafka

package main

import (
    "os"
    "fmt"
    "bufio"
    "strings"
    "net/http"
    "github.com/confluentinc/confluent-kafka-go/kafka"
)

// The below function long polls the API and awaits results
// The Tweets returned are dependent on the rules set linked to the developer token
// To view/set/delete rules see below
//
//
//
// List currently set rules
//
//  curl -X GET 'https://api.twitter.com/2/tweets/search/stream/rules' -H "Authorization: Bearer $APP_BEARER_KEY
//
// Set a rule
//
//  curl -X POST 'https://api.twitter.com/2/tweets/search/stream/rules' \
//  -H "Content-type: application/json" \
//  -H "Authorization: Bearer $APP_BEARER_KEY" -d \
//  '{
//      "add": [
//           {"value": "url_contains:discogs"}
//      ]
//   }'
//
//
//
//
// Delete rule(s)
//
//  curl -X POST 'https://api.twitter.com/2/tweets/search/stream/rules' \
//  -H "Content-type: application/json" \
//  -H "Authorization: Bearer $APP_BEARER_KEY" -d \
//  '{
//      "delete": {"ids": [123123,...]}
//   }'
//
// See https://developer.twitter.com/en/docs/twitter-api/tweets/filtered-stream/integrate/build-a-rule#build
// for more info on building rules

func tweets_worker(results chan <- string, bearer_key string) error{

    client := &http.Client{}

    req, err := http.NewRequest("GET", "https://api.twitter.com/2/tweets/search/stream", nil)

    if err != nil {
        return err
    }
    req.Header.Add("Content-type", "application/json")
    req.Header.Add("Authorization", bearer_key)

    resp, err := client.Do(req)
    defer resp.Body.Close()

    if err != nil {
        return err
    }

    // Long poll the endpoint
    bs := bufio.NewScanner(resp.Body)

    // Continuously iterate and await new tweets
    for bs.Scan() {
        line := strings.TrimSpace(bs.Text())
        results <- line
    }

    close(results)
    return nil
}


func main () {
    tweets_chan := make(chan string)

    // Get this from your Twitter devloper account
    app_bearer_key := os.Getenv("APP_BEARER_KEY")

    if app_bearer_key == "" {
        fmt.Println("Please provide a bearer key!")
        os.Exit(1)
    }

    bearer_key := "Bearer " + app_bearer_key

    p, err := kafka.NewProducer(&kafka.ConfigMap{
        "bootstrap.servers": "localhost:9092",
        "acks": "all",
    })

    defer p.Close()

    if err != nil {
        fmt.Printf("Failed to create producer: %s\n", err)
        os.Exit(1)
    }

    go tweets_worker(tweets_chan, bearer_key)

    topic := "musical_tweets"
    // Channel to receive delivery reports as we are writing async
    delivery_chan := make(chan kafka.Event, 10000)

    for tweet := range tweets_chan {
        fmt.Println(tweet)

        err = p.Produce(&kafka.Message{
            TopicPartition: kafka.TopicPartition{Topic: &topic, Partition: kafka.PartitionAny},
            Value: []byte(tweet)},
            delivery_chan,
        )

    }

}
