// Poll the Twitter FilteredStream API  using a goroutine and feed the result into Kafka

package main

import (
    "os"
    "fmt"
    "bufio"
    "strings"
    "net/http"
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
        return
    }

    bearer_key := "Bearer " + app_bearer_key

    go tweets_worker(tweets_chan, bearer_key)

    for tweet := range tweets_chan {
        fmt.Println(tweet)
    }

}
