package main 

import (
    "os"
    "fmt"
    "bufio"
    "strings"
    "net/http"
)

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

    for bs.Scan() {
        line := strings.TrimSpace(bs.Text())
        results <- line
    }

    close(results)
    return nil
}


func main () {
    tweets_chan := make(chan string)

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
