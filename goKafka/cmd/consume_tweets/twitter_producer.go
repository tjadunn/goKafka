package main 

import (
    "fmt"
    "bufio"
    "strings"
    "net/http"
)

func tweets_worker(results chan <- string) error{

    client := &http.Client{}
    bearer_key := "Bearer AAAAAAAAAAAAAAAAAAAAADTVkgEAAAAATsLsSVeP45Qa9JBm2Yj0SjN0QOY%3DhIl0nTSbx8M6K0EX3Mh3mf1fBvgaD3zzBws4SkXPolVjC0MpSm"

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

    go tweets_worker(tweets_chan)

    for tweet := range tweets_chan {
        fmt.Println(tweet)
    }

}
