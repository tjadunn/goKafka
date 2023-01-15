## Go Twitter Consumer

A lightweight API poller written in Go

The purpose of this service is to poll the twitter API and fetch tweets based on pre defined rules. These are then produced to a Kafka topic for downstream consumption


`Twitter API -> goKafka -> Kafka`
