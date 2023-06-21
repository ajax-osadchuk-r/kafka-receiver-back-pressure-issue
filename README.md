# MINIMAL REPRODUCIBLE EXAMPLE FOR https://github.com/reactor/reactor-kafka/issues/345

## Prepare local env

Open terminal in root project dir and execute: `cd ./env && docker-compose up -d`.

This step is required to deploy Kafka local env.
You can skip it, if you already have custom Kafka deployed, but make sure to update connection props if needed.

## Steps to reproduce

1. Open [ReactorKafkaTestApplication](src/main/java/com/example/reactorkafkatest/ReactorKafkaTestApplication.java) class
2. In kafkaReceiversRunner choose one of reproducible Kafka event handling methods
3. Start SpringBoot application
4. Wait a bit till second receiver will be started
5. Verify in logs DEBUG message "Rebalancing; waiting for N records in pipeline", where N is much more grater than
   `max.poll.records` value. In my case it was ~260.
6. Verify that some events were processed on both consumers after rebalance, 
   you can check it searching by event key (it is unique for each event in this app) in next log messages:
   ```   
   "RECEIVER_1:: Started handling of event#73"
   "RECEIVER_2:: Started handling of event#73"
   ```
