package com.example.reactorkafkatest;

import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.ApplicationRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;
import org.springframework.kafka.config.TopicBuilder;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;
import reactor.kafka.receiver.KafkaReceiver;
import reactor.kafka.receiver.ReceiverOptions;
import reactor.kafka.receiver.ReceiverRecord;
import reactor.kafka.sender.KafkaSender;
import reactor.kafka.sender.SenderOptions;
import reactor.kafka.sender.SenderRecord;
import reactor.util.retry.Retry;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.function.Function;

@SpringBootApplication
public class ReactorKafkaTestApplication {

    private static final Logger log = LoggerFactory.getLogger(ReactorKafkaTestApplication.class);

    private final CountDownLatch latch = new CountDownLatch(1);

    private static final String BOOTSTRAP_SERVERS = "localhost:9092";
    private static final String TEST_KAFKA_TOPIC = "test.topic";
    private static final String TEST_CONSUMER_GROUP = "test.group";

    private static final Duration EVENT_PUBLISHING_INTERVAL = Duration.ofMillis(10);
    private static final Duration EVENT_HANDLING_TIME = Duration.ofSeconds(1);
    private static final Duration EVENT_HANDLING_TIME_FOR_RETRY_CASE = Duration.ofMillis(10);
    private static final Duration INITIAL_RETRY_DELAY = Duration.ofSeconds(1);
    private static final int MAX_RETRY_ATTEMPTS = 3;

    private static Random RANDOM = new Random();

    public static void main(String[] args) {
        SpringApplication.run(ReactorKafkaTestApplication.class, args);
    }

    @Bean
    public NewTopic testKafkaTopic() {
        return TopicBuilder.name(TEST_KAFKA_TOPIC)
            .partitions(2)
            .replicas(1)
            .build();
    }

    @Bean
    public ApplicationRunner kafkaSenderAppRunner() {
        return args -> {
            SenderOptions<String, String> senderOptions = SenderOptions.create(
                Map.of(
                    ProducerConfig.CLIENT_ID_CONFIG, "test-producer",
                    ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS,
                    ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class,
                    ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class
                )
            );
            KafkaSender<String, String> sender = KafkaSender.create(senderOptions);
            Flux<SenderRecord<String, String, String>> eventsFux = Flux.interval(EVENT_PUBLISHING_INTERVAL)
                .map(this::createSenderRecord);
            sender.send(eventsFux)
                .doOnNext(res -> log.trace("Sending result: {}", res.recordMetadata()))
                .subscribe();
        };
    }

    @Bean
    public ApplicationRunner kafkaReceiversRunner() {
        Function<ReceiverRecord<String, String>, Mono<ReceiverRecord<String, String>>> handlingFunction =
//            Not reproducible case
//            this::handleWithThreadSleep;
//            Reproducible cases
//            this::handleWithMonoDelay;
            this::handleWithMonoRetry;

        return args -> {
            log.info("*************************************************************************************");
            log.info("STARTING RECEIVER 1");
            log.info("*************************************************************************************");
            startReceiver(handlingFunction);
            latch.await(); // wait till receiver1 will join to the group
            Thread.sleep(10_000);// wait a bit to handle some events on receiver1
            log.info("*************************************************************************************");
            log.info("STARTING RECEIVER 2");
            log.info("*************************************************************************************");
            startReceiver(handlingFunction);// start receiver2 and observe debug log on rebalance:
            // "Rebalancing; waiting for N records in pipeline"
        };
    }

    private void startReceiver(Function<ReceiverRecord<String, String>, Mono<ReceiverRecord<String, String>>> handlingFunction) {
        var receiverOptions = ReceiverOptions.<String, String>create(
                Map.of(
                    ConsumerConfig.CLIENT_ID_CONFIG, "test-consumer-" + RANDOM.nextInt(),
                    ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS,
                    ConsumerConfig.GROUP_ID_CONFIG, TEST_CONSUMER_GROUP,
                    ConsumerConfig.MAX_POLL_RECORDS_CONFIG, 3,
                    ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest"
                )
            )
            .schedulerSupplier(Schedulers::boundedElastic)
            .withKeyDeserializer(new StringDeserializer())
            .withValueDeserializer(new StringDeserializer())
            .addAssignListener(assignments -> {
                log.info("Assigned: {}", assignments);
                this.latch.countDown();
            })
            .subscription(List.of(TEST_KAFKA_TOPIC));

        KafkaReceiver.create(receiverOptions)
            .receive()
            .groupBy(rec -> rec.partition())
            .flatMap(group -> group
                .concatMap(handlingFunction)
            )
            .subscribe();
    }

    private Mono<ReceiverRecord<String, String>> handleWithThreadSleep(ReceiverRecord<String, String> record) {
        return Mono.just(record)
            .doOnNext(rec -> {
                try {
                    log.info("Started handling of event#{}", rec.key());
                    Thread.sleep(EVENT_HANDLING_TIME.toMillis());
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                } finally {
                    rec.receiverOffset().acknowledge();
                    log.info("Finished handling of event#{}", rec.key());
                }
            });
    }

    private Mono<ReceiverRecord<String, String>> handleWithMonoDelay(ReceiverRecord<String, String> record) {
        log.info("Started handling of event#{}", record.key());
        return Mono.delay(EVENT_HANDLING_TIME)
            .map(x -> record)
            .doOnNext(rec -> rec.receiverOffset().acknowledge())
            .doOnNext(rec -> log.info("Finished handling of event#{}", rec.key()));
    }

    private Mono<ReceiverRecord<String, String>> handleWithMonoRetry(ReceiverRecord<String, String> record) {
        log.info("Started handling of event#{}", record.key());
        return Mono.just(record)
            .flatMap(rec -> {
                try {
                    Thread.sleep(EVENT_HANDLING_TIME_FOR_RETRY_CASE.toMillis());
                } finally {
                    return Mono.<ReceiverRecord<String, String>>error(new RuntimeException());
                }
            })
            .retryWhen(
                Retry.backoff(MAX_RETRY_ATTEMPTS, INITIAL_RETRY_DELAY)
                    .doBeforeRetry(retrySignal ->
                        log.info("Handling retry {} for event#{}", retrySignal, record.key()))
            )
            .onErrorResume(ex -> {
                log.error("Failed to handle event#{}, publishing it to dead letter topic....", record.key());
                return Mono.just(record);
            })
            .doOnNext(rec -> rec.receiverOffset().acknowledge())
            .doOnNext(rec -> log.info("Finished handling of event#{}", rec.key()));
    }

    private SenderRecord<String, String, String> createSenderRecord(long eventNumber) {
        var key = String.valueOf(eventNumber);
        var value = "Event: " + eventNumber;
        ProducerRecord<String, String> producerrecord = new ProducerRecord<>(TEST_KAFKA_TOPIC, key, value);
        return SenderRecord.create(producerrecord, key);
    }

}
