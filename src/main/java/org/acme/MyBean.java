package org.acme;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.quarkus.scheduler.Scheduled;
import io.smallrye.common.annotation.Identifier;
import jakarta.enterprise.context.ApplicationScoped;
import org.apache.camel.Exchange;
import org.apache.camel.component.kafka.consumer.KafkaManualCommit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

@ApplicationScoped
@Identifier("my-bean")
public class MyBean {

    private static final Logger log = LoggerFactory.getLogger(MyBean.class);

    static AtomicInteger countJMS = new AtomicInteger();
    static AtomicInteger countKafka = new AtomicInteger();
    static AtomicInteger countTotalJMS = new AtomicInteger();
    static AtomicInteger countTotalKafka = new AtomicInteger();
    static AtomicLong started = new AtomicLong(0);

    ObjectMapper objectMapper = new ObjectMapper();

    static void init() {
        countJMS.set(0);
        countKafka.set(0);
        countTotalJMS.set(0);
        countTotalKafka.set(0);
        started.set(System.currentTimeMillis());
    }

    public Order fromJMS(String body) {
//        log.info("received jms " + body);
        countJMS.incrementAndGet();
//        if(body.equals("hello_5")) {
//            throw new RuntimeException("oops hello_5");
//        }
        return new Order(1, "X", body);
    }

    public void fail(Order order) {
        String ref = order.getReferenceValue().toString();
        log.error("Failed to process " + ref);
    }

    public String fromKafka(String body) throws JsonProcessingException {
        // log.info("received kafka " + body);
        countKafka.incrementAndGet();
        if(body.startsWith("{") && body.endsWith("}")) {
            String ref = objectMapper.readTree(body).findValue("reference_value").asText();
            return ref;
        } else {
            return body;
        }
    }

    public List<String> fromKafkaMulti(List<Exchange> exchanges) {
        // build jms bodies out of the multiple kafka messages
        return exchanges.stream().map(ex -> ((Order)  ex.getIn().getBody()).getReferenceValue().toString()).toList();
    }

    @Scheduled(every = "1s")
    public void logLastJms() {
        int value = countJMS.getAndSet(0);
        countTotalJMS.addAndGet(value);
        if (value != 0) {
            int rate = (int) (value * 1.0);
            long delta = System.currentTimeMillis() - started.get();
            int totalRate = (int) (countTotalJMS.get() * 1000.0 / delta);
            String s = "Received from Jms: " + value + " in last 1 sec (" + rate + " messages/s); total rate = "+totalRate+" messages/s";
            log.info(s);
        }
    }

    @Scheduled(every = "1s")
    public void logLastKafka() {
        int value = countKafka.getAndSet(0);
        countTotalKafka.addAndGet(value);
        if (value != 0) {
            int rate = (int) (value * 1.0);
            long delta = System.currentTimeMillis() - started.get();
            int totalRate = (int) (countTotalKafka.get() * 1000.0 / delta);
            String s = "Received from Kafka: " + value + " in last 1 sec (" + rate + " messages/s); total rate = "+totalRate+" messages/s";
            log.info(s);
        }
    }
}