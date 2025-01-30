package org.acme;

import org.apache.camel.builder.RouteBuilder;
import org.apache.camel.builder.endpoint.StaticEndpointBuilders;
import org.eclipse.microprofile.config.ConfigProvider;

import static org.apache.camel.builder.endpoint.StaticEndpointBuilders.sjms2;


public class Routes extends RouteBuilder {

    @Override
    public void configure() throws Exception {
        
        String queue = ConfigProvider.getConfig().getConfigValue("myapp.queue").getValue();
        String topic = ConfigProvider.getConfig().getConfigValue("myapp.topic").getValue();

//        String scenario = "jms"; // send 100000; dev service avg=23000; LO=25000
//        String scenario = "jms-to-kafka"; // send 100000; dev service avg=3200; LO=170
//        String scenario = "jms-to-kafka-tx"; // send 10000; dev service avg=97; LO=110
//        String scenario = "kafka"; // send 1000000; dev service avg=38000; LO=25000
//        String scenario = "kafka-to-jms"; // send 1000; dev service avg=120; LO=125
//        String scenario = "kafka-to-jms-manual-commit"; // send 1000; dev service avg=95; LO=30
//        String scenario = "sjms-to-kafka-with-ack";
//        String scenario = "noop";
        String scenario = "jms-ra";

        String sc = ConfigProvider.getConfig().getOptionalValue("scenario", String.class).orElse(scenario);
        log.info("running scenario " + sc);

        if (sc.equals("jms")) {

            // curl -X POST localhost:18080/hello/send-jms?count=1000
            from("jms:queue:"+queue+"?concurrentConsumers=10")
                    .bean("my-bean", "fromJMS");

        } else if (sc.equals("jms-ra")) {

            // curl -X POST localhost:18080/hello/send-jms?count=1000
            from(sjms2("queue:"+queue+"?concurrentConsumers=10").connectionFactory("#CFRA"))
                    .log("received ${body}");

        } else if (sc.equals("jms-to-kafka")) {

            // curl -X POST localhost:18080/hello/send-jms?count=1000
            from("jms:queue:"+queue+"?concurrentConsumers=10")
                    .bean("my-bean", "fromJMS")
                    .to("kafka:"+topic);

            from("kafka:"+topic)
                    .bean("my-bean", "fromKafka");

        } else if (sc.equals("jms-to-kafka-tx")) {

            // curl -X POST localhost:18080/hello/send-jms?count=1000
            from("jms:queue:"+queue+"?concurrentConsumers=10&transacted=true")
                    .bean("my-bean", "fromJMS")
                    .to("kafka:"+topic);

            from("kafka:"+topic)
                    .bean("my-bean", "fromKafka");

        } else if (sc.equals("sjms-to-kafka-tx")) {

            // curl -X POST localhost:18080/hello/send-jms?count=1000
            from("sjms2:queue:"+queue+"?concurrentConsumers=10&transacted=true")
                    .bean("my-bean", "fromJMS")
                    .to("kafka:"+topic);

//            from("kafka:"+topic)
//                    .bean("my-bean", "fromKafka");

        } else if (sc.equals("kafka-to-jms")) {

            // curl -X POST localhost:18080/hello/send-kafka?count=1000
            from("kafka:"+topic)
                    .bean("my-bean", "fromKafka")
                    .to("jms:queue:"+queue);

            from("jms:queue:"+queue+"?concurrentConsumers=10")
                    .bean("my-bean", "fromJMS");

        } else if (sc.equals("kafka-to-jms-manual-commit")) {

            // curl -X POST localhost:18080/hello/send-kafka?count=1000
            from("kafka:"+topic+"?allowManualCommit=true&autoCommitEnable=false")
                    .bean("my-bean", "fromKafka")
                    .to("jms:queue:"+queue)
                    .bean("my-bean", "commitKafka");

            from("jms:queue:"+queue+"?concurrentConsumers=10")
                    .bean("my-bean", "fromJMS");

        } else if (sc.equals("kafka")) {

            // curl -X POST localhost:18080/hello/send-kafka?count=1000
            from("kafka:"+topic)
                    .bean("my-bean", "fromKafka");

        } else if (sc.equals("sjms-to-kafka-with-ack")) {

            from("sjms2:queue:"+queue+"?concurrentConsumers=10&acknowledgementMode=CLIENT_ACKNOWLEDGE&asyncConsumer=true")
                    .bean("my-bean", "fromJMS")
                    .to("kafka:"+topic);
            // .bean("my-bean", "fail");

        } else {
            log.info("no scenario " + sc);
        }
    }
}