package com.mapr.examples;

import com.google.common.io.Resources;
import com.mapr.examples.models.Test;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.commons.lang3.RandomUtils;
import org.apache.kafka.common.serialization.StringSerializer;

import java.time.Instant;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Properties;

/**
 * This producer will send a bunch of messages to topic "fast-messages". Every so often,
 * it will send a message to "slow-messages". This shows how messages can be sent to
 * multiple topics. On the receiving end, we will see both kinds of messages but will
 * also see how the two topics aren't really synchronized.
 */
public class Producer {
    public static String generateRandomMetricId() {
        String firstPart = RandomStringUtils.randomAlphanumeric(2);
        String secondPart = RandomStringUtils.randomAlphanumeric(2);
        String thirdPart = RandomStringUtils.randomAlphanumeric(1);
        String fourthPart = RandomStringUtils.randomAlphanumeric(6);
        String metricId = firstPart + "-" + secondPart + "-" + thirdPart + "-" + fourthPart;
        return metricId;
    }

    public static void main(String[] args) throws IOException {
        // set up the producer
        KafkaProducer<String, String> producer;
        Properties properties = new Properties();
        Test metricValues= new Test();

        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, JsonBinarySerializer.class);

        try (InputStream props = Resources.getResource("producer.props").openStream()) {
            properties.load(props);
            producer = new KafkaProducer<>(properties);
        }

        Object test[];
        test = new Object[]{"xc-BE-D-wIoyOo",555,10,100,70,150,15};
        ArrayList blabla = new ArrayList();
        blabla.add(test);
        metricValues.setValues(blabla);

        try {
//            i - number of messages sent to kafka topic
            for (int i = 0; i < 10; i++) {

//                Arraylist collects generated metric messages in for loop
                ArrayList strings = new ArrayList();

//                a - number of metrics per message
                for (int a = 0; a < 10000; a++) {
                    String metricId = generateRandomMetricId();
                    int randomNumber = RandomUtils.nextInt(10, 101);
//                    If you want epoch time in milliseconds, delete "/ 1000L"
                    long epochTimeInSeconds = Instant.now().toEpochMilli() / 1000L;
                    String metricMessage = "[\"" + metricId + "\"," + epochTimeInSeconds + "10,100,70," + randomNumber + "," + "15]";
                    strings.add(metricMessage);
                }

                // send lots of messages
                producer.send(new ProducerRecord<String, String>(
                        properties.getProperty("mytopic.topic"),
//                        String.format("{\"type\":\""+metricId+"\", \"t\":%.3f, \"k\":%d}", System.nanoTime() * 1e-9, i)));
//                        String.format("{\"values\":[[\""+ metricId +"\"," + epochTime + "10,100,70," + randomNumber + "," + "15]]}")));
                        String.format("{\"values\":" + strings + "}")));

                // every so often send to a different topic
                if (i % 1000 == 0) {
                    producer.send(new ProducerRecord<String, String>(
                            properties.getProperty("summary-markers.topic"),
                            String.format("{\"type\":\"other\", \"t\":%.3f, \"k\":%d}", System.nanoTime() * 1e-9, i)));
                }
                producer.flush();
                System.out.println("Sent msg number " + i);
            }
        } catch (Throwable throwable) {
            System.out.printf("%s", throwable.getStackTrace());
        } finally {
            producer.close();
        }

    }
}
