package com.xbank.bigdata.efthavale.producer;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.io.FileNotFoundException;
import java.util.Properties;

public class Application {

    public static void main(String[] args) throws FileNotFoundException, InterruptedException {

        // Kafka ayarları yapılır.
        Properties properties=new Properties();
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,"206.189.117.218:9092");
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, new StringSerializer().getClass().getName());
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,new StringSerializer().getClass().getName());


        // Mevcut ayarlarla yeni bir kafka producer oluşturur.
        Producer producer=new KafkaProducer<String, String>(properties);

        DataGenerator dg=new DataGenerator();

        while(true)
        {
            Thread.sleep(500); //Bekleme süresi
            String data = dg.generate();
            ProducerRecord<String, String> rec=new ProducerRecord<String, String>("efthavale", data);
            producer.send(rec);
        }

    }

}
