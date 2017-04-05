package com.nihed;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.spark.*;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.*;
import org.apache.spark.streaming.*;
import org.apache.spark.streaming.api.java.*;
import org.apache.spark.streaming.kafka010.*;


import java.util.Arrays;
import java.util.Collection;
import java.util.Map;
import java.util.HashMap;

public class App 
{
    public static void main( String[] args ) throws InterruptedException {
        SparkConf conf = new SparkConf().setMaster("local[6]").setAppName("sampleOffsetTest");
        JavaStreamingContext jssc = new JavaStreamingContext(conf, Durations.seconds(1));

        Map<String, Object> kafkaParams = new HashMap();
        kafkaParams.put("bootstrap.servers", "localhost:9092");
        kafkaParams.put("key.deserializer", StringDeserializer.class);
        kafkaParams.put("value.deserializer", StringDeserializer.class);
        kafkaParams.put("group.id", "hello2");
        //kafkaParams.put("auto.offset.reset", "latest");
        kafkaParams.put("enable.auto.commit", false);
        scala.Array array;


        Collection<String> topics = Arrays.asList("test2");

        Map<TopicPartition, Long> offsets = new HashMap<TopicPartition, Long>();
        offsets.put(new TopicPartition("test2", 1), 6410155L);

        final JavaInputDStream<ConsumerRecord<String, String>> stream =
                KafkaUtils.createDirectStream(
                        jssc,
                        LocationStrategies.PreferConsistent(),
                        ConsumerStrategies.<String, String>Subscribe(topics, kafkaParams, offsets)

                );



        stream.foreachRDD(new VoidFunction2<JavaRDD<ConsumerRecord<String, String>>, Time>() {
            public void call(JavaRDD<ConsumerRecord<String, String>> consumerRecordJavaRDD, Time time) throws Exception {
                final OffsetRange[] offsetRanges = ((HasOffsetRanges) consumerRecordJavaRDD.rdd()).offsetRanges();
                for (OffsetRange offsetRange:offsetRanges) {
                    System.out.println(offsetRange.topic()+" "+offsetRange.partition()+" "+offsetRange.fromOffset()+" "+offsetRange.untilOffset());
                }

                consumerRecordJavaRDD.foreach(new VoidFunction<ConsumerRecord<String, String>>() {
                    public void call(ConsumerRecord<String, String> stringStringConsumerRecord) throws Exception {
                       System.out.println(stringStringConsumerRecord.value());
                    }
                });
            }
        });

        jssc.start();
        jssc.awaitTermination();
    }
}
