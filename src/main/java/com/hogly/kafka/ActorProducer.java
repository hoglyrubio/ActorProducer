package com.hogly.kafka;

import akka.actor.Props;
import akka.actor.UntypedActor;
import akka.event.Logging;
import akka.event.LoggingAdapter;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

public class ActorProducer extends UntypedActor {

  LoggingAdapter LOG = Logging.getLogger(getContext().system(), this);
  public static String START_SEND = "start sending message";
  private KafkaProducer<String, String> kafkaProducer;

  public ActorProducer() {
    this.kafkaProducer = buildProducer();
  }

  private KafkaProducer<String,String> buildProducer() {
    Properties props = new Properties();
    props.put("bootstrap.servers", "localhost:9092");
    props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
    props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
    return new KafkaProducer<>(props);
  }

  public static Props props() {
    return Props.create(ActorProducer.class);
  }

  @Override
  public void onReceive(Object message) throws Exception {
    if (START_SEND.equals(message)) {
      startSend();
    } else {
      unhandled(message);
    }
  }

  private void startSend() throws ExecutionException, InterruptedException {
    for (int i=0; i<10; i++) {
      ProducerRecord<String, String> record = new ProducerRecord<>("REP", "Rep 01", "Rep one");
      Future<RecordMetadata> result = kafkaProducer.send(record);
      RecordMetadata metadata = result.get();
      LOG.info("Sent. topic: {} partition: {} offset: {} ", metadata.topic(), metadata.partition(), metadata.offset());
    }
  }

}
