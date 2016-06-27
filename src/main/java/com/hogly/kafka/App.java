package com.hogly.kafka;

import akka.actor.ActorRef;
import akka.actor.ActorSystem;

public class App {

  public static void main(String[] args) {
    ActorSystem system = ActorSystem.create("actor-producer");
    ActorRef actorProducer = system.actorOf(ActorProducer.props(), "actorProducer");
    actorProducer.tell(ActorProducer.START_SEND, ActorRef.noSender());
  }

}
