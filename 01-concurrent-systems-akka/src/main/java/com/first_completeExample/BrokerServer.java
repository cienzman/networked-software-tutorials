package com.first_completeExample;

import java.io.File;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import akka.actor.ActorRef;
import akka.actor.ActorSystem;

public class BrokerServer {

    public static void main(String[] args) {

        Config conf = ConfigFactory.parseFile(new File("conf.conf"));
        ActorSystem sys = ActorSystem.create("BrokerSystem", conf);
        sys.actorOf(BrokerActor.props(), "broker");

        System.out.println("Broker server running on akka.tcp://BrokerSystem@127.0.0.1:6123/user/broker");
    }
}
