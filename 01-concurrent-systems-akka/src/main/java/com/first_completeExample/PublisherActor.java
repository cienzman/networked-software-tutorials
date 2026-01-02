package com.first_completeExample;

import java.time.Duration;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import akka.actor.AbstractActor;
import akka.actor.AbstractActorWithStash;
import akka.actor.ActorRef;
import akka.actor.ActorSelection;
import akka.actor.OneForOneStrategy;
import akka.actor.Props;
import akka.actor.SupervisorStrategy;
import akka.actor.AbstractActor.Receive;
import akka.japi.pf.DeciderBuilder;

public class PublisherActor extends AbstractActor {
	
	private ActorSelection broker;
	
	@Override
	public void preStart() {
	    String brokerPath = "akka.tcp://BrokerSystem@127.0.0.1:6123/user/broker";
	    this.broker = getContext().actorSelection(brokerPath);

	    System.out.println("PUBLISHER: connected to remote broker");
	}

	@Override
	public AbstractActor.Receive createReceive() {
		return receiveBuilder()
				//.match(ConfigMsg.class, this::onConfig)
				.match(PublishMsg.class, this::onPub)
				.build();
	}
	
	/*
	private void onConfig(ConfigMsg msg) {
		System.out.println("PUBLISHER: Received configuration message!");
		this.broker = msg.getActorRef();
	} */
	
	private void onPub(PublishMsg msg) {
		System.out.println("PUBLISHER: Received publish command!");
		broker.tell(msg, self());
	}

	static Props props() {
		return Props.create(PublisherActor.class);
	}
}
