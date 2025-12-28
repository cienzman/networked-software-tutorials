package com.first_completeExample;

import java.time.Duration;
import java.util.HashMap;
import java.util.Iterator;
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

public class SubscriberActor extends AbstractActor {

	private ActorSelection broker;

	@Override
	public void preStart() {
		String brokerPath = "akka.tcp://BrokerSystem@127.0.0.1:6123/user/broker";
		this.broker = getContext().actorSelection(brokerPath);

		System.out.println("SUBSCRIBER: connected to remote broker");
	}

	@Override
	public AbstractActor.Receive createReceive() {
		return receiveBuilder()
				//.match(ConfigMsg.class, this::onConfig)
				.match(SubscribeMsg.class, this::onSub)
				.match(NotifyMsg.class, this::onNot)
				.build();
	}

	/*
	private void onConfig(ConfigMsg msg) {
		System.out.println("SUBSCRIBER: Received configuration message!");
		this.broker = msg.getActorRef();
	} */

	private void onSub(SubscribeMsg msg) {
		System.out.println("SUBSCRIBER: Received subscribe command!");
		broker.tell(msg, getSelf());	
	}

	private void onNot(NotifyMsg msg) {
		System.out.println("SUBSCRIBER: Received notify message!");
		System.out.println("Received value: " + msg.getValue());

	}


	static Props props() {
		return Props.create(SubscriberActor.class);
	}
}
