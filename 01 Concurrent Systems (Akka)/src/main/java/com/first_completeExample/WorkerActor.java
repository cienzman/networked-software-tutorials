package com.first_completeExample;

import java.time.Duration;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import akka.actor.AbstractActor;
import akka.actor.AbstractActorWithStash;
import akka.actor.ActorRef;
import akka.actor.OneForOneStrategy;
import akka.actor.Props;
import akka.actor.SupervisorStrategy;
import akka.actor.AbstractActor.Receive;
import akka.japi.pf.DeciderBuilder;


public class WorkerActor extends AbstractActor {


	// Assume at most one subscriber exists for a given topic
	private Map<String, ActorRef> subscriptions = new HashMap<>();


	@Override
	public Receive createReceive() {
		return receiveBuilder()
				.match(SubscribeMsg.class, this::onSub)
				.match(PublishMsg.class, this::onPub
						).build();
	}


	private void onSub(SubscribeMsg msg) {
		subscriptions.put(msg.getTopic(), msg.getSender());
	}


	private void onPub(PublishMsg msg) throws Exception {
		// If the topic doesn't exist on this worker's map
		if (!subscriptions.containsKey(msg.getTopic()))
			throw new Exception("Topic not registered " + getContext().getSelf().toString());

		subscriptions.get(msg.getTopic()).tell(new NotifyMsg(msg.getValue()), self());
	}


	static Props props() {
		return Props.create(WorkerActor.class);
	}
}
