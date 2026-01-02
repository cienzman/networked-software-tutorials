package com.first_completeExample;

import java.time.Duration;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import com.second_completeExample.SensorProcessorActor;

import akka.actor.AbstractActor;
import akka.actor.AbstractActorWithStash;
import akka.actor.ActorRef;
import akka.actor.OneForOneStrategy;
import akka.actor.Props;
import akka.actor.SupervisorStrategy;
import akka.actor.AbstractActor.Receive;
import akka.japi.pf.DeciderBuilder;

public class BrokerActor extends AbstractActorWithStash {
	private ActorRef workerEven;
	private ActorRef workerOdd;
	
	public BrokerActor() {
		workerEven = getContext().actorOf(WorkerActor.props());
		workerOdd = getContext().actorOf(WorkerActor.props());
	}

	private static SupervisorStrategy strategy = new OneForOneStrategy(1, Duration.ofMinutes(1),
			DeciderBuilder.match(Exception.class, e -> SupervisorStrategy.resume()).build());

	@Override
	public SupervisorStrategy supervisorStrategy() {
		return strategy;
	}

	@Override
	public AbstractActor.Receive createReceive() {
		return batchFalse();
	}

	private final Receive batchFalse() {
		return receiveBuilder()
				.match(SubscribeMsg.class, this::onSub)
				.match(PublishMsg.class, this::onPub)
				.match(BatchMsg.class, this::onBatch)
				.build();
	}


	private Receive batchTrue() {
		return receiveBuilder()
				.match(SubscribeMsg.class, this::onSub)
				.match(PublishMsg.class, this::onPubBatch)
				.match(BatchMsg.class, this::onBatch)
				.build();
	}


	private void onSub(SubscribeMsg msg) {
		if (msg.getKey() % 2 == 1) {
			System.out.println("BROKER: Subscribed to odd worker");
			workerOdd.tell(msg, self());
		} else {
			System.out.println("BROKER: Subscribed to even worker");
			workerEven.tell(msg, self());
		}
	}

	private void onPub(PublishMsg msg) {
		workerOdd.tell(msg, self());
		workerEven.tell(msg, self());
	}
	
    private void onPubBatch(PublishMsg msg) {
        System.out.println("BROKER: publish message stashed");
        stash();
    }



	private void onBatch(BatchMsg msg) {
		if (msg.isOn()) {
			System.out.println("BROKER: batching turned on");
			getContext().become(batchTrue());
		} else {
			System.out.println("BROKER: batching turned off");
			getContext().become(batchFalse());
			unstashAll();
		}
	}


	static Props props() {
		return Props.create(BrokerActor.class);
	}
}
