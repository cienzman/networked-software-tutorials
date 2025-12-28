package com.counter.pt2;

import akka.actor.AbstractActor;
import akka.actor.Props;
import java.util.Optional;

public class CounterActorIncrementDecrement extends AbstractActor {


	private int counter;


	public CounterActorIncrementDecrement() {
		this.counter = 0;
	}


	@Override
	public Receive createReceive() {
		return receiveBuilder().match(IncrementMessage.class, this::onIncrementMessage).match(DecrementMessage.class, this::onDecrementMessage).build();
	}


	void onIncrementMessage(IncrementMessage msg) {
		++counter;
		System.out.println("IncrementMessage received to increase counter to " + counter);
	}
	
	void onDecrementMessage(DecrementMessage msg) {
		--counter;
		System.out.println("DecrementMessage received to increase counter to " + counter);
	}


	static Props props() {
		return Props.create(CounterActorIncrementDecrement.class);
	}


}


