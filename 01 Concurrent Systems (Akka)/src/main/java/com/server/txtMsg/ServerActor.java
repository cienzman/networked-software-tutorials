package com.server.txtMsg;

import akka.actor.AbstractActor;
import akka.actor.AbstractActorWithStash;
import akka.actor.ActorRef;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import akka.actor.Props;

public class ServerActor extends AbstractActorWithStash {

	public ServerActor() {
	}

	@Override
	public Receive createReceive() {
		return awake();
	}
	
	private final Receive awake() {
		return receiveBuilder(). //
				match(TextMsg.class, this::onTextMsg). //
				match(SleepMsg.class, this::onSleepMsg). //
				build();
	}
	
	private final Receive asleep() {
		return receiveBuilder(). //
				match(TextMsg.class, this::onTextMsgWhileSleeping). //
				match(WakeupMsg.class, this::onWakeupMsg). //
				build();
	}
	
	private void onWakeupMsg(WakeupMsg msg) {
		System.out.println("Becoming awake");	
		unstashAll();
		getContext().become(awake());
	}

	private void onSleepMsg(SleepMsg msg) {
		System.out.println("I'm the server now I'm Going to sleep");
		getContext().become(asleep());
	}

	void onTextMsg(TextMsg msg) {
		ActorRef client = getSender();
		client.tell(msg, this.getSelf());
		System.out.println("Server: I'm sending back "+msg.getContent()+"from "+client.path().name());
	}
	
	void onTextMsgWhileSleeping(TextMsg msg) {
		stash();
	}
	
	

	public static Props props() {
		return Props.create(ServerActor.class);
	}

}

















