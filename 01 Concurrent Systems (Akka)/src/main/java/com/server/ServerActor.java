package com.server;

import akka.actor.AbstractActor;
import akka.actor.ActorRef;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import akka.actor.Props;

public class ServerActor extends AbstractActor {
	
	// A thread-safe map to store contacts (key = id, value = name)
    private final Map<String, String> contactList;
	
	public ServerActor() {
		this.contactList = new ConcurrentHashMap<>();
	}

	@Override
	public Receive createReceive() {
		return receiveBuilder().match(PutMsg.class, this::onPutMsg).
				match(GetMsg.class, this::onGetMsg).
				build();
	}

	void onPutMsg(PutMsg msg) {
		contactList.put(msg.getName(),msg.getAddress());
		// add a log
	}
	
	void onGetMsg(GetMsg msg) {
		ActorRef my_client = getSender();
		String client_address = contactList.get(msg.getName());
		// System.out.println("The address of ... is", msg.getName(),client_address );
		// System.out.println("Counter decreased to " + counter);
		my_client.tell(new ReplyMsg(client_address), this.getSelf()); 
	}

	public static Props props() {
		return Props.create(ServerActor.class);
	}

}
