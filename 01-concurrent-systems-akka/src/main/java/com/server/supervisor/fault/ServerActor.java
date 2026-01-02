package com.server.supervisor.fault;

import akka.actor.AbstractActor;
import akka.actor.ActorRef;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import com.faultTolerance.counter.CounterSupervisor;

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

	void onPutMsg(PutMsg msg) throws Exception {
		if (msg.getName().equals(SupervisorActor.FAULT_NAME)) {
			System.out.println("FAULT ! I am the server and I'm emulating a FAULT!");		
			throw new Exception("Actor fault!"); 	
		} else {
			contactList.put(msg.getName(),msg.getAddress());
			System.out.println("I'm the server and I've just received a correct name that is "+msg.getName());
		}
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
