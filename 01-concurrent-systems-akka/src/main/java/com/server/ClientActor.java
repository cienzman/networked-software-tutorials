package com.server;

import static akka.pattern.Patterns.ask;
import scala.concurrent.duration.Duration;

import java.io.IOException;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeoutException;

import com.faultTolerance.counter.CounterActor;
import com.faultTolerance.counter.DataMessage;

import akka.actor.AbstractActor;
import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import akka.actor.Props;
import akka.actor.AbstractActor.Receive;
import akka.actor.typed.javadsl.ReceiveBuilder;
import akka.pattern.Patterns;
import scala.concurrent.Await;
import scala.concurrent.Future;

public class ClientActor extends AbstractActor{

	private static final int numThreads = 10;
	private static final int numMessages = 100;
	private ActorRef server;


	public ClientActor(ActorRef server) {
		this.server = server;
	}

	void onStartMsg(StartClientMsg msg) throws TimeoutException, InterruptedException {
		sendWhatYouWant();
	}
	
	public void sendWhatYouWant() throws TimeoutException, InterruptedException {
		// Send messages from multiple threads in parallel0
		final ExecutorService exec = Executors.newFixedThreadPool(numThreads);

		for (int i = 0; i < numMessages; i++) {
			final int id=i;
			// Send a Put Message
			PutMsg putMsg=new PutMsg(id);
			server.tell(putMsg, this.getSelf());
			System.out.println("ContactList updated with: NAME =  " + putMsg.getName() + " and ADDRESS = " + putMsg.getAddress());
			// exec.submit(() -> server.tell(new PutMsg(id), ActorRef.noSender()));
			
			// Send a Get Message
			GetMsg getMsg=new GetMsg(id);
			Future<Object> future = Patterns.ask(server, getMsg, 5000);
			System.out.println("My name is: " + getMsg.getName());
			ReplyMsg reply = (ReplyMsg) Await.result(future, Duration.create("5 seconds")); // you should implement defensive programming: create a clause for the replyMsg
			System.out.println("Waiting for reply ..... ");
			System.out.println("Received address: " + reply.getAddress());
			// exec.submit(() -> server.tell(getMsg, getSelf()));
		}
	}

	@Override
	public Receive createReceive() {
		return receiveBuilder().match(StartClientMsg.class, this::onStartMsg).
				build();
	}
	
	public static Props props(ActorRef server) {
		return Props.create(ClientActor.class, () -> new ClientActor(server));
	}

}
