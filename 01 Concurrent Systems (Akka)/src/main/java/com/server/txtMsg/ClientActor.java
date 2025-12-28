package com.server.txtMsg;

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
	private static final int numMessages = 1;
	private ActorRef server;
	
	@Override
	public Receive createReceive() {
		return receiveBuilder().match(StartClientMsg.class, this::onStartMsg).
				match(TextMsg.class, this::onTextMsg).
				build();
	}

	void onStartMsg(StartClientMsg msg) throws TimeoutException, InterruptedException {
		server = msg.getActorRef();
		sendTextMsg();
	}
	
	public void sendTextMsg() throws TimeoutException, InterruptedException {
		// Send messages from multiple threads in parallel0
		final ExecutorService exec = Executors.newFixedThreadPool(numThreads);

		for (int i = 0; i < numMessages; i++) {
			final int id=i;
			// Send a Text Message
			TextMsg textMsg0=new TextMsg(id);
			TextMsg textMsg1=new TextMsg(id+10);
			TextMsg textMsg2=new TextMsg(id+20);
			TextMsg textMsg3=new TextMsg(id+30);
			TextMsg textMsg4=new TextMsg(id+40);
			TextMsg textMsg5=new TextMsg(id+50);
			TextMsg textMsg6=new TextMsg(id+60);
			TextMsg textMsg7=new TextMsg(id+70);
			SleepMsg sleepMsg=new SleepMsg();
			WakeupMsg wakeupMsg=new WakeupMsg();
			// server.tell(putMsg, this.getSelf());
			exec.submit(() -> server.tell(textMsg0, this.getSelf()));
			System.out.println("Client: " + self().path().name()+ " Message: " +textMsg0.getContent());
			
			exec.submit(() -> server.tell(textMsg1, this.getSelf()));
			System.out.println("Client: " + self().path().name()+ " Message: " +textMsg1.getContent());
			exec.submit(() -> server.tell(textMsg2, this.getSelf()));
			System.out.println("Client: " + self().path().name() + " Message: " +textMsg2.getContent());
			exec.submit(() -> server.tell(textMsg3, this.getSelf()));
			System.out.println("Client: " + self().path().name() + " Message: " +textMsg3.getContent());
			
			exec.submit(() -> server.tell(sleepMsg, this.getSelf()));
			System.out.println("Client: " + self().path().name()+ " Message: " +sleepMsg.getContent());

			exec.submit(() -> server.tell(textMsg4, this.getSelf()));
			System.out.println("Client: " + self().path().name()+ " Message: " +textMsg4.getContent());
			exec.submit(() -> server.tell(textMsg5, this.getSelf()));
			System.out.println("Client: " + self().path().name() + " Message: " +textMsg5.getContent());
			exec.submit(() -> server.tell(textMsg6, this.getSelf()));
			System.out.println("Client: " + self().path().name() + " Message: " +textMsg6.getContent());
			
			exec.submit(() -> server.tell(wakeupMsg, this.getSelf()));
			System.out.println("Client: " + self().path().name() + " Message: " +wakeupMsg.getContent());
			
		}
	}
	
	void onTextMsg(TextMsg msg) {
		System.out.println("I receive back: " + msg.getContent());
	}
	
	public static Props props() { // ActorRef server
		return Props.create(ClientActor.class);
		// this.server = server;
	}

}
