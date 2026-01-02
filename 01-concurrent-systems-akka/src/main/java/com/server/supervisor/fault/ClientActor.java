package com.server.supervisor.fault;

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

	@Override
	public Receive createReceive() {
		return receiveBuilder().match(StartClientMsg.class, this::onStartMsg).
				match(ReplyMsg.class, this::onReplyMsg).
				build();
	}

	void onStartMsg(StartClientMsg msg) throws TimeoutException, InterruptedException {
		this.server = msg.getServer();
		sendWhatYouWant();
	}

	public void sendWhatYouWant() throws TimeoutException, InterruptedException {
		// Send messages from multiple threads in parallel0
		final ExecutorService exec = Executors.newFixedThreadPool(numThreads);

		// Send a Put Message
		try {
			PutMsg putMsg1=new PutMsg("Hello1");
			PutMsg putMsg2=new PutMsg("Hello2");
			PutMsg putFaultMsg=new PutMsg("Fault");
			PutMsg putMsg3=new PutMsg("Hello3");
			PutMsg putMsg4=new PutMsg("Hello4");
			server.tell(putMsg1, this.getSelf());
			System.out.println(" Client --> NAME =  " + putMsg1.getName() + " and ADDRESS = " + putMsg1.getAddress());
			server.tell(putMsg2, this.getSelf());
			System.out.println(" Client --> NAME =  " + putMsg2.getName() + " and ADDRESS = " + putMsg2.getAddress());
			server.tell(putFaultMsg, this.getSelf());
			System.out.println(" Client --> NAME =  " + putFaultMsg.getName() + " and ADDRESS = " + putFaultMsg.getAddress());
			server.tell(putMsg3, this.getSelf());
			System.out.println(" Client --> NAME =  " + putMsg3.getName() + " and ADDRESS = " + putMsg3.getAddress());
			server.tell(putMsg4, this.getSelf());
			System.out.println(" Client --> NAME =  " + putMsg4.getName() + " and ADDRESS = " + putMsg4.getAddress());
			// exec.submit(() -> server.tell(new PutMsg(id), ActorRef.noSender()));
		} catch (Exception e1) {
			//e1.printStackTrace();
			e1.getMessage();
		}

		// Send a Get Message for Hello1
		GetMsg getMsg=new GetMsg("Hello1");
		Future<Object> future = Patterns.ask(server, getMsg, 5000);
		// scala.concurrent.Future<Object> future = ask(server, getMsg, 5000);
		System.out.println("This is a get message: my name is " + getMsg.getName());
		try {
			ReplyMsg reply = (ReplyMsg) Await.result(future, Duration.create("5 seconds")); // you should implement defensive programming: create a clause for the replyMsg
			System.out.println("Waiting for reply ..... ");
			if(reply.getAddress() != null) {
				System.out.println("Received address: " + reply.getAddress());
			} else {
				System.out.println("Cannot Receive address: uncorrect name ");
			}
		} catch (TimeoutException | InterruptedException e) {
			// TODO Auto-generated catch block
			// e.printStackTrace();
			e.getMessage();
		}


		// Send a Get Message FAULT
		GetMsg getFaultMsg=new GetMsg("Fault");
		// scala.concurrent.Future<Object> futureFault = ask(server, getFaultMsg, 5000);
		Future<Object> futureFault = Patterns.ask(server, getFaultMsg, 5000);
		System.out.println("My name is: " + getFaultMsg.getName());
		try {
			ReplyMsg replyFault = (ReplyMsg) Await.result(futureFault, Duration.create("5 seconds")); // you should implement defensive programming: create a clause for the replyMsg
			System.out.println("Waiting for reply ..... ");
			if(replyFault.getAddress() != null) {
				System.out.println("Received address: " + replyFault.getAddress());
			} else {
				System.out.println("Cannot Receive address: uncorrect name ");
			}
			// exec.submit(() -> server.tell(getMsg, getSelf()));
		} catch (TimeoutException | InterruptedException e) {
			// TODO Auto-generated catch block
			//e.printStackTrace();
			e.getMessage();
		}


		// Send a Get Message Hello2
		GetMsg getMsg2=new GetMsg("Hello2");
		Future<Object> future2 = Patterns.ask(server, getMsg2, 5000);
		// scala.concurrent.Future<Object> future2 = ask(server, getMsg2, 5000);
		System.out.println("My name is: " + getMsg2.getName());
		try {
			ReplyMsg reply2 = (ReplyMsg) Await.result(future2, Duration.create("5 seconds")); // you should implement defensive programming: create a clause for the replyMsg
			System.out.println("Waiting for reply ..... ");
			if(reply2.getAddress() != null) {
				System.out.println("Received address: " + reply2.getAddress());
			} else {
				System.out.println("Cannot Receive address: uncorrect name ");
			}
			// exec.submit(() -> server.tell(getMsg, getSelf()));
		} catch (TimeoutException | InterruptedException e) {
			// TODO Auto-generated catch block
			//e.printStackTrace();
			e.getMessage();
		}


	}

	void onReplyMsg(ReplyMsg msg) {
		System.out.println("CLIENT: Unsolicited reply, this should not happen!");
	}

	public static Props props() {
		return Props.create(ClientActor.class);
	}

}
