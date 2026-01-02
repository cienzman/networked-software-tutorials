package com.server.supervisor.fault;

import static akka.pattern.Patterns.ask;
import static java.util.concurrent.TimeUnit.SECONDS;

import java.io.IOException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeoutException;

import com.faultTolerance.counter.CounterActor;
import com.faultTolerance.counter.CounterSupervisorActor;

import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import akka.actor.Props;
import akka.pattern.Patterns;
import scala.concurrent.Await;
import scala.concurrent.Future;
import scala.concurrent.duration.Duration;

public class Start {
	
	public static void main(String[] args) throws Exception, InterruptedException {
		
		scala.concurrent.duration.Duration timeout = scala.concurrent.duration.Duration.create(5, SECONDS);
		
		final ActorSystem sys = ActorSystem.create("System");
		final ActorRef supervisor = sys.actorOf(SupervisorActor.props(), "supervisor");
		//final ActorRef server = sys.actorOf(ServerActor.props(), "server");
		final ActorRef client = sys.actorOf(ClientActor.props(), "client");
		ActorRef server = null;
		
		
			// Asks the supervisor to create the child actor and returns a reference
			scala.concurrent.Future<Object> waitingForServer = ask(supervisor, Props.create(ServerActor.class), 5000);
			try {
			server = (ActorRef) waitingForServer.result(timeout, null);
		} catch (TimeoutException | InterruptedException e) {
			//e.printStackTrace();
			e.getMessage();
		} 
		
		client.tell(new StartClientMsg(server), ActorRef.noSender());

		// Wait for all messages to be sent and received
		try {
			System.in.read();
		} catch (IOException e) {
			//e.printStackTrace();
			e.getMessage();
		}
		sys.terminate();

	}

}
