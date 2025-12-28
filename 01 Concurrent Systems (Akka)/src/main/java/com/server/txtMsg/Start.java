package com.server.txtMsg;

import java.io.IOException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import akka.pattern.Patterns;
import scala.concurrent.Await;
import scala.concurrent.Future;
import scala.concurrent.duration.Duration;

public class Start {
	
	public static void main(String[] args) throws Exception, InterruptedException {

		final ActorSystem sys = ActorSystem.create("System");
		final ActorRef server = sys.actorOf(ServerActor.props(), "server");
		final ActorRef client = sys.actorOf(ClientActor.props(), "client");
		final ActorRef client2 = sys.actorOf(ClientActor.props(), "client2");
		//final ActorRef client2 = sys.actorOf(ClientActor.props(), "client2");
		
		client.tell(new StartClientMsg(server), ActorRef.noSender());
		client2.tell(new StartClientMsg(server), ActorRef.noSender());

		// Wait for all messages to be sent and received
		try {
			System.in.read();
		} catch (IOException e) {
			e.printStackTrace();
		}
		sys.terminate();
	}
}
