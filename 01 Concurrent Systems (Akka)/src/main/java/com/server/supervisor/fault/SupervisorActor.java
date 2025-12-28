package com.server.supervisor.fault;

import akka.actor.AbstractActor;
import akka.actor.OneForOneStrategy;
import akka.actor.Props;
import akka.actor.SupervisorStrategy;
import akka.japi.pf.DeciderBuilder;
import java.time.Duration;

public class SupervisorActor extends AbstractActor {
	
	public static final String FAULT_NAME ="name: Fault";

	 // #strategy
    private static SupervisorStrategy strategy =
        new OneForOneStrategy(
            1, // Max no of retries --> at most 1 fault
            Duration.ofMinutes(1), // Within what time period
            DeciderBuilder.match(Exception.class, e -> SupervisorStrategy.stop())
                .build());
    
    /*
     * 
     * DeciderBuilder.match(Exception.class, e -> SupervisorStrategy.resume())  // the state is saved 
	 * DeciderBuilder.match(Exception.class, e -> SupervisorStrategy.restart()) // the state is  not save, and when the actor restart actually it is another actor 
     * DeciderBuilder.match(Exception.class, e -> SupervisorStrategy.stop())  // the 2nd is not delivered because the actor is not active.
     * */

    @Override
    public SupervisorStrategy supervisorStrategy() {
      return strategy;
    }

	public SupervisorActor() {
	}

	@Override
	public Receive createReceive() {
		// Creates the child actor within the supervisor actor context
		return receiveBuilder()
		          .match(
		              Props.class,
		              props -> {
		                getSender().tell(getContext().actorOf(props), getSelf());
		              })
		          .build();
	}

	static Props props() {
		return Props.create(SupervisorActor.class);
	}

}
