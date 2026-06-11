#include "contiki.h"
#include <stdio.h>
#include <stdlib.h>
#include <stdbool.h>

/* Defines */
#define LEN 3
#define MAX_FOR_TIMER 2

/*---------------------------------------------------------------------------*/
PROCESS(producer_process, "Producer process");
PROCESS(consumer_process, "Consumer process");
AUTOSTART_PROCESSES(&producer_process, &consumer_process);

/*---------------------------------------------------------------------------*/
static process_event_t event_data_ready;
static process_event_t event_slot_free; 

static int queue[LEN];
static int queue_count = 0; // Track number of items explicitly

/*---------------------------------------------------------------------------*/
/* Helpers */


static void print_queue() {
  printf("## queue [%d/%d] -> [", queue_count, LEN);
  for(int k = 0; k < LEN; k++) {
    if(k < queue_count) {
        printf("%d", queue[k]);
    } else {
        printf("0");
    }
    
    if(k != LEN - 1) printf(", ");
  }
  printf("]\n");
}

/* Queue Management */
static bool queue_is_full() {
  return queue_count >= LEN;
}

static bool queue_is_empty() {
  return queue_count == 0;
}

static void queue_push(int item) {
  if (queue_count < LEN) {
    queue[queue_count] = item;
    queue_count++;
  }
}

static int queue_pull() {
  int item = queue[0];
  // Shift items left
  for(int k = 0; k < LEN - 1; k++) {
    queue[k] = queue[k+1];
  }
  queue[LEN-1] = 0;
  queue_count--;
  return item;
}

/*---------------------------------------------------------------------------*/
PROCESS_THREAD(producer_process, ev, data)
{
  static struct etimer timer_p;
  static int random_p;
  
  PROCESS_BEGIN();

  event_data_ready = process_alloc_event();

  printf("[Producer] Started\n");

  while(1) {
    random_p=(rand()%MAX_FOR_TIMER)+1;
    etimer_set(&timer_p, CLOCK_SECOND*random_p);
    printf("Producer Timer: %d s\n",random_p);
    PROCESS_WAIT_EVENT_UNTIL(etimer_expired(&timer_p));

    /*int delay = 1 + rand() % 2; // try to see what happens with int delay = rand() % 2;
    for(int k = 0; k < delay; k++) PROCESS_PAUSE();*/

      if(queue_is_full()){ 
        printf("[Producer] Queue FULL. Suspending...\n");
        PROCESS_WAIT_EVENT_UNTIL(ev == event_slot_free);
        printf("[Producer] Resumed (Slot available)\n");
      }

      int item = 1 + rand() % 10;
      queue_push(item);
      printf("[Producer] Produced %d\n", item);
      print_queue();
      
      if(queue_count == 1){ // send event only if queue was empty
        process_post(&consumer_process, event_data_ready, NULL);
      }
      
  }
  PROCESS_END();
}

/*---------------------------------------------------------------------------*/
PROCESS_THREAD(consumer_process, ev, data)
{
  static struct etimer timer_c;
  static int random_c;

  PROCESS_BEGIN();

  event_slot_free = process_alloc_event();

  printf("[Consumer] Started\n");

  while(1){
    random_c= (rand()%MAX_FOR_TIMER)+1;
    etimer_set(&timer_c, CLOCK_SECOND * random_c);
    printf("Consumer Timer: %d s\n",random_c);
    PROCESS_WAIT_EVENT_UNTIL(etimer_expired(&timer_c));
    /*int delay = (rand() % 5) + 5; 
    for(int k = 0; k < delay; k++) PROCESS_PAUSE();*/

    if(queue_is_empty()){ 
      printf("[Consumer] Queue EMPTY. Suspending...\n");
      PROCESS_WAIT_EVENT_UNTIL(ev == event_data_ready);
      printf("[Consumer] Resumed (Data available)\n");
    } 

    int item = queue_pull();
    printf("[Consumer] Consumed %d\n", item);
    print_queue();

    if(queue_count == 2){ // send event only if queue was full
      process_post(&producer_process, event_slot_free, NULL);
    }
  }
  PROCESS_END();
}