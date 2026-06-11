#include "contiki.h"

#include <stdio.h>

/*---------------------------------------------------------------------------*/
static void ctimer_callback(void *data);
#define CTIMER_INTERVAL 2 * CLOCK_SECOND
static struct ctimer print_ctimer;
/*---------------------------------------------------------------------------*/
static void rtimer_callback(struct rtimer *t, void *data);
#define RTIMER_HARD_INTERVAL 2 * RTIMER_SECOND
static struct rtimer print_rtimer;
/*---------------------------------------------------------------------------*/
#define RTIMER_CLOCK_OFFSET RTIMER_SECOND / 250 // 4ms
/*---------------------------------------------------------------------------*/
PROCESS(hello_world_ctimer, "Hello world process");
AUTOSTART_PROCESSES(&hello_world_ctimer);
/*---------------------------------------------------------------------------*/
static void ctimer_callback(void *data){

  printf("%s", (char *)data);
  
  /* Reschedule the ctimer. */
  ctimer_set(&print_ctimer, CTIMER_INTERVAL, ctimer_callback, "Hello world CT\n");  
}
/*---------------------------------------------------------------------------*/
static void rtimer_callback(struct rtimer *t, void *data){

  printf("%s", (char *)data);
  //process_poll(&hello_world_ctimer); //CASE 2
  
  /* Reschedule the rtimer. */
  rtimer_set(&print_rtimer, t->time + RTIMER_HARD_INTERVAL, 0, rtimer_callback, "Hello world RT\n");
}
/*---------------------------------------------------------------------------*/
PROCESS_THREAD(hello_world_ctimer, ev, data)
{
  PROCESS_BEGIN();


  rtimer_init();

  /* Schedule the rtimer: absolute time */
  rtimer_set(&print_rtimer, RTIMER_NOW() + RTIMER_HARD_INTERVAL - RTIMER_CLOCK_OFFSET, 0, rtimer_callback, "Hello world RT\n");

  /* Schedule the ctimer. */
  ctimer_set(&print_ctimer, CTIMER_INTERVAL, ctimer_callback, "Hello world CT\n");
  
  /*
  while(1) { //CASE 2
    PROCESS_WAIT_EVENT();

    if(ev == PROCESS_EVENT_POLL) {
        // Print only when polled by the rtimer callback
        printf("Hello world RT\n");
    }
  }*/

  /* Only useful for platform native. */
  PROCESS_WAIT_EVENT();

  PROCESS_END();
}


/*
* - cd nsds-contiki-ng/tools/cooja
* - ant run
* - file --> new_simulation --> motes --> sky motes --> browse to timer.c --> compile --> create

CASE 1:
static void rtimer_callback(struct rtimer *t, void *data){
  printf("%s", (char *)data);
  Reschedule the rtimer.
  rtimer_set(&print_rtimer, RTIMER_NOW() + RTIMER_HARD_INTERVAL, 0, rtimer_callback, "Hello world RT\n");
}
  without the while in the main
* - What we notice from output is that (Real-time Timer) is slowly drifting (getting later by about 1ms every cycle),
* - while the ctimer (Callback Timer) remains perfectly stable.
* - The cause of the drift is that in the rtimer callback when the timer fires, the cpu executes printf(...) (which takes time).
      at that point when the code reaches the rtimer_set we will have that RTIMER_NOW() is so more the time in which the callback started,
      but it is the time in which the callback started plus the time to execute the printf --> drifting
  - We can fix this by exploiting the struct rtimer *t parameters that is passed to the rtimer callback and that contains the real scheduled time
      of the current timer event. --> so we can use  t->time instead of RTIMER_NOW().
  - Notice that this problem is not present for the ctimer because ctimer is based on the system's low-resolution tick count (clock_time()) and
      when ctimer_callback runs, the time spent in printf is usually less than the duration of a single clock tick and so when ctimer_set is called 
      the system tick counter hasn't had a chance to increment yet.

CASE 2: 
static void rtimer_callback(struct rtimer *t, void *data){
  process_poll(&hello_world_ctimer); //CASE 2
  rtimer_set(&print_rtimer, RTIMER_NOW() + RTIMER_HARD_INTERVAL, 0, rtimer_callback, "Hello world RT\n");
}

while(1) { //CASE 2
  PROCESS_WAIT_EVENT();

  if(ev == PROCESS_EVENT_POLL) {
      // Print only when polled by the rtimer callback
      printf("Hello world RT\n");
  }
}

- This is an alternative since it uses process_poll() in the callback instead of the printf --> drift avoided beacuse printf is not used.


RTIMER VS CTIMER
  rtimer_init();
  rtimer_set(&print_rtimer, RTIMER_NOW() + RTIMER_HARD_INTERVAL, 0, rtimer_callback, "Hello world RT\n");

- once the drift is solved the output is like:
        CT at 02.694
        RT at 02.699
        CT at 04.694
        RT at 04.699
        CT at 06.694
        RT at 06.699
        ...
        ...
- This is not a bug or a drift; The 5ms difference exists because they started counting from slightly different "reference points".
- At startup (Time 00:00.694) the clock tick happens. Then our CPU takes 5ms to run the code and to reach the rtimer_set instruction (00:00.699).
- So this is why rtimer is scheduled for 00:00.699 + 2s = 02.699
- To solve this problem we can set an offset of k milliseconds:
    -- #define RTIMER_CLOCK_OFFSET RTIMER_SECOND / 250          (// k = 4ms)
    and write in the main:
    -- rtimer_set(&print_rtimer, RTIMER_NOW() + RTIMER_HARD_INTERVAL - RTIMER_CLOCK_OFFSET, 0, rtimer_callback, "Hello world RT\n");
  This will help us to delete the delay. 
  To determine k we have to run the program again to check the new delay after the addition of the instruction:
      #define RTIMER_CLOCK_OFFSET RTIMER_SECOND / 250 

*/
