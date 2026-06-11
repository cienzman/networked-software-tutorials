#include "contiki.h"
#include "net/routing/routing.h"
#include "net/netstack.h"
#include "net/ipv6/simple-udp.h"

#include "sys/log.h"
#include "random.h"

#define LOG_MODULE "App"
#define LOG_LEVEL LOG_LEVEL_INFO

#define UDP_CLIENT_PORT	8765
#define UDP_SERVER_PORT	5678
#define MAX_RECEIVERS 10
#define MAX_READINGS 10
#define SEND_INTERVAL (2 * CLOCK_SECOND)

#define ALERT_THRESHOLD 19

static struct simple_udp_connection udp_conn;
//static process_event_t alert_event;

static unsigned readings[MAX_READINGS];
static uip_ipaddr_t receivers[MAX_RECEIVERS];
static unsigned next_reading=0;
static unsigned known_clients=0;

PROCESS(udp_server_process, "UDP server");
AUTOSTART_PROCESSES(&udp_server_process);
/*---------------------------------------------------------------------------*/
static void
udp_rx_callback(struct simple_udp_connection *c,
         const uip_ipaddr_t *sender_addr,
         uint16_t sender_port,
         const uip_ipaddr_t *receiver_addr,
         uint16_t receiver_port,
         const uint8_t *data,
         uint16_t datalen)
{
  static uint8_t flag = 0;
  static uint8_t i = 0;
  static uint8_t j = 0;
  for( i = 0; i < known_clients; i++){
    if(uip_ipaddr_cmp(sender_addr, &receivers[i])){
      flag = 1; // found
      break;
    }
  }

  if( flag != 1 ){ //not foud
    if (known_clients < MAX_RECEIVERS){
      uip_ipaddr_copy(&receivers[known_clients], sender_addr);
      known_clients++;
    }
    else{
      LOG_INFO("MAX N CLIENTS REACHED");
      return;
    }
  }

  unsigned reading = *(unsigned *)data;

  /* Add reading */
  readings[next_reading++] = reading;
  if (next_reading == MAX_READINGS) {
    next_reading = 0;
  }

  /* Compute average */
  float average;
  unsigned sum = 0;
  unsigned no = 0;  
  for ( j=0; j<MAX_READINGS; j++) {
    if (readings[j]!=0){
      sum = sum+readings[j];
      no++;
    }
  }
  average = ((float)sum)/no;
  LOG_INFO("Current average is %f \n",average);

  if(average > ALERT_THRESHOLD){
    process_poll(&udp_server_process);
    //process_post(&udp_server_process, alert_event, NULL);
  }
  
}
/*---------------------------------------------------------------------------*/
PROCESS_THREAD(udp_server_process, ev, data)
{
  PROCESS_BEGIN();

  static struct etimer periodic_timer;
  static int i = 0;
  static int j = 0;

  // Init event
  //alert_event=process_alloc_event();

  /* Initialize temperature buffer */
  for ( i=0; i<next_reading; i++) {
    readings[i] = 0;
  }  

  /* Initialize DAG root */
  NETSTACK_ROUTING.root_start();

  /* Initialize UDP connection */
  simple_udp_register(&udp_conn, UDP_SERVER_PORT, NULL,
                      UDP_CLIENT_PORT, udp_rx_callback);

  while (1) {
    PROCESS_WAIT_EVENT_UNTIL(ev==PROCESS_EVENT_POLL);
    LOG_INFO("[server]: WAKING UP! with know clients = %d", known_clients);
    for( j = 0; j < known_clients; j++){ 
      LOG_INFO("[server] send ALERT to ");
      LOG_INFO_6ADDR(&receivers[j]);
      simple_udp_sendto(&udp_conn, NULL, 0, &receivers[j]);
      etimer_set(&periodic_timer, random_rand() % SEND_INTERVAL);
      PROCESS_WAIT_EVENT_UNTIL(etimer_expired(&periodic_timer));
    }
  }
  
  
  PROCESS_END();
}