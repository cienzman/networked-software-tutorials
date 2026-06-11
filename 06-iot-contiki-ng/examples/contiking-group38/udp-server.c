#include "contiki.h"
#include "net/routing/routing.h"
#include "net/netstack.h"
#include "net/ipv6/simple-udp.h"
#include "random.h"
#include "sys/log.h"
#include <string.h> 

#define LOG_MODULE "App"
#define LOG_LEVEL LOG_LEVEL_INFO

#define UDP_CLIENT_PORT 8765
#define UDP_SERVER_PORT 5678

#define LOCK_DURATION (5 * CLOCK_SECOND)
#define ERROR_CODE 0
#define LOCK_SUCCESS_CODE 100
#define UNLOCK_SUCCESS_CODE 101

static struct simple_udp_connection udp_conn;
static uint8_t data_structure = 0;

static uip_ipaddr_t current_lock_holder;
static uint8_t is_locked = 0; 

static struct etimer lock_timer;

PROCESS(udp_server_process, "UDP server");
AUTOSTART_PROCESSES(&udp_server_process);
/*---------------------------------------------------------------------------*/

static void
send_response_code(const uip_ipaddr_t *receiver_addr, uint8_t code)
{
  simple_udp_sendto(&udp_conn, &code, sizeof(code), receiver_addr);
}

static void
udp_rx_callback(struct simple_udp_connection *c,
         const uip_ipaddr_t *sender_addr,
         uint16_t sender_port,
         const uip_ipaddr_t *receiver_addr,
         uint16_t receiver_port,
         const uint8_t *data,
         uint16_t datalen)
{
  /*
    LOCK REQUEST --> 0
    READ --> 1
    WRITE --> e.g., 2, 3, 4, 5, 6, 7, 8, 9
  */
  if (datalen == 0) {
    LOG_INFO("Received empty request. Ignoring.\n");
    return;
  }
  
  uint8_t data_received = data[0];
  
  LOG_INFO("Received request %u from ", data_received);
  LOG_INFO_6ADDR(sender_addr);
  LOG_INFO_("\n");

  if(data_received == 0) { // LOCK REQUEST
    if(is_locked == 0) {
      is_locked = 1;
      uip_ipaddr_copy(&current_lock_holder, sender_addr);
      etimer_set(&lock_timer, LOCK_DURATION);
      send_response_code(sender_addr, LOCK_SUCCESS_CODE);
      
    } else {
      send_response_code(sender_addr, ERROR_CODE);
    }
    
  } else if (data_received == 1) { // READ REQUEST
    LOG_INFO("Read request. Sending current value: %u\n", data_structure);
    simple_udp_sendto(&udp_conn, &data_structure, sizeof(data_structure), sender_addr);
    
  } else if (data_received > 1) { // WRITE REQUEST 
    
    if (is_locked == 1 && uip_ipaddr_cmp(sender_addr, &current_lock_holder)) {

      data_structure = data_received; 
  
      is_locked = 0;
      memset(&current_lock_holder, 0, sizeof(current_lock_holder)); 
      etimer_stop(&lock_timer);
      
      LOG_INFO("Write successful. New value: %u. Lock released.\n", data_structure);
      send_response_code(sender_addr, UNLOCK_SUCCESS_CODE); 

    } else {
      LOG_INFO("Write request rejected. Lock not held or not locked.\n");
      send_response_code(sender_addr, ERROR_CODE);
    }
    
  } else {
    LOG_INFO("Unknown command code received: %u\n", data_received);
    send_response_code(sender_addr, ERROR_CODE);
  }
}
/*---------------------------------------------------------------------------*/
PROCESS_THREAD(udp_server_process, ev, data)
{
  PROCESS_BEGIN();


  NETSTACK_ROUTING.root_start();


  simple_udp_register(&udp_conn, UDP_SERVER_PORT, NULL,
                      UDP_CLIENT_PORT, udp_rx_callback);
  

  is_locked = 0;
  memset(&current_lock_holder, 0, sizeof(current_lock_holder));

  while(1) {

    PROCESS_WAIT_EVENT_UNTIL(etimer_expired(&lock_timer));

    if (etimer_expired(&lock_timer)) {

      LOG_INFO("Lock timeout expired! Releasing lock held by ");
      LOG_INFO_6ADDR(&current_lock_holder);
      LOG_INFO_(".\n");


      is_locked = 0;
      memset(&current_lock_holder, 0, sizeof(current_lock_holder));

    }
  }

  PROCESS_END();
}
/*---------------------------------------------------------------------------*/