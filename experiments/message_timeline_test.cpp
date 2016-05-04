/*
 * gms_test.cpp
 *
 *  Created on: Apr 26, 2016
 *      Author: edward
 */

#include <iostream>
#include <string>
#include <cstdlib>
#include <map>
#include <time.h>
#include <list>
#include <cassert>
using std::string;
using std::cin;
using std::cout;
using std::endl;
using std::map;
using std::list;

#include "../derecho_group.h"
#include "block_size.h"
#include "../rdmc/util.h"
#include "../managed_group.h"
#include "../view.h"
#include "log.h"

static const int GMS_PORT = 12345;

int main (int argc, char *argv[]) {

  uint32_t server_rank = 0;
  uint32_t num_nodes;
  uint32_t node_rank;

  map<uint32_t, std::string> node_addresses;

  query_addresses(node_addresses, node_rank);
  num_nodes = node_addresses.size();
  long long unsigned int max_msg_size = 1000000;
  long long unsigned int block_size = 100000;

  int num_messages = 1000;
    
  bool done = false;
  auto stability_callback = [&num_messages, &done, &num_nodes] (int sender_rank, long long int index, char *buf, long long int msg_size) {
    // cout << "In stability callback; sender = " << sender_rank << ", index = " << index << endl;
    if (index == num_messages-1 && sender_rank == (int)num_nodes-1) {
      done = true;
    }
  };


  derecho::ManagedGroup managed_group(GMS_PORT, node_addresses, node_rank, server_rank, max_msg_size, stability_callback, block_size);

  cout <<  "Finished constructing/joining ManagedGroup" << endl;

  while(managed_group.get_members().size() < num_nodes) {
  }
  auto members = managed_group.get_members();
  int my_rank = -1;
  for (unsigned int i = 0; i < members.size(); ++i) {
    if (members[i] == node_rank) {
      my_rank = i;
    }
  }
  assert (my_rank >= 0);

  list <msg_status_log> trace;
    
  for (int i = 0; i < num_messages; ++i) {
    // random message size between 1 and 100
    unsigned int msg_size = (rand()%7 + 2) * (max_msg_size / 10);
    // trace.push_back (*(new msg_status_log (CONCEIVED_AT_CLIENT, my_rank, i)));
    char *buf = managed_group.get_sendbuffer_ptr(msg_size);
    //        cout << "After getting sendbuffer for message " << i << endl;
    //        managed_group.debug_print_status();
    while (!buf) {
      buf= managed_group.get_sendbuffer_ptr(msg_size);
    }
    // trace.push_back (*(new msg_status_log (BEING_GENERATED_AT_CLIENT, my_rank, i)));
    for (unsigned int j = 0; j < msg_size; ++j) {
      buf[j] = 'a'+i;
    }
    // trace.push_back (*(new msg_status_log (SENT_AT_CLIENT, my_rank, i)));
    managed_group.send();
  }
  while (!done) {
  }

  trace.sort(compare);
  auto trace2 = managed_group.get_trace();
  trace2.sort(compare);
  trace.merge(trace2, compare);
  
  set_file("message_status");

  print(trace, my_rank);
  
  managed_group.barrier_sync();

  managed_group.leave();
    
  cout << "Finished destroying managed_group" << endl;
}
