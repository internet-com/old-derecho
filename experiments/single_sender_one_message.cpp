/*
 * a single sender sends a single random message to the group
 * the message is verified at the receivers
 */

#include <iostream>
#include <vector>
#include <time.h>

#include "../derecho_group.h"
#include "../rdmc/util.h"
#include "../rdmc/message.h"
#include "../rdmc/verbs_helper.h"
#include "../rdmc/rdmc.h"
#include "../rdmc/microbenchmarks.h"
#include "../rdmc/group_send.h"
#include "../sst/sst.h"
#include "../sst/tcp.h"

using std::cout;
using std::endl;
using std::cin;
using std::vector;

int main () {
  srand(time(NULL));
  
  uint32_t node_rank;
  uint32_t num_nodes;
  map<uint32_t, std::string> node_addresses;

  query_addresses(node_addresses, node_rank);
  num_nodes = node_addresses.size();

  // initialize RDMA resources, input number of nodes, node rank and ip addresses and create TCP connections
  rdmc::initialize(node_addresses, node_rank);

  // initialize tcp connections
  sst::tcp::tcp_initialize(node_rank, node_addresses);
  
  // initialize the rdma resources
  sst::verbs_initialize();
  
  vector <int> members(num_nodes);
  for (int i = 0; i < (int)num_nodes; ++i) {
    members[i] = i;
  }

  
  long long unsigned int buffer_size = 100;
  long long unsigned int block_size = 10;

  auto stability_callback = [] (int sender_id, long long int index, char *buf, long long int msg_size) {
    cout << "Delivered a message" << endl;
    cout << "The message is:" << endl;
    for (int i = 0; i < msg_size; ++i) {
      cout << buf[i];
    }
    cout << endl;
  };
  
  derecho::DerechoGroup g (members, node_rank, buffer_size, block_size, stability_callback);

  cout << "Derecho group created" << endl;

  if (node_rank == 0) {
    unsigned int msg_size = 10;
    char* buf = g.get_position (msg_size);
    for (unsigned int i = 0; i < msg_size; ++i) {
      buf[i] = rand ()%26 + 'a';
    }
    cout << "Calling send" << endl;
    g.send();
    cout << "send call finished" << endl;
  }
  while (true) {
    
  }
  
  return 0;
}
