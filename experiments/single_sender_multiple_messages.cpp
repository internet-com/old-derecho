/*
 * a single sender sends multiple random messages of random size to the group
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
  vector<std::string> node_addresses;

  query_addresses(node_addresses, node_rank);
  num_nodes = node_addresses.size();

  // initialize RDMA resources, input number of nodes, node rank and ip addresses and create TCP connections
  rdmc::initialize(node_addresses, node_rank);

  // initialize tcp connections
  sst::tcp::tcp_initialize(num_nodes, node_rank, node_addresses);
  
  // initialize the rdma resources
  sst::verbs_initialize();
  
  vector <int> members(num_nodes);
  for (int i = 0; i < (int)num_nodes; ++i) {
    members[i] = i;
  }

  
  long long int buffer_size = 10000;
  long long int block_size = 10;

  auto k0_callback = [] (int sender_id, long long int index, char *buf, long long int msg_size) {
    cout << "Message " << index << " by node " << sender_id << " of size " << msg_size << " is received " << endl;
  };
  auto k1_callback = [] (int sender_id, long long int index, char *buf, long long int msg_size) {
    cout << "Message " << index << " by node " << sender_id << " of size " << msg_size << " is stable " << endl;
  };
  
  derecho::derecho_group<2> g (members, node_rank, buffer_size, block_size, k0_callback, k1_callback);

  int num_messages = 100;
  if (node_rank == 0) {
    for (int i = 0; i < num_messages; ++i) {
      // random message size between 1 and 100
      int msg_size = (rand()%7 + 2) * 10;
      long long int pos = g.get_position (msg_size);
      while (pos < 0) {
	pos = g.get_position (msg_size);
      }
      for (int j = 0; j < msg_size; ++j) {
	g.buffers[node_rank][pos+j] = rand ()%26 + 'a';
      }
      g.send();
    }
  }
  while (true) {
    
  }
  
  return 0;
}
