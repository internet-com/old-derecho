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

  
  long long int buffer_size = 100;
  long long int block_size = 10;

  auto k0_callback = [] (int sender_id, long long int index, char *buf, long long int msg_size) {
    cout << "Received a message" << endl;
    cout << "The message is:" << endl;
    for (int i = 0; i < msg_size; ++i) {
      cout << buf[i];
    }
    cout << endl;
  };
  auto k1_callback = [] (int sender_id, long long int index, char *buf, long long int msg_size) {cout << "Some message is stable" << endl;};
  
  derecho::derecho_group<2> g (members, node_rank, buffer_size, block_size, k0_callback, k1_callback);

  cout << "Derecho group created" << endl;

  int num_messages = 20;
  if (node_rank == 0) {
    for (int i = 0; i < num_messages; ++i) {
      // random message size between 1 and 100
      int msg_size = rand()%100+1;
      long long int pos = g.get_position (msg_size);
      while (pos < 0) {
	pos = g.get_position (msg_size);
      }
      cout << "pos is " << pos << endl;
      for (int i = 0; i < msg_size; ++i) {
	g.buffers[node_rank][i] = rand ()%26 + 'a';
      }
      cout << "Calling send" << endl;
      g.send();
      cout << "send call finished" << endl;
    }
  }
  while (true) {
    
  }
  
  return 0;
}
