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
  const int N = 4;
  
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

  long long int unit = 1000;
  long long int buffer_size = 10*unit*unit*unit;
  long long int block_size = unit;

  long long int msg_size = 100*unit*unit;
  int num_messages = 1000;
  
  bool done = false;
  auto k0_callback = [] (int sender_id, long long int index, char *buf, long long int msg_size) {
  };
  auto k1_callback = [&num_messages, &done] (int sender_id, long long int index, char *buf, long long int msg_size) {
    if (index == num_messages-1) {
      cout << "Done" << endl;
      done = true;
    }
  };
  
  derecho::derecho_group<N> g (members, node_rank, buffer_size, block_size, k0_callback, k1_callback);

  struct timespec start_time;
  // start timer
  clock_gettime(CLOCK_REALTIME, &start_time);
  for (int i = 0; i < num_messages; ++i) {
    long long int pos = g.get_position (msg_size);
    while (pos < 0) {
      pos = g.get_position (msg_size);
    }
    g.send();
  }
  while (!done) {
    
  }
  struct timespec end_time;
  clock_gettime(CLOCK_REALTIME, &end_time);
  long long int nanoseconds_elapsed = (end_time.tv_sec*1e9 + end_time.tv_nsec)- (start_time.tv_sec*1e9 + start_time.tv_nsec);
  cout << (msg_size * num_messages * 8 + 0.0)/nanoseconds_elapsed << endl;
  for (unsigned int i = 0; i < num_nodes; ++i) {
    if (i != node_rank) {
      sst::tcp::sync (members[i]);
    }
  }
  cout << "Exiting" << endl;
  return 0;
}
