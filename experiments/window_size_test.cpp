#include <iostream>
#include <fstream>
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
#include "block_size.h"

using std::cout;
using std::endl;
using std::cin;
using std::vector;

int main (int argc, char *argv[]) {
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
  
  // -_- -_- -_- -_- -_-
  vector<int> sst_members (num_nodes);
  for (int i = 0; i < (int)num_nodes; ++i) {
    sst_members[i] = i;
  }

  long long unsigned int msg_size = atoll(argv[1]);
  unsigned int window_size = atoll(argv[2]);
  long long unsigned int block_size = get_block_size (msg_size);
  long long unsigned int buffer_size = msg_size * window_size;
  cout << "buffer_size=" << buffer_size << ", block_size=" << block_size << ", msg_size=" << msg_size << endl;
  int num_messages = 1000;
  
  bool done = false;
  auto stability_callback = [&num_messages, &done] (int sender_id, long long int index, char *buf, long long int msg_size) {
    if (index == num_messages-1) {
      cout << "Done" << endl;
      done = true;
    }
  };
  
  derecho::derecho_group g (members, node_rank, buffer_size, block_size, stability_callback, rdmc::BINOMIAL_SEND, window_size);

  struct timespec start_time;
  // start timer
  clock_gettime(CLOCK_REALTIME, &start_time);
  for (int i = 0; i < num_messages; ++i) {
    char* buf = g.get_position (msg_size);
    while (!buf) {
      buf = g.get_position (msg_size);
    }
    g.send();
  }
  while (!done) {
    
  }
  struct timespec end_time;
  clock_gettime(CLOCK_REALTIME, &end_time);
  long long int nanoseconds_elapsed = (end_time.tv_sec-start_time.tv_sec)*(long long int)1e9 + (end_time.tv_nsec-start_time.tv_nsec);
  double bw = (msg_size * (long long int) num_messages * (long long int) 8 + 0.0)/nanoseconds_elapsed;
  struct Result {
    double bw;
  };
  sst::SST<Result, sst::Mode::Writes> *sst = new sst::SST<Result, sst::Mode::Writes> (sst_members, node_rank);
  (*sst)[node_rank].bw = bw;
  sst->put();
  sst->sync_with_members();
  double total_bw = 0.0;
  for (unsigned int i = 0; i < num_nodes; ++i) {
    total_bw += (*sst)[i].bw;
  }
  std::ofstream fout;
  std::string filename = "data_window_size";
  fout.open(filename, std::ofstream::app);
  fout << msg_size << " " << window_size << " " << total_bw << endl;
  fout.close();  
}
