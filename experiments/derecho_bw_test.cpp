#include <iostream>
#include <fstream>
#include <vector>
#include <time.h>

#include "../derecho_group.h"
#include "block_size.h"
#include "aggregate_bandwidth.h"
#include "log_results.h"
#include "initialize.h"

using std::vector;

int main (int argc, char *argv[]) {
  srand(time(NULL));
  
  uint32_t node_rank;
  uint32_t num_nodes;
  
  initialize(node_rank, num_nodes);
  
  vector <int> members(num_nodes);
  for (int i = 0; i < (int)num_nodes; ++i) {
    members[i] = i;
  }

  int window_size = 3;
  long long unsigned int msg_size = atoll(argv[1]);
  long long unsigned int block_size = get_block_size (msg_size);
  long long unsigned int buffer_size = msg_size * window_size;
  int num_messages = 1000;
  
  bool done = false;
  auto stability_callback = [&num_messages, &done, &num_nodes] (int sender_id, long long int index, char *buf, long long int msg_size) {
    cout << "In stability callback; sender = " << sender_id << ", index = " << index << endl;
    if (index == num_messages-1 && sender_id == (int)num_nodes-1) {
      done = true;
    }
  };
  
  derecho::derecho_group g (members, node_rank, buffer_size, block_size, stability_callback);

  struct timespec start_time;
  // start timer
  clock_gettime(CLOCK_REALTIME, &start_time);
  for (int i = 0; i < num_messages; ++i) {
    char* buf = g.get_position (msg_size);
    while (!buf) {
      cout << "in here" << endl;
      buf = g.get_position (msg_size);
    }
    g.send();
  }
  while (!done) {
    int n;
    std::cin >> n;
    g.print();
  }
  struct timespec end_time;
  clock_gettime(CLOCK_REALTIME, &end_time);
  long long int nanoseconds_elapsed = (end_time.tv_sec-start_time.tv_sec)*(long long int)1e9 + (end_time.tv_nsec-start_time.tv_nsec);
  double bw = (msg_size * num_messages * num_nodes * 8 + 0.0)/nanoseconds_elapsed;
  double avg_bw = aggregate_bandwidth(members, node_rank, bw);
  log_results(msg_size, avg_bw, "data_derecho_bw");
}
