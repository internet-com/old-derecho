/*
 * multiple senders send multiple random messages of random size to the group
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
using derecho::DerechoGroup;
using derecho::DerechoRow;

constexpr int MAX_GROUP_SIZE = 8;

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

  
  long long unsigned int buffer_size = 10000;
  long long unsigned int block_size = 10;

  int num_messages = 1000;
  
  auto stability_callback = [] (int sender_id, long long int index, char *buf, long long int msg_size) {cout << "Sender: " << sender_id << ", index: " << index << endl;};
  
  std::shared_ptr<sst::SST<DerechoRow<MAX_GROUP_SIZE>, sst::Mode::Writes>> derecho_sst =
          std::make_shared<sst::SST<DerechoRow<8>, sst::Mode::Writes>>(members, node_rank);
  DerechoGroup<MAX_GROUP_SIZE> g (members, node_rank, derecho_sst, buffer_size, block_size, stability_callback);

  for (int i = 0; i < num_messages; ++i) {
    // random message size between 1 and 100
    unsigned int msg_size = (rand()%7 + 2) * 10;
    char *buf = g.get_position (msg_size);
    while (!buf) {
      buf= g.get_position (msg_size);
    }
    for (unsigned int j = 0; j < msg_size; ++j) {
      buf[j] = 'a'+i;
    }
    g.send();
  }
  while (true) {
  }
  
  return 0;
}
