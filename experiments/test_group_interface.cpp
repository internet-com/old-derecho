#include <vector>

#include "../derecho_group.h"
#include "../rdmc/util.h"
#include "../rdmc/message.h"
#include "../rdmc/verbs_helper.h"
#include "../rdmc/rdmc.h"
#include "../rdmc/microbenchmarks.h"
#include "../rdmc/group_send.h"
#include "../sst/sst.h"
#include "../sst/tcp.h"

using namespace sst;
using sst::tcp::tcp_initialize;
using sst::tcp::sync;
using std::cout;
using std::endl;
using std::cin;
using std::vector;

int main () {
  uint32_t node_rank;
  uint32_t num_nodes;
  vector<std::string> node_addresses;

  query_addresses(node_addresses, node_rank);
  num_nodes = node_addresses.size();

  // initialize RDMA resources, input number of nodes, node rank and ip addresses and create TCP connections
  rdmc::initialize(node_addresses, node_rank);

  // initialize tcp connections
  tcp_initialize(num_nodes, node_rank, node_addresses);
  
  // initialize the rdma resources
  verbs_initialize();
  
  vector <int> members(num_nodes);
  for (int i = 0; i < (int)num_nodes; ++i) {
    members[i] = i;
  }

  long long int buffer_size = 100;
  long long int block_size = 10;

  auto k0_callback = [] (int sender_id, long long int index, char *buf, long long int msg_size) {cout << "Received a message" << endl;};
  auto k1_callback = [] (int sender_id, long long int index, char *buf, long long int msg_size) {cout << "Some message is stable" << endl;};
  
  derecho::derecho_group<2> g (members, node_rank, buffer_size, block_size, k0_callback, k1_callback);

  cout << "Derecho group created" << endl;

  int msg_size = 5;
  long long int pos = g.get_position (msg_size);
  cout << "pos is " << pos << endl;
  for (int i = 0; i < msg_size; ++i) {
    g.buffers[node_rank][i] = 'a';
  }
  cout << "Calling send" << endl;
  g.send(pos, msg_size);
  cout << "send call finished" << endl;
  while (true) {
    
  }
  
  return 0;
}
