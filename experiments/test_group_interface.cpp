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
  map<uint32_t, std::string> node_addresses;

  query_addresses(node_addresses, node_rank);
  num_nodes = node_addresses.size();

  // initialize RDMA resources, input number of nodes, node rank and ip addresses and create TCP connections
  rdmc::initialize(node_addresses, node_rank);

  // initialize tcp connections
  tcp_initialize(node_rank, node_addresses);
  
  // initialize the rdma resources
  verbs_initialize();
  
  vector <int> members(num_nodes);
  for (int i = 0; i < (int)num_nodes; ++i) {
    members[i] = i;
  }

  long long unsigned int buffer_size = 100;
  long long unsigned int block_size = 10;

  auto stability_callback = [] (int sender_id, long long int index, char *buf, long long int msg_size) {cout << "Some message is stable" << endl;};
  
  derecho::DerechoGroup g (members, node_rank, buffer_size, block_size, stability_callback);

  cout << "Derecho group created" << endl;

  if (node_rank == 0) {
    int msg_size = 50;
    char* buf = g.get_position (msg_size);
    for (int i = 0; i < msg_size; ++i) {
      buf[i] = 'a';
    }
    cout << "Calling send" << endl;
    g.send();
    cout << "send call finished" << endl;
  }
  while (true) {
    int n;
    cin >> n;
    g.sst_print();
  }
  
  return 0;
}
