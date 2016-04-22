#include <vector>

#include "../derecho_group.h"
#include "initialize.h"

using namespace sst;
using std::cout;
using std::endl;
using std::cin;
using std::vector;

int main () {
  uint32_t node_rank;
  uint32_t num_nodes;

  initialize(node_rank, num_nodes);
  
  vector <int> members(num_nodes);
  for (int i = 0; i < (int)num_nodes; ++i) {
    members[i] = i;
  }

  long long unsigned int buffer_size = 100;
  long long unsigned int block_size = 10;

  auto stability_callback = [] (int sender_id, long long int index, char *buf, long long int msg_size) {cout << "Some message is stable" << endl;};
  
  derecho::derecho_group g (members, node_rank, buffer_size, block_size, stability_callback);

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
    g.print();
  }
  
  return 0;
}
