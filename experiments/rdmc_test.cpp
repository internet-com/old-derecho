#include <iostream>
#include <memory>
#include <map>
#include <time.h>

#include "../rdmc/util.h"
#include "../rdmc/message.h"
#include "../rdmc/verbs_helper.h"
#include "../rdmc/rdmc.h"
#include "../rdmc/microbenchmarks.h"
#include "../rdmc/group_send.h"

using std::cout;
using std::endl;
using std::cin;

uint32_t node_rank;
uint32_t num_nodes;
std::map <uint32_t, std::string> node_addresses;

int main (int argc, char *argv[]) {
  query_addresses(node_addresses, node_rank);
  num_nodes = node_addresses.size();

  // initialize RDMA resources, input number of nodes, node rank and ip addresses and create TCP connections
  rdmc::initialize(node_addresses, node_rank);

  // set block size
  const size_t block_size = atoll(argv[1]);
  // all the nodes are in the group
  const uint16_t group_size = num_nodes;
  // size of one message
  long long int msg_size = atoll(argv[2]);
  // size of the buffer
  const size_t buffer_size = msg_size;
  // buffer for the message - received here by the receivers and generated here by the sender
  std::unique_ptr<char[]> buffer(new char[buffer_size]);
  auto mr = std::make_shared<rdma::memory_region>(buffer.get(), buffer_size);

  // create the vector of members - node 0 is the sender
  vector<uint32_t> members(group_size);
  for(uint32_t i = 0; i < group_size; i++) {
    members[i] = i;
  }

  int count = 0;
  int num_messages = 1000;
  // type of send algorithm
  rdmc::send_algorithm type = rdmc::BINOMIAL_SEND;  
  // create the group
  rdmc::create_group(
		     0, members, block_size, type,
		     [&](size_t length) -> rdmc::receive_destination {
		       return {mr, 0};
		     },
		     [&count](char *data, size_t size) {
		       ++count;
		     },
		     [](optional<uint32_t>){});
  // code for the receivers
  if(node_rank > 0) {
    while (count < num_messages) {
    }
  }
  // sender code
  else {
    struct timespec start_time, end_time;
    clock_gettime(CLOCK_REALTIME, &start_time);
    for (int i = 0; i < num_messages; ++i) {
      // send the message
      rdmc::send(0, mr, 0, msg_size);
      while (count <= i) {
      }
    }
    clock_gettime(CLOCK_REALTIME, &end_time);
    long long int nanoseconds_elapsed = (end_time.tv_sec-start_time.tv_sec) * (long long int)1e9 + (end_time.tv_nsec-start_time.tv_nsec);
    cout << (num_messages*msg_size*(long long int)8+0.0)/nanoseconds_elapsed << endl;
  }
}
