#include <iostream>
#include <fstream>
#include <memory>
#include <map>
#include <time.h>

#include "../rdmc/util.h"
#include "../rdmc/message.h"
#include "../rdmc/verbs_helper.h"
#include "../rdmc/rdmc.h"
#include "../rdmc/microbenchmarks.h"
#include "../rdmc/group_send.h"
#include "../sst/sst.h"
#include "../sst/verbs.h"

using std::cout;
using std::endl;
using std::cin;
using std::vector;

uint32_t node_rank;
uint32_t num_nodes;
std::map <uint32_t, std::string> node_addresses;

size_t get_block_size (long long int msg_size) {
  switch (msg_size) {
  case 10:
  case 100:
  case 1000:
    return msg_size;
  case 10000:
    return 5000;
  case 100000:
  case 1000000:
    return 100000;
  case 10000000:
  case 100000000:
  case 1000000000:
    return 1000000;
  default:
    cout << "Not handled" << endl;
    exit (0);
  }
}

int main (int argc, char *argv[]) {
  query_addresses(node_addresses, node_rank);
  num_nodes = node_addresses.size();

  // initialize RDMA resources, input number of nodes, node rank and ip addresses and create TCP connections
  rdmc::initialize(node_addresses, node_rank);

  // initialize tcp connections
  sst::tcp::tcp_initialize(node_rank, node_addresses);
  
  // initialize the rdma resources
  sst::verbs_initialize();
  
  // size of one message
  long long int msg_size = atoll(argv[1]);
  // set block size
  const size_t block_size = get_block_size (msg_size);
  // size of the buffer
  const size_t buffer_size = msg_size;

  // create the vector of members - node 0 is the sender
  vector<uint32_t> members(num_nodes);
  for(uint32_t i = 0; i < num_nodes; i++) {
    members[i] = i;
  }

  // -_- -_- -_- -_- -_-
  vector<int> sst_members (num_nodes);
  for (int i = 0; i < (int)num_nodes; ++i) {
    sst_members[i] = i;
  }

  vector<std::unique_ptr<char[]>> buffers;
  vector<std::shared_ptr<rdma::memory_region>> mrs;

  vector<int> counts(num_nodes, 0);
  int num_messages = 1000;
  // type of send algorithm
  rdmc::send_algorithm type = rdmc::BINOMIAL_SEND;

  vector <uint32_t> rotated_members(num_nodes);
  for (unsigned int i = 0; i < num_nodes; ++i) {
    for (unsigned int j = 0; j < num_nodes; ++j) {
      rotated_members[j] = (uint32_t) members[(i+j)%num_nodes];
    }
    // buffer for the message - received here by the receivers and generated here by the sender
    std::unique_ptr<char[]> buffer(new char[buffer_size]);
    auto mr = std::make_shared<rdma::memory_region>(buffer.get(), buffer_size);
    buffers.push_back (std::move (buffer));
    mrs.push_back (mr);
    
    // create the group
    rdmc::create_group(
  		       i, rotated_members, block_size, type,
  		       [&mrs, i](size_t length) -> rdmc::receive_destination {
  			 return {mrs[i], 0};
  		       },
  		       [&counts, i](char *data, size_t size) {
  			 ++counts[i];
  		       },
  		       [](optional<uint32_t>){});
  }

  struct timespec start_time, end_time;
  clock_gettime(CLOCK_REALTIME, &start_time);
  for (int i = 0; i < num_messages; ++i) {
    // send the message
    rdmc::send(node_rank, mrs[node_rank], 0, msg_size);
    while (counts[node_rank] <= i) {
    }
  }
  clock_gettime(CLOCK_REALTIME, &end_time);
  long long int nanoseconds_elapsed = (end_time.tv_sec-start_time.tv_sec) * (long long int)1e9 + (end_time.tv_nsec-start_time.tv_nsec);
  double bw = ((long long int)num_messages*msg_size*(long long int)8+0.0)/nanoseconds_elapsed;
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
  std::string filename = "data_rdmc_bw";
  fout.open(filename, std::ofstream::app);
  fout << msg_size << " " << total_bw << endl;
  fout.close();  
}
