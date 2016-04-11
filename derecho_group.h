#ifndef DERECHO_GROUP_H
#define DERECHO_GROUP_H

#include <functional>
#include <boost/optional.hpp>
#include <mutex>
#include <condition_variable>
#include <tuple>
#include <map>
#include <set>
#include <queue>
#include <vector>
#include <cassert>
#include <memory>

#include "rdmc/rdmc.h"
#include "sst/sst.h"

using std::cout;
using std::endl;
using std::vector;
using std::pair;

namespace derecho {
  typedef std::function<void (int, long long int, char*, long long int)> message_callback;
  
  enum MESSAGE_STATUS {
    BEING_GENERATED,
    GENERATED,
    READY_TO_SEND,
    RDMC_RECEIVED,
    K0_PROCESSED
  };

  struct Row {
    long long int seq_num;
    long long int stable_num;
    long long int delivered_num;
  };

  struct msg_info {
    int sender_id;
    long long int index;
    long long unsigned int offset;
    long long unsigned int size;
  };
  
  // combines sst and rdmc to give an abstraction of a group where anyone can send
  // template parameter is for the group size - used for the SST row-struct
  class derecho_group {
    // number of members
    int num_members;
    // vector of member id's
    vector <int> members;
    // index of the local node into the members vector
    int member_index;
    // block size used for message transfer
    // we keep it simple; one block size for messages from all senders
    long long unsigned int block_size;
    // size of the circular buffer
    long long unsigned int buffer_size;
    // send algorithm for constructing a multicast from point-to-point unicast
    // binomial pipeline by default
    rdmc::send_algorithm type;
    unsigned int window_size;
    // callback for when a message is globally stable
    message_callback global_stability_callback;

    // pointers for each circular buffer - buffer from start to end-1 (with possible wrap around) is free
    vector <long long unsigned int> start, end;
    
    // buffers to store incoming/outgoing messages
    vector <std::unique_ptr<char[]> > buffers;
    // memory regions wrapping the buffers for RDMA ops
    vector <std::shared_ptr<rdma::memory_region> > mrs;
    
    // index to be used the next time get_position is called
    // when next_message is not none, then next_message.index = future_message_index-1
    long long int future_message_index = 0;
    // next_message is the message that will be sent when send is called the next time
    // it is boost::none when there is no message to send
    boost::optional <msg_info> next_message;
    std::vector <long long int> msg_nums;
    std::queue <msg_info> pending_sends;
    std::queue <msg_info> sends_in_pipeline;
    std::map <long long int, msg_info> locally_stable_messages;
    std::mutex msg_state_mtx;
    std::condition_variable derecho_cv;

    sst::SST<Row, sst::Mode::Writes> *sst;    

    void send_loop ();
  public:
    // the constructor - takes the list of members, send parameters (block size, buffer size), K0 and K1 callbacks
    derecho_group (vector <int> _members, int node_rank, long long unsigned int _buffer_size, long long unsigned int _block_size, message_callback global_stability_callback, rdmc::send_algorithm _type = rdmc::BINOMIAL_SEND, unsigned int _window_size = 3);
    // get a position in the buffer before sending
    char* get_position (long long unsigned int msg_size);
    // note that get_position and send are called one after the another - regexp for using the two is (get_position.send)*
    // this still allows making multiple send calls without acknowledgement; at a single point in time, however, there is only one message per sender in the RDMC pipeline
    void send ();

    void sst_print ();
  };
}
#endif /* DERECHO_GROUP_H */
