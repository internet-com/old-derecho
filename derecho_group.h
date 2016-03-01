#ifndef DERECHO_GROUP_H
#define DERECHO_GROUP_H

#include "rdmc/rdmc.h"

namespace derecho {
  // combines sst and rdmc to give an abstraction of a group where anyone can send
  class derecho_group {
    // number of members
    int num_members;
    // vector of member id's
    vector <int> members;
    // index of the local node into the members vector
    int member_index;
    // block size used for message transfer
    // we keep it simple; one block size for messages from all senders
    long long int block_size;
    // size of the circular buffer
    long long int buffer_size;
    // send algorithm for constructing a multicast from point-to-point unicast
    // binomial pipeline by default
    rdmc::send_algorithm type;
    // pointers for each circular buffer - buffer from start to end-1 (with possible wrap around) is free
    vector <int> start, end;
    // memory regions wrapping the buffers for RDMA ops
    vector <shared_ptr<memory_region> > mrs;

  public:
    // buffers to store incoming/outgoing messages
    // it is public so that data can be generated from outside directly into the send buffer
    // eliminating the need for memory copy
    vector <unique_ptr<char[]> > buffers;
    // the constructor - takes the list of members, send parameters (block size, buffer size), K0 and K1 callbacks
    derecho_group (vector <int> _members, long long int _buffer_size, long long int _block_size, send_recv_callback k0_callback, stability_callback k1_callback, rdmc::type _type = rdmc::BINOMIAL_SEND);
    // get a position in the buffer before sending
    long long int get_position (long long int size);
    // send the message in the last position returned by the last successful call to get_position
    // note that get_position and send are called one after the another - regexp for using the two is (get_position.send)*
    // note that this allows making multiple send calls without acknowledgement; at a single point in time, however, there is only one message per sender in the RDMC pipeline
    void send ();
  };
}

#endif /* DERECHO_GROUP_H
