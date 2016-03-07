#ifndef DERECHO_GROUP_H
#define DERECHO_GROUP_H

#include <functional>
#include <queue>

#include "rdmc/rdmc.h"
#include "sst/sst.h"

namespace derecho {
  typedef std::function<void (int, long long int, long long int, long long int)> send_recv_callback;
  typedef std::function<void (int, long long int, long long int, long long int)> stability_callback;
  
  // combines sst and rdmc to give an abstraction of a group where anyone can send
  // template parameter is for the group size - used for the SST row-struct
  template <int N>
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
    vector <long long int> start, end;
    // index of messages - one for each sender
    vector <long long int> index;
    // callbacks for message send/recv and stability
    send_recv_callback k0_callback;
    stability_callback k1_callback;
    // memory regions wrapping the buffers for RDMA ops
    vector <shared_ptr<memory_region> > mrs;
    // queue for address and size of sent/received messages
    // pushed on successful send/receive by RDMC and popped before calling K0_callback by SST
    queue <char*, size_t> send_recv_queue;
    queue <char*, size_t> stability_queue;
    
    struct Row {
      long long int seq_num[N];
    };
    
    SST_writes<Row> *sst;    
    
  public:
    // buffers to store incoming/outgoing messages
    // it is public so that data can be generated from outside directly into the send buffer
    // eliminating the need for memory copy
    vector <unique_ptr<char[]> > buffers;
    // the constructor - takes the list of members, send parameters (block size, buffer size), K0 and K1 callbacks
    derecho_group (vector <int> _members, long long int _buffer_size, long long int _block_size, send_recv_callback _k0_callback, stability_callback _k1_callback, rdmc::type _type = rdmc::BINOMIAL_SEND);
    // get a position in the buffer before sending
    long long int get_position (long long int size);
    // send the message in the last position returned by the last successful call to get_position
    // note that get_position and send are called one after the another - regexp for using the two is (get_position.send)*
    // this still allows making multiple send calls without acknowledgement; at a single point in time, however, there is only one message per sender in the RDMC pipeline
    void send ();
  };

  template <int N>
  derecho_group (vector <int> _members, long long int _buffer_size, long long int _block_size, send_recv_callback _k0_callback, stability_callback _k1_callback, rdmc::type _type = rdmc::BINOMIAL_SEND) {
    // copy the parameters
    members = _members;
    num_members = members.size();
    // find the member_index
    for (int i = 0; i < num_members; ++i) {
      if (members[i] == rdmc::node_rank) {
	member_index = i;
	break;
      }
    }
    block_size = _block_size;
    buffer_size = _buffer_size;
    type = _type;

    // initialize start, end and indexes
    start.resize(num_members);
    end.resize(num_members);
    index.resize(num_members);
    for (int i = 0; i < num_members; ++i) {
      start[i] = end[i] = 0;
      index[i] = -1;
    }
    
    // rotated list of members - used for creating n internal RDMC groups
    vector <int> rotated_members (num_members);

    // create num_members groups one at a time
    for (int i = 0; i < num_members; ++i) {
      /* members[i] is the sender for the i^th group
       * for now, we simply rotate the members vector to supply to create_group
       * even though any arrangement of receivers in the members vector is possible
       */
      // allocate buffer for the group
      unique_ptr<char[]> buffer(new char[buffer_size]);
      buffers.push_back (std::move (buffer));
      // create a memory region encapsulating the buffer
      shared_ptr<memory_region> mr = make_shared<memory_region>(buffers[i].get(), buffer_size);
      mrs.push_back (mr);
      for (int j = 0; j < num_members; ++j) {
	rotated_members[j] = members[(i+j)%num_members];
      }
      // i is the group number
      // receive desination checks if the message will exceed the buffer length at current position in which case it returns the beginning position
      rdmc::create_group(i, rotated_members, block_size, type,
			 [&mrs, i, &start, &buffer_size](size_t length) -> rdmc::receive_destination {
			   return {mrs[i], (buffer_size-start[i] < length)? 0:start[i]}
			 },
			 [i, &index](char *data, size_t size){
			   send_recv_queue.push (pair <char*, size_t> (data, size));
			   index[i]++;
			 },
			 [](optional<uint32_t>){});
    }

    // create the SST writes table
    sst = new SST_writes<Row> (members, rdmc::node_rank);
    for (int i = 0; i < num_members; ++i) {
      for (int j = 0; j < N; ++j) {
	(*sst)[i].seq_num = -1;
      }
    }
    sst->sync_with_members();
    // register message send/recv predicates and stability predicates
    int local_index = -1;
    for (int i = 0; i < num_members; ++i) {
      auto send_recv_pred = [&index=index[i], local_index] (SST_writes <Row> *sst) mutable {
	if (index > local_index) {
	  local_index++;
	  return true;
	}
      };
      auto send_recv_trig = [&index=index[i], i, local_index, &k0_callback, &send_recv_queue, &member_index] (SST_writes <Row> *sst) mutable {
	local_index++;
	auto p = send_recv_queue.front();
	send_recv_queue.pop();
	k0_callback (i, local_index, p.first, p.second);
	// update SST, so that the sender can see the receipt of the message
	(*sst)[member_index].seq_num[i]++;
	stability_queue.push (p);
      };
      sst->predicates.insert (send_recv_pred, send_recv_trig, PredicateType::RECURRENT);
    }
    int stable = -1;
    auto stability_pred = [stable, &num_members, &member_index] (SST_writes <Row> *sst) mutable {
      int min_msg = (*sst)[member_index].seq_num[member_index];
      // minimum of message number received
      for (int i = 0; i < num_members; ++i) {
	if ((*sst)[i].seq_num[member_index] < min_msg) {
	  min_msg = (*sst)[i].seq_num[member_index];
	}
      }
      if (min_msg > stable) {
	stable++;
	return true;
      }
      return false;
    };
    auto stability_trig = [&index=index[member_index], stable, &k1_callback, &buffer_size] (SST_writes <Row> *sst) mutable {
      auto p = stability_queue.front();
      stability_queue.pop();
      k1_callback (member_index, stable, p.first, p.second);
      ++stable;
      if (buffer_size - end[member_index] <= p.second) {
	end[member_index] = p.second;
      }
      else {
	end[member_index] += p.second;
      }
    }
  }

  template <int N>
  void derecho_group<int>::send () {
    rdmc::send(member_index, mrs[member_index], msg_offset, msg_size);
    msg_offset += msg_size;
    if (msg_offset >= buffer_size) {
      msg_offset = 0;
    }
  }
}

#endif /* DERECHO_GROUP_H
