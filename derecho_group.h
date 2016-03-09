#ifndef DERECHO_GROUP_H
#define DERECHO_GROUP_H

#include <functional>
#include <queue>
#include <cassert>
#include <memory>

#include "rdmc/rdmc.h"
#include "sst/sst.h"

namespace derecho {
  typedef std::function<void (int, long long int, char*, long long int)> send_recv_callback;
  typedef std::function<void (int, long long int, char*, long long int)> stability_callback;
  
  // combines sst and rdmc to give an abstraction of a group where anyone can send
  // template parameter is for the group size - used for the SST row-struct
  template <int N>
  class derecho_group {
    // number of members
    int num_members;
    // vector of member id's
    std::vector <int> members;
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
    std::vector <long long int> start, end;
    // index of messages - one for each sender
    std::vector <long long int> index;
    // callbacks for message send/recv and stability
    send_recv_callback k0_callback;
    stability_callback k1_callback;
    // memory regions wrapping the buffers for RDMA ops
    std::vector <std::shared_ptr<rdma::memory_region> > mrs;
    // queue for address and size of sent/received messages
    // pushed on successful send/receive by RDMC and popped before calling K0_callback by SST
    std::vector <std::queue <std::pair <char*, size_t>>> send_recv_queue;
    std::queue <std::pair <char*, size_t>> stability_queue;
    
    struct Row {
      long long int seq_num[N];
    };
    
    sst::SST_writes<Row> *sst;    
    
  public:
    // buffers to store incoming/outgoing messages
    // it is public so that data can be generated from outside directly into the send buffer
    // eliminating the need for memory copy
    std::vector <std::unique_ptr<char[]> > buffers;
    // the constructor - takes the list of members, send parameters (block size, buffer size), K0 and K1 callbacks
    derecho_group (std::vector <int> _members, int node_rank, long long int _buffer_size, long long int _block_size, send_recv_callback _k0_callback, stability_callback _k1_callback, rdmc::send_algorithm _type = rdmc::BINOMIAL_SEND);
    // get a position in the buffer before sending
    long long int get_position (long long int msg_size);
    // note that get_position and send are called one after the another - regexp for using the two is (get_position.send)*
    // this still allows making multiple send calls without acknowledgement; at a single point in time, however, there is only one message per sender in the RDMC pipeline
    void send (long long int buffer_offset, long long int msg_size);
  };

  template <int N>
  derecho_group<N>::derecho_group (std::vector <int> _members, int node_rank, long long int _buffer_size, long long int _block_size, send_recv_callback _k0_callback, stability_callback _k1_callback, rdmc::send_algorithm _type) {
    // copy the parameters
    members = _members;
    num_members = members.size();
    assert (num_members == N);
    // find the member_index
    for (int i = 0; i < num_members; ++i) {
      if (members[i] == node_rank) {
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

    send_recv_queue.resize(num_members);
    
    // rotated list of members - used for creating n internal RDMC groups
    std::vector <uint32_t> rotated_members (num_members);

    // create num_members groups one at a time
    for (int i = 0; i < num_members; ++i) {
      /* members[i] is the sender for the i^th group
       * for now, we simply rotate the members vector to supply to create_group
       * even though any arrangement of receivers in the members vector is possible
       */
      // allocate buffer for the group
      std::unique_ptr<char[]> buffer(new char[buffer_size]);
      buffers.push_back (std::move (buffer));
      // create a memory region encapsulating the buffer
      std::shared_ptr<rdma::memory_region> mr = std::make_shared<rdma::memory_region>(buffers[i].get(), buffer_size);
      mrs.push_back (mr);
      for (int j = 0; j < num_members; ++j) {
	rotated_members[j] = (uint32_t) members[(i+j)%num_members];
      }
      // i is the group number
      // receive desination checks if the message will exceed the buffer length at current position in which case it returns the beginning position
      rdmc::create_group(i, rotated_members, block_size, type,
			 [&mr=this->mrs[i], &start=this->start[i], &buffer_size=this->buffer_size](size_t length) -> rdmc::receive_destination {
			   return {mr, (buffer_size-start < (long long int) length)? 0:(size_t)start};
			 },
			 [&index=this->index[i], &send_recv_queue=this->send_recv_queue[i]](char *data, size_t size){
			   send_recv_queue.push (std::pair <char*, size_t> (data, size));
			   index++;
			 },
			 [](boost::optional<uint32_t>){});
    }

    std::cout << "RDMC groups created" << std::endl;

    // create the SST writes table
    sst = new sst::SST_writes<Row> (members, node_rank);
    for (int i = 0; i < num_members; ++i) {
      for (int j = 0; j < num_members; ++j) {
	(*sst)[i].seq_num[j] = -1;
      }
    }
    sst->sync_with_members();
    // register message send/recv predicates and stability predicates
    int local_index = -1;
    for (int i = 0; i < num_members; ++i) {
      auto send_recv_pred = [&index=this->index[i], local_index] (sst::SST_writes <Row> *sst) mutable {
	if (index > local_index) {
	  local_index++;
	  return true;
	}
	return false;
      };
      auto send_recv_trig = [i, local_index, &k0_callback=this->k0_callback, &send_recv_queue=this->send_recv_queue[i], &member_index=this->member_index, &stability_queue=this->stability_queue] (sst::SST_writes <Row> *sst) mutable {
	local_index++;
	auto p = send_recv_queue.front();
	char *buf = p.first;
	long long int msg_size = p.second;
	send_recv_queue.pop();
	k0_callback (i, local_index, buf, msg_size);
	// update SST, so that the sender can see the receipt of the message
	(*sst)[member_index].seq_num[i]++;
	stability_queue.push (p);
      };
      sst->predicates.insert (send_recv_pred, send_recv_trig, sst::PredicateType::RECURRENT);
    }
    int stable = -1;
    auto stability_pred = [stable, &num_members=this->num_members, &member_index=this->member_index] (sst::SST_writes <Row> *sst) mutable {
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
    auto stability_trig = [stable, &k1_callback=this->k1_callback, &buffer_size=this->buffer_size, member_index=this->member_index, &stability_queue=this->stability_queue, &end=this->end[member_index]] (sst::SST_writes <Row> *sst) mutable {
      auto p = stability_queue.front();
      char *buf = p.first;
      long long int msg_size = p.second;
      stability_queue.pop();
      k1_callback (member_index, stable, buf, msg_size);
      stable++;
      if (buffer_size - end <= msg_size) {
	end = msg_size;
      }
      else {
	end += msg_size;
      }
    };
    sst->predicates.insert (stability_pred, stability_trig, sst::PredicateType::RECURRENT);
  }

  template <int N>
  long long int derecho_group<N>::get_position (long long int msg_size) {
    long long int my_start = start[member_index];
    long long int my_end = end[member_index];
    if (my_start < my_end) {
      if (my_end - my_start >= msg_size) {
	start[member_index] += msg_size;
	return my_start;
      }
      else {
	return -1;
      }
    }
    else {
      if (buffer_size - my_start >= msg_size) {
	start[member_index] += msg_size;
	return my_start;
      }
      else if (my_end >= msg_size) {
	start[member_index] = msg_size;
	return 0;
      }
      else {
	return -1;
      }
    }
  }
  
  template <int N>
  void derecho_group<N>::send (long long int buffer_offset, long long int msg_size) {
    rdmc::send(member_index, mrs[member_index], buffer_offset, msg_size);
  }
}

#endif /* DERECHO_GROUP_H */
