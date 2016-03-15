#ifndef DERECHO_GROUP_H
#define DERECHO_GROUP_H

#include <functional>
#include <tuple>
#include <cassert>
#include <memory>

#include "rdmc/rdmc.h"
#include "sst/sst.h"

using std::cout;
using std::endl;
using std::vector;
using std::pair;

namespace derecho {
  enum MESSAGE_STATUS {
    BEING_GENERATED,
    GENERATED,
    READY_TO_SEND,
    RDMC_RECEIVED,
    K0_PROCESSED
  };

  typedef std::function<void (int, long long int, char*, long long int)> send_recv_callback;
  typedef std::function<void (int, long long int, char*, long long int)> stability_callback;
  
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
    // callbacks for message send/recv and stability
    send_recv_callback k0_callback;
    stability_callback k1_callback;
    // memory regions wrapping the buffers for RDMA ops
    vector <std::shared_ptr<rdma::memory_region> > mrs;
    // map of <i,index,stage> |-> <buf,size>
    std::map <std::tuple<int, long long int, int>, pair <char*, long long int>> msg_info;
    
    struct Row {
      long long int seq_num[N];
    };
    
    sst::SST_writes<Row> *sst;    

    void initialize_position ();
    void initialize_send ();
    
  public:
    // buffers to store incoming/outgoing messages
    // it is public so that data can be generated from outside directly into the send buffer
    // eliminating the need for memory copy
    vector <std::unique_ptr<char[]> > buffers;
    // the constructor - takes the list of members, send parameters (block size, buffer size), K0 and K1 callbacks
    derecho_group (vector <int> _members, int node_rank, long long int _buffer_size, long long int _block_size, send_recv_callback _k0_callback, stability_callback _k1_callback, rdmc::send_algorithm _type = rdmc::BINOMIAL_SEND);
    // get a position in the buffer before sending
    std::function<long long int (long long int)> get_position;
    // note that get_position and send are called one after the another - regexp for using the two is (get_position.send)*
    // this still allows making multiple send calls without acknowledgement; at a single point in time, however, there is only one message per sender in the RDMC pipeline
    std::function<void ()> send;
  };

  template <int N>
  derecho_group<N>::derecho_group (vector <int> _members, int node_rank, long long int _buffer_size, long long int _block_size, send_recv_callback _k0_callback, stability_callback _k1_callback, rdmc::send_algorithm _type) {
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

    k0_callback = _k0_callback;
    k1_callback = _k1_callback;

    // initialize start, end and indexes
    start.resize(num_members);
    end.resize(num_members);
    for (int i = 0; i < num_members; ++i) {
      start[i] = end[i] = 0;
    }
    
    // rotated list of members - used for creating n internal RDMC groups
    vector <uint32_t> rotated_members (num_members);

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
      cout << "Size of MR is " << mr->size << endl;
      mrs.push_back (mr);
      cout << "Size of MR in the vector is " << mrs[i]->size << endl;
      for (int j = 0; j < num_members; ++j) {
	rotated_members[j] = (uint32_t) members[(i+j)%num_members];
      }
      // i is the group number
      // receive desination checks if the message will exceed the buffer length at current position in which case it returns the beginning position
      rdmc::create_group(i, rotated_members, block_size, type,
			 [i, &start=this->start[i], &buffer_size=this->buffer_size, &mrs=this->mrs](size_t length) -> rdmc::receive_destination {
			   cout << "Size of MR is " << mrs[i]->size << endl;
			   cout << "Length of incoming message is " << length << endl;
			   size_t offset = (buffer_size-start < (long long int) length)? 0:(size_t)start;
			   cout << "Offset is " << offset << endl;
			   return {mrs[i], (buffer_size-start < (long long int) length)? 0:(size_t)start};
			 },
			 [&msg_info=this->msg_info, index=(long long int) 0, i, member_index=this->member_index](char *data, size_t size) mutable {
			   cout << "In completion callback" << endl;
			   msg_info[std::tuple<int, long long int, int> (i, index, RDMC_RECEIVED)] = pair <char*, size_t> (data, size);
			   if (i == member_index) {
			     msg_info[std::tuple<int, long long int, int> (i, index+1, READY_TO_SEND)] = pair <char*, size_t> (0, 0);
			   }
			   index++;
			 },
			 [](boost::optional<uint32_t>){});
    }
    
    cout << "RDMC groups created" << endl;
    
    // create the SST writes table
    sst = new sst::SST_writes<Row> (members, node_rank);
    for (int i = 0; i < num_members; ++i) {
      for (int j = 0; j < num_members; ++j) {
	(*sst)[i].seq_num[j] = -1;
      }
    }
    sst->put();
    sst->sync_with_members();

    auto send_message_pred = [&msg_info=this->msg_info, index=(long long int) 0, member_index=this->member_index] (sst::SST_writes <Row> *sst) mutable {
      if (msg_info.find (std::tuple<int, long long int, int> (member_index, index, GENERATED)) != msg_info.end() && msg_info.find (std::tuple<int, long long int, int> (member_index, index, READY_TO_SEND)) != msg_info.end()) {
	auto p = msg_info[std::tuple<int, long long int, int> (member_index, index, GENERATED)];
	cout << "P is: ";
	cout << p.first << " " << p.second << endl << std::flush;
	index++;
	return true;
      }
      return false;
    };
    auto send_message_trig = [&msg_info=this->msg_info, index=(long long int) 0, member_index=this->member_index, &mrs=this->mrs] (sst::SST_writes <Row> *sst) mutable {
      auto p = msg_info[std::tuple<int, long long int, int> (member_index, index, GENERATED)];
      msg_info.erase (std::tuple<int, long long int, int> (member_index, index, GENERATED));
      msg_info.erase(std::tuple<int, long long int, int> (member_index, index, READY_TO_SEND));
      char *buf = p.first;
      long long int msg_size = p.second;
      index++;
      cout << "Calling RDMC send: message size is " << msg_size << endl << std::flush;
      rdmc::send(member_index, mrs[member_index], buf-mrs[member_index]->buffer, msg_size);
    };
    sst->predicates.insert (send_message_pred, send_message_trig, sst::PredicateType::RECURRENT);

    initialize_position();
    initialize_send();
    
    // register message send/recv predicates and stability predicates
    for (int i = 0; i < num_members; ++i) {
      auto send_recv_pred = [&msg_info=this->msg_info, index=(long long int) 0, i] (sst::SST_writes <Row> *sst) mutable {
	if (msg_info.find (std::tuple<int, long long int, int> (i, index, RDMC_RECEIVED)) != msg_info.end()) {
	  index++;
	  return true;
	}
	return false;
      };
      auto send_recv_trig = [&msg_info=this->msg_info, index=(long long int) 0, i, &member_index=this->member_index, &k0_callback=this->k0_callback] (sst::SST_writes <Row> *sst) mutable {
	cout << "in send recv trigger" << endl;
	auto p = msg_info[std::tuple<int, long long int, int> (i, index, RDMC_RECEIVED)];
	msg_info.erase (std::tuple<int, long long int, int> (i, index, RDMC_RECEIVED));
	char *buf = p.first;
	long long int msg_size = p.second;
	cout << "Calling k0_callback function" << endl;
	k0_callback (i, index, buf, msg_size);
	// update SST, so that the sender can see the receipt of the message
	(*sst)[member_index].seq_num[i]++;
	sst->put(offsetof (Row, seq_num[0]) + i*sizeof (((Row*) 0)->seq_num[0]), sizeof (((Row*) 0)->seq_num[0]));
	if (i == member_index) {
	  msg_info[std::tuple<int, long long int, int> (i, index, K0_PROCESSED)] = p;
	}
	index++;
      };
      sst->predicates.insert (send_recv_pred, send_recv_trig, sst::PredicateType::RECURRENT);
    }
    auto stability_pred = [&msg_info=this->msg_info, index=(long long int) 0, &num_members=this->num_members, &member_index=this->member_index] (sst::SST_writes <Row> *sst) mutable {
      int min_msg = (*sst)[member_index].seq_num[member_index];
      // minimum of message number received
      for (int i = 0; i < num_members; ++i) {
	if ((*sst)[i].seq_num[member_index] < min_msg) {
	  min_msg = (*sst)[i].seq_num[member_index];
	}
      }
      if (min_msg >= index) {
	index++;
	return true;
      }
      return false;
    };
    auto stability_trig = [&msg_info=this->msg_info, index=(long long int) 0, &buffer_size=this->buffer_size, member_index=this->member_index, &end=this->end[member_index], &k1_callback=this->k1_callback] (sst::SST_writes <Row> *sst) mutable {
      cout << "In stability trigger" << endl;
      auto p = msg_info[std::tuple<int, long long int, int> (member_index, index, K0_PROCESSED)];
      char *buf = p.first;
      long long int msg_size = p.second;
      msg_info.erase(std::tuple<int, long long int, int> (member_index, index, K0_PROCESSED));
      cout << "Calling k1_callback function" << endl;
      k1_callback (member_index, index, buf, msg_size);
      if (buffer_size - end <= msg_size) {
	end = msg_size;
      }
      else {
	end += msg_size;
      }
      index++;
    };
    sst->predicates.insert (stability_pred, stability_trig, sst::PredicateType::RECURRENT);

    msg_info[std::tuple<int, long long int, int> (member_index, 0, READY_TO_SEND)] = pair <char*, long long int> (0,0);
  }

  template <int N>
  void derecho_group<N>::initialize_position () {
    get_position = [this, index=0] (long long int msg_size) mutable -> long long int {
      // if the size of the message is greater than the buffer size, then return a -1 without thinking
      if (msg_size > buffer_size) {
	cout << "Can't send messages of size larger than the size of the circular buffer" << endl;
	return -1;
      }
      long long int my_start = start[member_index];
      long long int my_end = end[member_index];
      if (my_start == my_end) {
	my_start = 0;
	my_end = 0;
	start[member_index] = 0;
	end[member_index] = 0;
      }
      if (my_start < my_end) {
	if (my_end - my_start >= msg_size) {
	  start[member_index] += msg_size;
	  msg_info[std::tuple<int, long long int, int> (member_index, index, BEING_GENERATED)] = pair <char*, int> (mrs[member_index]->buffer+my_start, msg_size);
	  return my_start;
	}
	else {
	  return -1;
	}
      }
      else {
	if (buffer_size - my_start >= msg_size) {
	  start[member_index] += msg_size;
	  msg_info[std::tuple<int, long long int, int> (member_index, index, BEING_GENERATED)] = pair <char*, int> (mrs[member_index]->buffer+my_start, msg_size);
	  return my_start;
	}
	else if (my_end >= msg_size) {
	  start[member_index] = msg_size;
	  msg_info[std::tuple<int, long long int, int> (member_index, index, BEING_GENERATED)] = pair <char*, int> (mrs[member_index]->buffer, msg_size);
	  return 0;
	}
	else {
	  return -1;
	}
      }
    };
  }

  template <int N>
  void derecho_group<N>::initialize_send () {
    send = [&msg_info=this->msg_info, index=0, member_index=this->member_index] () mutable {
      auto p = msg_info[std::tuple<int, long long int, int> (member_index, index, BEING_GENERATED)];
      msg_info.erase (std::tuple<int, long long int, int> (member_index, index, BEING_GENERATED));
      msg_info[std::tuple<int, long long int, int> (member_index, index, GENERATED)] = p;
      ++index;
    };
  }
}
#endif /* DERECHO_GROUP_H */
