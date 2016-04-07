#include "derecho_group.h"
#include <vector>
#include <map>

namespace derecho {
  derecho_group::derecho_group (vector <int> _members, int node_rank, long long int _buffer_size, long long int _block_size, message_callback _local_stability_callback, message_callback _global_stability_callback, rdmc::send_algorithm _type, int _window_size) {
    // copy the parameters
    members = _members;
    num_members = members.size();
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
    window_size = _window_size;

    local_stability_callback = _local_stability_callback;
    global_stability_callback = _global_stability_callback;

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
      mrs.push_back (mr);
      for (int j = 0; j < num_members; ++j) {
	rotated_members[j] = (uint32_t) members[(i+j)%num_members];
      }
      // i is the group number
      // receive desination checks if the message will exceed the buffer length at current position in which case it returns the beginning position
      rdmc::create_group(i, rotated_members, block_size, type,
			 [this, i](size_t length) -> rdmc::receive_destination {
			   return {mrs[i], (buffer_size-start[i] < (long long int) length)? 0:(size_t)start[i]};
			 },
			 [this, i](char *data, size_t size) {
			   static long long int index[N];
			   insert_in_map(i, index[i], RDMC_RECEIVED, data, size);
			   if (i == member_index) {
			     insert_in_map(i, index[i]+1, READY_TO_SEND, 0, 0);
			   }
			   index[i]++;
			 },
			 [](boost::optional<uint32_t>){});
    }
    
    // create the SST writes table
    sst = new sst::SST_writes<Row> (members, node_rank);
    for (int i = 0; i < num_members; ++i) {
      (*sst)[i].seq_num = -1;
      (*sst)[i].stable_num = -1;
    }
    sst->put();
    sst->sync_with_members();

    auto send_message_pred = [this] (sst::SST_writes <Row> *sst) {
      static long long int index = 0;      
      if (search_in_map(member_index, index, GENERATED) && search_in_map(member_index, index, READY_TO_SEND)) {
	index++;
	return true;
      }
      return false;
    };
    auto send_message_trig = [this] (sst::SST_writes <Row> *sst) {
      static long long int index = 0; 
      auto p = access_from_map(member_index, index, GENERATED);
      erase_from_map (member_index, index, GENERATED);
      erase_from_map (member_index, index, READY_TO_SEND);
      char *buf = p.first;
      long long int msg_size = p.second;
      index++;
      rdmc::send(member_index, mrs[member_index], buf-mrs[member_index]->buffer, msg_size);
    };
    sst->predicates.insert (send_message_pred, send_message_trig, sst::PredicateType::RECURRENT);

    // register message send/recv predicates and stability predicates
    for (int i = 0; i < num_members; ++i) {
      auto send_recv_pred = [this, i] (sst::SST_writes <Row> *sst) {
	static long long int index[N];
	if (search_in_map(i, index[i], RDMC_RECEIVED)) {
	  index[i]++;
	  return true;
	}
	return false;
      };
      auto send_recv_trig = [this, i] (sst::SST_writes <Row> *sst) {
	static long long int index[N];
	auto p = access_from_map(i, index[i], RDMC_RECEIVED);
	erase_from_map (i, index[i], RDMC_RECEIVED);
	char *buf = p.first;
	long long int msg_size = p.second;
	local_stability_callback (i, index[i], buf, msg_size);
	// update SST, so that the sender can see the receipt of the message
	(*sst)[member_index].seq_num[i]++;
	// sst->put(offsetof (Row, seq_num[0]) + i*sizeof (((Row*) 0)->seq_num[0]), sizeof (((Row*) 0)->seq_num[0]));
	sst->put();
	if (i == member_index) {
	  insert_in_map (i, index[i], K0_PROCESSED, buf, msg_size);
	}
	index[i]++;
      };
      sst->predicates.insert (send_recv_pred, send_recv_trig, sst::PredicateType::RECURRENT);
    }
    auto stability_pred = [this] (sst::SST_writes <Row> *sst) {
      static long long int index = 0;      
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
    auto stability_trig = [this] (sst::SST_writes <Row> *sst) {
      static long long int index = 0;      
      auto p = access_from_map(member_index, index, K0_PROCESSED);
      char *buf = p.first;
      long long int msg_size = p.second;
      erase_from_map (member_index, index, K0_PROCESSED);
      global_stability_callback (member_index, index, buf, msg_size);
      if (buffer_size - end[member_index] <= msg_size) {
	end[member_index] = msg_size;
      }
      else {
	end[member_index] += msg_size;
      }
      index++;
    };
    sst->predicates.insert (stability_pred, stability_trig, sst::PredicateType::RECURRENT);

    insert_in_map (member_index, 0, READY_TO_SEND, 0, 0);
  }

  char* derecho_group::get_position (long long int msg_size) {
    // if the size of the message is greater than the buffer size, then return a -1 without thinking
    if (msg_size > buffer_size) {
      cout << "Can't send messages of size larger than the size of the circular buffer" << endl;
      return NULL;
    }
    if (start[member_index] == end[member_index]) {
      start[member_index] = end[member_index] = 0;
    }
    long long int my_start = start[member_index];
    long long int my_end = end[member_index];
    if (my_start < my_end) {
      if (my_end - my_start >= msg_size) {
	start[member_index] += msg_size;
	if (start[member_index] == buffer_size) {
	  start[member_index] = 0;
	}
	next_message = {member_index, future_message_index++, my_start, msg_size};
	return buffers[member_index].get() + my_start;
      }
      else {
	return NULL;
      }
    }
    else {
      if (buffer_size - my_start >= msg_size) {
	start[member_index] += msg_size;
	if (start[member_index] == buffer_size) {
	  start[member_index] = 0;
	}
	next_message = {member_index, future_message_index++, my_start, msg_size};
	return buffers[member_index].get() + my_start;
      }
      else if (my_end >= msg_size) {
	start[member_index] = msg_size;
	if (start[member_index] == buffer_size) {
	  start[member_index] = 0;
	}
	next_message = {member_index, future_message_index++, 0, msg_size};
	return buffers[member_index].get();
      }
      else {
	return NULL;
      }
    }
  }

  void derecho_group::send () {
    unique_lock <mutex> lock (msg_state_mtx);
    assert (next_message);
    pending_sends.push_back (*next_message);
    next_message = boost::none;
  }
}

