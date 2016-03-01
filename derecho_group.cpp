#include "derecho_group.h"

namespace derecho {
  derecho_group::derecho_group (vector <int> _members, size_t _block_size, int _num_out, int _msg_size, rdmc::completion_callback_t send_callback, rdmc::type _type = rdmc::BINOMIAL_SEND) {
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

    // initialize start and end
    start = end = 0;
    
    // rotated list of members - used for creating n internal RDMC groups
    vector <int> rotated_members (num_members);

    // create num_members group one by one
    for (int i = 0; i < num_members; ++i) {
      /* members[i] is the sender for the i^th group
       * for now, we simply rotate the members vector to supply to create_group
       * even though any arrangement of receivers in the members vector is possible
       */
      // allocate buffer for the group
      unique_ptr<char[]> buffer(new char[buffer_size]);
      buffers.push_back (std::move (buffer));
      shared_ptr<memory_region> mr = make_shared<memory_region>(buffers[i].get(), buffer_size);
      mrs.push_back (mr);
      for (int j = 0; j < num_members; ++j) {
	rotated_members[j] = members[(i+j)%num_members];
      }
      // i is the group number
      rdmc::create_group(i, rotated_members, block_size, type,
			 [&](size_t length) -> rdmc::receive_destination {
			   return {mr, (size_t)msg_size*((size_t)cur_msg%num_out)};
			 },
			 send_callback);
      std::tuple<uint16_t, size_t, rdmc::send_algorithm> send_params(num_members, block_size, type);
    }
  }

  void send () {
    rdmc::send(member_index, mrs[member_index], msg_offset, msg_size);
    msg_offset += msg_size;
    if (msg_offset >= buffer_size) {
      msg_offset = 0;
    }
  }
}
