#ifndef DERECHO_GROUP_H
#define DERECHO_GROUP_H

#include "rdmc/rdmc.h"

namespace derecho {

  class derecho_group {
    int num_members;
    vector <int> members;
    int member_index;
    size_t block_size;
    int num_out;
    int msg_size;
    size_t buffer_size;
    rdmc::send_algorithm type;
    int msg_offset;
    vector <unique_ptr<char[]> > buffers;
    vector <shared_ptr<memory_region> > mrs;

  public:
    derecho_group (vector <int> _members, size_t _block_size, int _num_out, int _msg_size, rdmc::completion_callback_t send_callback); 

    void send ();
  };
}

#endif /* DERECHO_GROUP_H
