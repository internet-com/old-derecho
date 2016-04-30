#pragma once

#include <cassert>
#include <algorithm>
#include <chrono>
#include <thread>

#include "derecho_group.h"


namespace derecho {

using std::vector;
using std::map;
using std::mutex;
using std::unique_lock;
using std::lock_guard;
using std::cout;
using std::endl;

/**
 *
 * @param _members A list of node IDs of members in this group
 * @param node_rank The rank (ID) of this node in the group
 * @param _sst The SST this group will use; created by the GMS (membership
 * service) for this group.
 * @param _buffer_size The size of the message buffer to be created for RDMC
 * sending/receiving (there will be one for each sender in the group)
 * @param _block_size The block size to use for RDMC
 * @param _global_stability_callback The function to call locally when a message
 * is globally stable
 * @param _type The type of RDMC algorithm to use
 * @param _window_size The message window size (number of outstanding messages
 * that can be in progress at once before blocking sends).
 */
template<unsigned int N>
DerechoGroup<N>::DerechoGroup(vector<node_id_t> _members, node_id_t my_node_id, std::shared_ptr<sst::SST<DerechoRow<N>, sst::Mode::Writes>> _sst,
        long long unsigned int _max_payload_size, message_callback _global_stability_callback, long long unsigned int _block_size,
        unsigned int _window_size, rdmc::send_algorithm _type) :
            members(_members), num_members(members.size()), block_size(_block_size), max_msg_size(compute_max_msg_size(_max_payload_size, _block_size)),
            type(_type), window_size(_window_size), global_stability_callback(_global_stability_callback), rdmc_group_num_offset(0),
            wedged(false),thread_shutdown(false), background_threads(), sst(_sst) {
    // find my member_index
    for (int i = 0; i < num_members; ++i) {
        if (members[i] == my_node_id) {
            member_index = i;
            break;
        }
    }
    assert(window_size >= 1);

    send_slot = 0;
    recv_slots.resize(num_members,0);

    // rotated list of members - used for creating n internal RDMC groups
    vector<uint32_t> rotated_members(num_members);

    // create num_members groups one at a time
    for (int i = 0; i < num_members; ++i) {
        /* members[i] is the sender for the i^th group
         * for now, we simply rotate the members vector to supply to create_group
         * even though any arrangement of receivers in the members vector is possible
         */
        // allocate buffer for the group
        std::unique_ptr<char[]> buffer(new char[max_msg_size*window_size]);
        buffers.push_back(std::move(buffer));
        // create a memory region encapsulating the buffer
        std::shared_ptr<rdma::memory_region> mr = std::make_shared<rdma::memory_region>(buffers[i].get(),max_msg_size*window_size);
        mrs.push_back(mr);
        for (int j = 0; j < num_members; ++j) {
            rotated_members[j] = members[(i + j) % num_members];
        }
        // When RDMC receives a message, it should store it in locally_stable_messages and update the received count
        auto rdmc_receive_handler =
                [this, i](char* data, size_t size) {
                    assert(this->sst);
                    assert(&sst == &(this->sst));
                    cout << "In receive handler, SST has " << sst->get_num_rows() << " rows; member_index is " << member_index << endl;
                    cout << "Received message from sender " << i << ", index = " << (*sst)[member_index].nReceived[i]+1 << endl;
                    lock_guard <mutex> lock (msg_state_mtx);
                    header* h = (header*) data;
                    (*sst)[member_index].nReceived[i]++;
                    locally_stable_messages[(*sst)[member_index].nReceived[i]*num_members + i] = {i,
                            (*sst)[member_index].nReceived[i], (long long unsigned int) (data-buffers[i].get()), size};
                    for (unsigned int j = 0; j < h->pause_sending_turns; ++j) {
                        (*sst)[member_index].nReceived[i]++;
                        locally_stable_messages[(*sst)[member_index].nReceived[i]*num_members + i] = {i, (*sst)[member_index].nReceived[i], 0, 0};
                    }
                    auto* min_ptr = std::min_element(std::begin((*sst)[member_index].nReceived), &(*sst)[member_index].nReceived[num_members]);
                    int index = std::distance(std::begin((*sst)[member_index].nReceived), min_ptr);
                    auto new_seq_num = (*min_ptr + 1) * num_members + index-1;
                    if (new_seq_num > (*sst)[member_index].seq_num) {
                        (*sst)[member_index].seq_num = new_seq_num;
//                        sst->put (offsetof (DerechoRow<N>, seq_num), sizeof (new_seq_num));
                        sst->put();
                    } else {
//                        size_t size_nReceived = sizeof((*sst)[member_index].nReceived[i]);
//                        sst->put(offsetof(DerechoRow<N>, nReceived) + i * size_nReceived, size_nReceived);
                        sst->put();
                    }
                };
        //Capture rdmc_receive_handler by copy! The reference to it won't be valid after this constructor ends!
        auto receive_handler_plus_notify =
                [this, rdmc_receive_handler](char* data, size_t size) {
                    rdmc_receive_handler(data, size);
                    //signal background writer thread
                    derecho_cv.notify_one();
                };
        // i is the group number
        // receive destination checks if the message will exceed the buffer length at current position in which case it returns the beginning position
        if (i == member_index) { 
            //In the group in which this node is the sender, we need to signal the writer thread 
            //to continue when we see that one of our messages was delivered.
            rdmc::create_group(i + rdmc_group_num_offset, rotated_members, block_size, type,
                    [this, i](size_t length) -> rdmc::receive_destination {
                        auto offset = max_msg_size * recv_slots[i];
                        recv_slots[i] = (recv_slots[i]+1)%window_size;
                        return {mrs[i],offset};
                    },
                    receive_handler_plus_notify,
                    [](boost::optional<uint32_t>) {});
        } else {
            rdmc::create_group(i + rdmc_group_num_offset, rotated_members, block_size, type,
                    [this, i](size_t length) -> rdmc::receive_destination {
                        auto offset = max_msg_size * recv_slots[i];
                        recv_slots[i] = (recv_slots[i]+1)%window_size;
                        return {mrs[i],offset};
                    }, 
                    rdmc_receive_handler, 
                    [](boost::optional<uint32_t>) {});
        }
    }
    
    // create the SST writes table
//    sst = new sst::SST<DerechoRow<N>, sst::Mode::Writes>(members, node_rank);
    cout << "Initializing DerechoGroup" << endl;
    for (int i = 0; i < num_members; ++i) {
        for(int j = 0; j < num_members; ++j) {
            (*sst)[i].nReceived[j] = -1;
        }
        (*sst)[i].seq_num = -1;
        (*sst)[i].stable_num = -1;
        (*sst)[i].delivered_num = -1;
    }
    sst->put();
    sst->sync_with_members();

    cout << "In DerechoGroup constructor, SST has " << sst->get_num_rows() << " rows; member_index is " << member_index << endl;

    auto stability_pred = [this] (const sst::SST <DerechoRow<N>, sst::Mode::Writes> & sst) {
        return !wedged;
    };
    auto stability_trig = [this] (sst::SST <DerechoRow<N>, sst::Mode::Writes> & sst) {
        // compute the min of the seq_num
        long long int min_seq_num = sst[0].seq_num;
        for (int i = 0; i < num_members; ++i) {
            if (sst[i].seq_num < min_seq_num) {
                min_seq_num = sst[i].seq_num;
            }
        }
        if (min_seq_num > sst[member_index].stable_num) {
            sst[member_index].stable_num = min_seq_num;
//            sst.put (offsetof (DerechoRow<N>, stable_num), sizeof (min_seq_num));
            sst.put ();
        }
    };
    sst->predicates.insert(stability_pred, stability_trig, sst::PredicateType::RECURRENT);

    auto delivery_pred = [this] (const sst::SST <DerechoRow<N>, sst::Mode::Writes> & sst) {
        return !wedged;
    };
    auto delivery_trig = [this] (sst::SST <DerechoRow<N>, sst::Mode::Writes> & sst) {
        lock_guard <mutex> lock (msg_state_mtx);
        // compute the min of the stable_num
        long long int min_stable_num = sst[0].stable_num;
        for (int i = 0; i < num_members; ++i) {
            if (sst[i].stable_num < min_stable_num) {
                min_stable_num = sst[i].stable_num;
            }
        }

        if (!locally_stable_messages.empty()) {
            long long int least_undelivered_seq_num = locally_stable_messages.begin()->first;
            if (least_undelivered_seq_num <= min_stable_num) {
                msg_info msg = locally_stable_messages.begin()->second;
                if (msg.size > 0) {
                    char* buf = buffers[msg.sender_id].get() + msg.offset;
                    header* h = (header*) buf;
                    global_stability_callback (msg.sender_id, msg.index, buf + h->header_size, msg.size);
                }
                sst[member_index].delivered_num = least_undelivered_seq_num;
//                sst.put (offsetof (DerechoRow<N>, delivered_num), sizeof (least_undelivered_seq_num));
                sst.put ();
                locally_stable_messages.erase (locally_stable_messages.begin());
            }
        }
    };
    sst->predicates.insert(delivery_pred, delivery_trig, sst::PredicateType::RECURRENT);
    
    auto derecho_pred = [this] (const sst::SST <DerechoRow<N>, sst::Mode::Writes> & sst) {
        long long int seq_num = next_message_to_deliver*num_members + member_index;
        for (int i = 0; i < num_members; ++i) {
            if (sst[i].delivered_num < seq_num) {
                return false;
            }
        }
        return true;
    };
    auto derecho_trig = [this] (sst::SST <DerechoRow<N>, sst::Mode::Writes> & sst) {
        derecho_cv.notify_one ();
        next_message_to_deliver++;
    };
    sst->predicates.insert(derecho_pred, derecho_trig, sst::PredicateType::RECURRENT);
    
    // start sender thread
    background_threads.emplace_back(std::thread(&DerechoGroup::send_loop, this));

    cout << "DerechoGroup: Registered predicates and started thread" << endl;

}

template<unsigned int N>
DerechoGroup<N>::DerechoGroup(std::vector<node_id_t> _members, node_id_t my_node_id, std::shared_ptr<sst::SST<DerechoRow<N>, sst::Mode::Writes>> _sst,
        const DerechoGroup& old_group) : DerechoGroup(_members, my_node_id, _sst, old_group.max_msg_size - sizeof(header),
                old_group.global_stability_callback, old_group.block_size, old_group.window_size, old_group.type) {}

template<unsigned int N>
DerechoGroup<N>::~DerechoGroup() {
    sst->disable_all_predicates();
    thread_shutdown = true;
    for (int i = 0; i < num_members; ++i) {
        rdmc::destroy_group(i + rdmc_group_num_offset);
    }
    derecho_cv.notify_all();
    cout << "DerechoGroup: Done destroying RDMC groups" << endl;
    for(auto& thread : background_threads) {
        thread.join();
    }
}

template<unsigned int N>
long long unsigned int DerechoGroup<N>::compute_max_msg_size(const long long unsigned int max_payload_size, const long long unsigned int block_size) {
    auto max_msg_size = max_payload_size + sizeof(header);
    if (max_msg_size % block_size != 0) {
        max_msg_size = (max_msg_size / block_size + 1) * block_size;
    }
    return max_msg_size;
}

template<unsigned int N>
void DerechoGroup<N>::wedge() {
    wedged = true;
}

template<unsigned int N>
void DerechoGroup<N>::send_loop() {
    auto should_send = [&]() {
        if (pending_sends.empty () || wedged) {
            return false;
        }
        msg_info msg = pending_sends.front ();
        if ((*sst)[member_index].nReceived[member_index] < msg.index-1) {
            return false;
        }

        for (int i = 0; i < num_members; ++i) {
            if ((*sst)[i].delivered_num < (msg.index-window_size)*num_members + member_index) {
                return false;
            }
        }
        return true;
    };
    auto should_wake = [&](){
        return thread_shutdown || should_send();
    };
    unique_lock<mutex> lock(msg_state_mtx);
    while (!thread_shutdown) {
        derecho_cv.wait(lock, should_wake);
        if (!thread_shutdown){
            msg_info msg = pending_sends.front();
            cout << "DerechoGroup sending message " << msg.index << endl;
            debug_print();
            rdmc::send(member_index + rdmc_group_num_offset, mrs[member_index], msg.offset, msg.size);
            pending_sends.pop();
        }
    }
    cout <<  "DerechoGroup send thread shutting down" << endl;
}

template<unsigned int N>
char* DerechoGroup<N>::get_position(long long unsigned int payload_size, int pause_sending_turns) {
    long long unsigned int msg_size = payload_size + sizeof(header);
    if (msg_size > max_msg_size) {
        cout << "Can't send messages of size larger than the maximum message size which is equal to " << max_msg_size << endl;
        return NULL;
    }
    for (int i = 0; i < num_members; ++i) {
        if ((*sst)[i].delivered_num < (future_message_index-window_size)*num_members + member_index) {
            return NULL;
        }
    }
    auto pos = max_msg_size*send_slot;
    send_slot = (send_slot+1)%window_size;
    next_message = {member_index, future_message_index, pos, msg_size};
    char* buf = buffers[member_index].get()+pos;
    ((header*)buf)->header_size = sizeof(header);
    ((header*)buf)->pause_sending_turns = pause_sending_turns;
    future_message_index += pause_sending_turns+1;
    return buf+sizeof(header);
}

template<unsigned int N>
void DerechoGroup<N>::send() {
    {
        lock_guard<mutex> lock(msg_state_mtx);
        assert(next_message);
        pending_sends.push(*next_message);
        next_message = boost::none;
    }
    derecho_cv.notify_one();
}

template<unsigned int N>
  void DerechoGroup<N>::debug_print () {
    cout << "In DerechoGroup SST has " << sst->get_num_rows() << " rows; member_index is " << member_index << endl;
    cout << "Printing SST" << endl;
    for (int i = 0; i < num_members; ++i) {
      cout << (*sst)[i].seq_num << " " << (*sst)[i].stable_num << " " << (*sst)[i].delivered_num << endl;
    }
    cout << endl;

    cout << "Printing last_received_messages" << endl;
    for (int i = 0; i < num_members; ++i) {
      cout << (*sst)[member_index].nReceived[i] << " " << endl;
    }
    cout << endl;
  }

} //namespace derecho

