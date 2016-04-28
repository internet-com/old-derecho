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
#include <memory>

#include "derecho_row.h"
#include "rdmc/rdmc.h"
#include "sst/sst.h"

namespace derecho {

using std::vector;

typedef std::function<void(int, long long int, char*, long long int)> message_callback;

struct __attribute__ ((__packed__)) header {
        uint32_t header_size;
        uint32_t pause_sending_turns;
};

struct msg_info {
        int sender_id;
        long long int index;
        long long unsigned int offset;
        long long unsigned int size;
};

/**
 * SST row state variables needed to track message completion status in this group.
 */
struct MessageTrackingRow {
        /** Sequence numbers are interpreted like a row-major pair:
         * (sender, counter) becomes sender + num_members * counter.
         * Since the global order is round-robin, the correct global order of
         * messages becomes a consecutive sequence of these numbers: with 4
         * senders, we expect to receive (0,0), (1,0), (2,0), (3,0), (0,1),
         * (1,1), ... which is 0, 1, 2, 3, 4, 5, ....
         *
         * This variable is the highest sequence number that has been received
         * in-order by this node; if a node updates seq_num, it has received all
         * messages up to seq_num in the global round-robin order. */
        long long int seq_num;
        /** This represents the highest sequence number that has been received
         * by every node, as observed by this node. If a node updates stable_num,
         * then it believes that all messages up to stable_num in the global
         * round-robin order have been received by every node. */
        long long int stable_num;
        /** This represents the highest sequence number that has been delivered
         * at this node. Messages are only delievered once stable, so it must be
         * at least stable_num. */
        long long int delivered_num;
};

/** combines sst and rdmc to give an abstraction of a group where anyone can send
 * template parameter is the maximum possible group size - used for the GMS SST row-struct */
template<unsigned int N>
class DerechoGroup {
        /** vector of member id's */
        std::vector<int> members;
        /**  number of members */
        int num_members;
        /** index of the local node in the members vector, which should also be its row index in the SST */
        int member_index;
        /** Block size used for message transfer.
         * we keep it simple; one block size for messages from all senders */
        long long unsigned int block_size;
        // maximum size of any message that can be sent
        long long unsigned int max_msg_size;
        /** Send algorithm for constructing a multicast from point-to-point unicast.
         *  Binomial pipeline by default. */
        rdmc::send_algorithm type;
        unsigned int window_size;
        /** callback for when a message is globally stable */
        message_callback global_stability_callback;
        /** Indicates whether this sending group is paused pending a reconfiguration.
         * Once wedged, no more messages will be sent or delivered in this group.
         * Atomic because it's shared with the background sender thread. */
        std::atomic<bool> wedged;

        int send_slot;
        vector<int> recv_slots;
        /** buffers to store incoming/outgoing messages */
        std::vector<std::unique_ptr<char[]> > buffers;
        /** memory regions wrapping the buffers for RDMA ops */
        std::vector<std::shared_ptr<rdma::memory_region> > mrs;

        /** Index to be used the next time get_position is called.
         * When next_message is not none, then next_message.index = future_message_index-1 */
        long long int future_message_index = 0;
        /** next_message is the message that will be sent when send is called the next time.
         * It is boost::none when there is no message to send. */
        boost::optional<msg_info> next_message;
//        /** last_received_messages[i] is the largest index of the message received from sender i */
//        std::vector<long long int> last_received_messages;
        std::queue<msg_info> pending_sends;
        std::map<long long int, msg_info> locally_stable_messages;
        long long int next_message_to_deliver = 0;
        std::mutex msg_state_mtx;
        std::condition_variable derecho_cv;

        /** A flag to signal background threads to shut down; set to true when the group is destroyed. */
        std::atomic<bool> thread_shutdown;
        /** Holds references to background threads, so that we can shut them down during destruction. */
        std::vector<std::thread> background_threads;

        /** Helper variable to indicate that this group has been partially destroyed by calling destroy_rdmc_groups(). */
        bool groups_are_destroyed = false;

        /** The SST, shared between this group and its GMS. */
        std::shared_ptr<sst::SST<DerechoRow<N>, sst::Mode::Writes>> sst;

        void send_loop();
    public:
        // the constructor - takes the list of members, send parameters (block size, buffer size), K0 and K1 callbacks
        DerechoGroup(std::vector<int> _members, int node_rank, std::shared_ptr<sst::SST<DerechoRow<N>, sst::Mode::Writes>> _sst,
                long long unsigned int _max_payload_size, message_callback global_stability_callback, long long unsigned int _block_size,
                unsigned int _window_size = 3, rdmc::send_algorithm _type = rdmc::BINOMIAL_SEND);
        /** Constructor to initialize a new derecho_group from an old one, preserving the same settings but providing a new list of members. */
        DerechoGroup(std::vector<int> _members, int node_rank, std::shared_ptr<sst::SST<DerechoRow<N>, sst::Mode::Writes>> _sst, const DerechoGroup& old_group);
        ~DerechoGroup();
        /** get a pointer into the buffer, to write data into it before sending */
        char* get_position(long long unsigned int payload_size, int pause_sending_turns = 0);
        /** Note that get_position and send are called one after the another - regexp for using the two is (get_position.send)*
         * This still allows making multiple send calls without acknowledgement; at a single point in time, however,
         * there is only one message per sender in the RDMC pipeline */
        void send();
        /** Stops all sending and receiving in this group, in preparation for shutting it down. */
        void wedge();
        /** Tears down all the RDMC groups used in this group, in preparation for destroying it. */
        void destroy_rdmc_groups();
        /** Debugging function; prints the current state of the SST to stdout. */
        void debug_print();
};
} //namespace derecho

#include "derecho_group_impl.h"

#endif /* DERECHO_GROUP_H */
