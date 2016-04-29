#ifndef MANAGED_GROUP_H_
#define MANAGED_GROUP_H_

#include <mutex>
#include <list>
#include <string>
#include <utility>
#include <map>

#include "view.h"
#include "rdmc/connection.h"

namespace derecho {

template<typename T>
struct LockedQueue {
    private:
        std::mutex mutex;
        using lock_t = std::unique_lock<std::mutex>;
        std::list<T> underlying_list;
    public:
        struct LockedListAccess {
            private:
                lock_t lock;
            public:
                std::list<T> &access;
                LockedListAccess(std::mutex& m, std::list<T>& a) :
                    lock(m), access(a) {};
        };
        LockedListAccess locked() {
            return LockedListAccess{ mutex, underlying_list };
        }
};

class ManagedGroup {
    private:

        /** Maps node IDs (what RDMC/SST call "ranks") to IP addresses.
         * Currently, this mapping must be completely known at startup. */
        std::map<node_id_t, std::string> member_ips_by_id;

        /** Contains client sockets for all pending joins, except the current one.*/
        LockedQueue<tcp::socket> pending_joins;

        /** The socket connected to the client that is currently joining, if any */
        tcp::socket joining_client_socket;
        /** The node ID that has been assigned to the client that is currently joining, if any. */
        node_id_t joining_client_id;
		/** A cached copy of the last known value of this node's suspected[] array.
		 * Helps the SST predicate detect when there's been a change to suspected[].*/ 
		std::vector<bool> last_suspected;

		/** The port that this instance of the GMS communicates on. */
        const int gms_port;

        /** Indicates whether the GMS wants to disable all of its predicates except "suspected_changed" */
        std::atomic<bool> preds_disabled;
        /** A flag to signal background threads to shut down; set to true when the group is destroyed. */
        std::atomic<bool> thread_shutdown;
        /** Holds references to background threads, so that we can shut them down during destruction. */
        std::vector<std::thread> background_threads;

        /** Sends a joining node the new view that has been constructed to include it.*/
        void commit_join(const View& new_view, tcp::socket& client_socket);

        bool has_pending_join(){
            return pending_joins.locked().access.size() > 0;
        }

        /** Assuming this node is the leader, handles a join request from a client.*/
        void receive_join(tcp::socket& client_socket);

        /** Starts a new Derecho group with this node as the only member, and initializes the GMS. */
        static std::unique_ptr<View> start_group(const ip_addr& my_ip);
        /** Joins an existing Derecho group, initializing this object to participate in its GMS. */
        static std::unique_ptr<View> join_existing(const ip_addr& leader_ip, const int leader_port);

        //Ken's helper methods
        static void deliver_in_order(const View& Vc, int Leader);
        static void await_meta_wedged(const View& Vc);
        static int await_leader_globalMin_ready(const View& Vc);
        static void leader_ragged_edge_cleanup(View& Vc);
        static void follower_ragged_edge_cleanup(View& Vc);

        static bool suspected_not_equal(const View::DerechoSST& gmsSST, const std::vector<bool> old);
        static void copy_suspected(const View::DerechoSST& gmsSST, std::vector<bool>& old);
        static bool changes_contains(const View::DerechoSST& gmsSST, const node_id_t q);
        static int min_acked(const View::DerechoSST& gmsSST, const std::vector<bool>& failed);

		/** Constructor helper method to encapsulate creating all the predicates. */
		void register_predicates();

        /** Creates the SST and derecho_group for the current view, using the current view's member list.
         * The parameters are all the possible parameters for constructing derecho_group. */
        void setup_sst_and_rdmc(long long unsigned int _max_payload_size, message_callback global_stability_callback,
                long long unsigned int _block_size, unsigned int _window_size, rdmc::send_algorithm _type);
        /** Sets up the SST and derecho_group for a new view, based on the settings in the current view
         * (and copying over the SST data from the current view). */
        void transition_sst_and_rdmc(View& newView, int whichFailed);

        /** May hold a pointer to the partially-constructed next view, if we are
         * in the process of transitioning to a new view. */
        std::unique_ptr<View> next_view;

    public:
        std::unique_ptr<View> curr_view; //must be a pointer so we can re-assign it

        /** Constructor, starts or joins a managed Derecho group.
         * If my_ip == leader_ip, starts a new group with this node as the leader.
         * The rest of the parameters are the parameters for the derecho_group that should
         * be constructed for communications within this managed group. */
        ManagedGroup(const int gms_port, const ip_addr& my_ip, const ip_addr& leader_ip,
                long long unsigned int _max_payload_size, message_callback global_stability_callback, long long unsigned int _block_size,
                unsigned int _window_size = 3, rdmc::send_algorithm _type = rdmc::BINOMIAL_SEND);

        ~ManagedGroup();
        /** Causes this node to cleanly leave the group by setting itself to "failed." */
        void leave();

        /** Reports to the GMS that the given node has failed. */
        void report_failure(const node_id_t who);
        /** Gets a reference to the current derecho_group for the group being managed.
         * Clients can use this to send and receive messages. */
        DerechoGroup<View::N>& current_derecho_group();

};

} /* namespace derecho */

#endif /* MANAGED_GROUP_H_ */
