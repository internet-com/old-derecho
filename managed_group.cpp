/*
 * group_manager.cpp
 *
 *  Created on: Apr 22, 2016
 *      Author: edward
 */

#include <map>
#include <vector>
#include <memory>
#include <exception>
#include <stdexcept>
#include <iostream>
#include <iterator>
#include <cstring>
#include <atomic>

#include "managed_group.h"

#include "derecho_group.h"
#include "derecho_row.h"
#include "view.h"
#include "view_utils.h"
#include "sst/sst.h"

namespace derecho {

using std::map;
using std::vector;
using std::make_shared;
using std::make_unique;
using std::unique_ptr;
using std::cout;
using std::endl;
using sst::SST;

ManagedGroup::ManagedGroup(const int gms_port, const map<node_id_t, ip_addr>& member_ips, node_id_t my_id, node_id_t leader_id, long long unsigned int _max_payload_size, message_callback global_stability_callback,
        long long unsigned int _block_size, unsigned int _window_size, rdmc::send_algorithm _type) :
        member_ips_by_id(member_ips), last_suspected(View::N), gms_port(gms_port), preds_disabled(false), thread_shutdown(false), background_threads(), next_view(nullptr) {
    cout << "Doing global setup of SST and RDMC" << endl;
    //TODO: pull these out
    rdmc::initialize(member_ips_by_id, my_id);
    sst::tcp::tcp_initialize(my_id, member_ips_by_id);
    sst::verbs_initialize();

    if (my_id != leader_id){
        curr_view = join_existing(member_ips_by_id[leader_id], gms_port);
    } else {
        curr_view = start_group(my_id);
        tcp::connection_listener serversocket(gms_port);
        tcp::socket client_socket = serversocket.accept();
        ip_addr& joiner_ip = client_socket.remote_ip;
        curr_view->num_members++;
        curr_view->member_ips.push_back(joiner_ip);
        for (const auto& entry : member_ips_by_id) {
            if (entry.second == joiner_ip) {
                curr_view->members.push_back(entry.first);
                break;
            }
        }
        curr_view->failed.push_back(false);

        client_socket.write((char*) &curr_view->vid, sizeof(curr_view->vid));
        client_socket.write((char*) &curr_view->num_members, sizeof(curr_view->num_members));
        for (auto nodeID : curr_view->members) {
            client_socket.write((char*) &nodeID, sizeof(nodeID));
        }
        for (auto & str : curr_view->member_ips) {
            //includes null-terminator
            const int str_size = str.size() + 1;
            client_socket.write((char*) &str_size, sizeof(str_size));
            client_socket.write(str.c_str(), str_size);
        }
        for (bool fbool : curr_view->failed) {
            client_socket.write((char*) &fbool, sizeof(fbool));
        }
    }
    curr_view->my_rank = curr_view->rank_of(my_id);
    //Temporarily disabled because all member IP->ID mappings are fixed at startup
//    for(int rank = 0; rank < curr_view->num_members; ++rank) {
//        member_ips_by_id[curr_view->members[rank]] = curr_view->member_ips[rank];
//    }
    
    setup_sst_and_rdmc(_max_payload_size, global_stability_callback, _block_size, _window_size, _type);

    background_threads.emplace_back(std::thread{[&](){
        tcp::connection_listener serversocket(gms_port);
        while (!thread_shutdown)
            pending_joins.locked().access.emplace_back(serversocket.accept());
    }});

    register_predicates();
}

void ManagedGroup::register_predicates() {

    using DerechoSST = View::DerechoSST;

    auto suspected_changed = [this](const DerechoSST& sst) {
        return suspected_not_equal(sst, last_suspected);
    };
    auto suspected_changed_trig = [this](DerechoSST& gmsSST) {
        View& Vc = *curr_view;
        int myRank = curr_view->my_rank;
        //These fields had better be synchronized.
        assert(gmsSST.get_local_index() == curr_view->my_rank);
        // Aggregate suspicions into gmsSST[myRank].Suspected;
        for (int r = 0; r < Vc.num_members; r++)
        {
            for (int who = 0; who < Vc.num_members; who++)
            {
                gmssst::set(gmsSST[myRank].suspected[who], gmsSST[myRank].suspected[who] || gmsSST[r].suspected[who]);
            }
        }

        for (int q = 0; q < Vc.num_members; q++)
        {
            if (gmsSST[myRank].suspected[q] && !Vc.failed[q])
            {
                if (Vc.nFailed + 1 >= Vc.num_members / 2)
                {
                    throw std::runtime_error("Majority of a Derecho group simultaneously failed … shutting down");
                }

                gmsSST.freeze(q); // Cease to accept new updates from q
                Vc.rdmc_sending_group->wedge();
                gmssst::set(gmsSST[myRank].wedged, true); // RDMC has halted new sends and receives in theView
                Vc.failed[q] = true;
                Vc.nFailed++;

                if (Vc.nFailed > Vc.num_members / 2 || (Vc.nFailed == Vc.num_members / 2 && Vc.num_members % 2 == 0))
                {
                    throw std::runtime_error("Potential partitioning event: this node is no longer in the majority and must shut down!");
                }

                gmsSST.put();
                if (Vc.IAmLeader() && !changes_contains(gmsSST, Vc.members[q])) // Leader initiated
                {
                    if ((gmsSST[myRank].nChanges - gmsSST[myRank].nCommitted) == View::N)
                    {
                        throw std::runtime_error("Ran out of room in the pending changes list");
                    }

                    gmssst::set(gmsSST[myRank].changes[gmsSST[myRank].nChanges % View::N], Vc.members[q]); // Reports the failure (note that q NotIn members)
                    gmssst::increment(gmsSST[myRank].nChanges);
                    std::cout << std::string("NEW SUSPICION: adding ") << Vc.members[q] << std::string(" to the CHANGES/FAILED list") << std::endl;
                    gmsSST.put();
                }
            }
        }
        copy_suspected(gmsSST, last_suspected);
    };

    //Only start one join at a time
    auto start_join_pred = [this](const DerechoSST& sst) {
        return !preds_disabled && curr_view->IAmLeader() && has_pending_join() && joining_client_socket.is_empty();
    };
    auto start_join_trig = [this](DerechoSST& sst) {
        cout << "GMS received a new client connection" << endl;
        joining_client_socket = std::move(pending_joins.locked().access.front()); //list.front() is now invalid because sockets are move-only, but C++ leaves it on the list
        pending_joins.locked().access.pop_front(); //because C++ list doesn't properly implement queues, this returns void
        receive_join(joining_client_socket);
    };


    auto change_commit_ready = [this](const DerechoSST& gmsSST) {
        return !preds_disabled && curr_view->my_rank == curr_view->rank_of_leader()
                && min_acked(gmsSST, curr_view->failed) > gmsSST[gmsSST.get_local_index()].nCommitted;
    };
    auto commit_change = [this](DerechoSST& gmsSST) {
        gmsSST[gmsSST.get_local_index()].nCommitted = min_acked(gmsSST, curr_view->failed); // Leader commits a new request
        gmsSST.put();
    };

    auto leader_proposed_change = [this](const DerechoSST& gmsSST) {
        if(curr_view->rank_of_leader() < 0 || curr_view->rank_of_leader() > curr_view->num_members-1) {
            cout << "Error! curr_view->rank_of_leader() returned " << curr_view->rank_of_leader() << endl;
        }
        return !preds_disabled
                && gmsSST[curr_view->rank_of_leader()].nChanges > gmsSST[gmsSST.get_local_index()].nAcked;
    };
    auto ack_proposed_change = [this](DerechoSST& gmsSST) {
        //These fields had better be synchronized.
        assert(gmsSST.get_local_index() == curr_view->my_rank);
        int myRank = gmsSST.get_local_index();
        int leader = curr_view->rank_of_leader();
        wedge_view(*curr_view);
        if (myRank != leader)
        {
            gmssst::set(gmsSST[myRank].changes, gmsSST[leader].changes); // Echo (copy) the vector including the new changes
            gmssst::set(gmsSST[myRank].joiner_ip, gmsSST[leader].joiner_ip); //Echo the new member's IP
            gmssst::set(gmsSST[myRank].nChanges, gmsSST[leader].nChanges); // Echo the count
            gmssst::set(gmsSST[myRank].nCommitted, gmsSST[leader].nCommitted);
        }

        gmssst::set(gmsSST[myRank].nAcked, gmsSST[leader].nChanges); // Notice a new request, acknowledge it
        gmsSST.put();
    };

    auto leader_committed_next_view = [this](const DerechoSST& gmsSST) {
        return !preds_disabled && gmsSST[curr_view->rank_of_leader()].nCommitted > curr_view->vid;
    };
    auto start_view_change = [this](DerechoSST& gmsSST)  {
        preds_disabled = true; // Disables all the other SST predicates, except suspected_changed and the one I'm about to register

        View& Vc = *curr_view;
        int myRank = curr_view->my_rank;
        //These fields had better be synchronized.
        assert(gmsSST.get_local_index() == curr_view->my_rank);

        wedge_view(Vc);
        node_id_t currChangeID = gmsSST[myRank].changes[Vc.vid % View::N];
        next_view = std::make_unique<View>();
        next_view->vid = Vc.vid + 1;
        next_view->IKnowIAmLeader = Vc.IKnowIAmLeader;
        node_id_t myID = Vc.members[myRank];
        bool failed;
        int whoFailed = Vc.rank_of(currChangeID);
        if (whoFailed != -1)
        {
            failed = true;
            next_view->nFailed = Vc.nFailed - 1;
            next_view->num_members = Vc.num_members - 1;
        }
        else
        {
            failed = false;
            next_view->nFailed = Vc.nFailed;
            next_view->num_members = Vc.num_members;
            int new_member_rank = next_view->num_members++;
            next_view->members[new_member_rank] = currChangeID;
            next_view->member_ips[new_member_rank] = std::string(const_cast<cstring &>(gmsSST[myRank].joiner_ip));
            member_ips_by_id[currChangeID] = next_view->member_ips[new_member_rank];
        }

        int m = 0;
        for (int n = 0; n < Vc.num_members; n++)
        {
            if (n != whoFailed)
            {
                next_view->members[m] = Vc.members[n];
                next_view->failed[m] = Vc.failed[n];
                ++m;
            }
        }

        next_view->who = std::make_shared<node_id_t>(currChangeID);
        if ((next_view->my_rank = next_view->rank_of(myID)) == -1)
        {
            std::cout << std::string("Some other process reported that I failed.  Process ") << myID << std::string(" terminating") << std::endl;
            throw std::runtime_error("Some other process reported that I failed.");
        }

        if (next_view->gmsSST != nullptr)
        {
            throw std::runtime_error("Overwriting the SST");
        }

        //At this point we need to await "meta wedged."
        //To do that, we create a predicate that will fire when meta wedged is true, and put the rest of the code in its trigger.

        auto is_meta_wedged = [&Vc] (const DerechoSST& gmsSST) {
            for(int n = 0; n < gmsSST.get_num_rows(); ++n) {
                if(!Vc.failed[n] && !gmsSST[n].wedged) {
                    return false;
                }
            }
            return true;
        };
        auto meta_wedged_continuation = [this, failed, whoFailed] (DerechoSST& gmsSST) {

            if(curr_view->IAmLeader()) {
                //The leader doesn't need to wait any more, it can execute continuously from here.

                leader_ragged_edge_cleanup(*curr_view); // Finalize deliveries in Vc
                if(!failed) {
                    //Send the view to the newly joined client before we try to do SST and RDMC setup
                    commit_join(*next_view, joining_client_socket);
                    //Close the client's socket
                    joining_client_socket = tcp::socket();
                }
                //This will block until everyone responds to SST/RDMC initial handshakes
                transition_sst_and_rdmc(*next_view, whoFailed);
                next_view->gmsSST->put();

                //Overwrite the current view with the next view, causing it to get destroyed
                curr_view = std::move(next_view);
                curr_view->newView(*curr_view); // Announce the new view to the application

                //Register predicates in the new view
                register_predicates();

                // First task with my new view...
                if (IAmTheNewLeader(*curr_view)) // I'm the new leader and everyone who hasn't failed agrees
                {
                    merge_changes(*curr_view); // Create a combined list of Changes
                }
            } else {
                //Non-leaders need another level of continuation. This necessitates copying code, unfortunately...

                View& curr_view_captured = *curr_view;
                auto leader_globalMin_is_ready = [&curr_view_captured](const DerechoSST& gmsSST) {
                    return gmsSST[curr_view_captured.rank_of_leader()].globalMinReady;
                };
                auto globalMin_ready_continuation = [this, whoFailed](DerechoSST& gmsSST) {
                    follower_ragged_edge_cleanup(*curr_view);
                    //This will block until everyone responds to SST/RDMC initial handshakes
                    transition_sst_and_rdmc(*next_view, whoFailed);
                    next_view->gmsSST->put();

                    //Overwrite the current view with the next view, causing it to get destroyed
                    curr_view = std::move(next_view);
                    curr_view->newView(*curr_view); // Announce the new view to the application

                    //Register predicates in the new view
                    register_predicates();

                    // First task with my new view...
                    if (IAmTheNewLeader(*curr_view)) // I'm the new leader and everyone who hasn't failed agrees
                    {
                        merge_changes(*curr_view); // Create a combined list of Changes
                    }
                };
                gmsSST.predicates.insert(leader_globalMin_is_ready, globalMin_ready_continuation, sst::PredicateType::ONE_TIME);
            }

        };
        gmsSST.predicates.insert(is_meta_wedged, meta_wedged_continuation, sst::PredicateType::ONE_TIME);

    };

    curr_view->gmsSST->predicates.insert(suspected_changed, suspected_changed_trig, sst::PredicateType::RECURRENT);
	curr_view->gmsSST->predicates.insert(start_join_pred, start_join_trig, sst::PredicateType::RECURRENT);
	curr_view->gmsSST->predicates.insert(change_commit_ready, commit_change, sst::PredicateType::RECURRENT);
	curr_view->gmsSST->predicates.insert(leader_proposed_change, ack_proposed_change, sst::PredicateType::RECURRENT);
	curr_view->gmsSST->predicates.insert(leader_committed_next_view, start_view_change, sst::PredicateType::RECURRENT);
}

ManagedGroup::~ManagedGroup() {
    thread_shutdown = true;
    //force accept to return.
    tcp::socket s{"localhost", gms_port};
    for(auto& thread : background_threads) {
        thread.join();
    }
}

void ManagedGroup::setup_sst_and_rdmc(long long unsigned int max_payload_size, message_callback global_stability_callback,
        long long unsigned int block_size, unsigned int window_size, rdmc::send_algorithm type) {

    cout << "Starting SST and DerechoGroup with members ";
    std::copy(curr_view->members.begin(), curr_view->members.end(), std::ostream_iterator<node_id_t>(cout));
    cout << "; I am node " << curr_view->members[curr_view->my_rank] << endl;

    curr_view->gmsSST = make_shared<sst::SST<DerechoRow<View::N>>>(curr_view->members, curr_view->members[curr_view->my_rank]);
    for(int r = 0; r < curr_view->num_members; ++r) {
        gmssst::init((*curr_view->gmsSST)[r]);
    }
    curr_view->rdmc_sending_group = make_unique<DerechoGroup<View::N>>(curr_view->members, curr_view->members[curr_view->my_rank],
            curr_view->gmsSST, max_payload_size, global_stability_callback, block_size, window_size, type);
}

/**
 *
 * @param newView The new view in which to construct an SST and derecho_group
 * @param whichFailed The rank of the node in the old view that failed, if any.
 */
void ManagedGroup::transition_sst_and_rdmc(View& newView, int whichFailed) {
//    map<uint32_t, std::string> ips_by_rank;
//    for(int rank = 0; rank < newView.num_members; ++rank) {
//        ips_by_rank[rank] = newView.members[rank];
//    }
//    rdmc::initialize(ips_by_rank, newView.my_rank);

    cout << "Reinitializing SST and RDMC to transition to view " << newView.vid << endl;
    cout << "New members are ";
    std::copy(newView.members.begin(), newView.members.end(), std::ostream_iterator<node_id_t>(cout));
    cout << " and I am node " << newView.members[newView.my_rank] << endl;
//    if(whichFailed == -1) { //This is a join
//        rdmc::add_address(newView.members.back(), newView.member_ips.back());
//    }
//
//    //TODO: SST needs an "add one member" method; this attempts to re-connect to all the existing members
//    sst::tcp::tcp_initialize(newView.my_rank, member_ips_by_id);
    newView.gmsSST = make_shared<sst::SST<DerechoRow<View::N>>>(newView.members, newView.members[newView.my_rank]);
    newView.rdmc_sending_group = make_unique<DerechoGroup<View::N>>(newView.members, newView.members[newView.my_rank],
            newView.gmsSST, *curr_view->rdmc_sending_group);

    int m = 0;
    for (int n = 0; n < newView.num_members; n++)
    {
        if (n != whichFailed)
        {
            //the compiler won't upcast these references inside the function call,
            //but it can figure out what I mean if I declare them as locals.
            volatile auto& new_row = (*newView.gmsSST)[m++];
            volatile auto& old_row = (*curr_view->gmsSST)[n];
            gmssst::template init_from_existing<View::N>(new_row, old_row);
            new_row.vid = newView.vid;
        }
    }

}

unique_ptr<View> ManagedGroup::start_group(const node_id_t my_id) {
    cout << "Starting new empty group with myself as leader." << endl;
    unique_ptr<View> newView = std::make_unique<View>(1);
    newView->members[0] = my_id;
    newView->member_ips[0] = member_ips_by_id[my_id];
    newView->failed[0] = false;
    newView->IKnowIAmLeader = true;
    return newView;
}

unique_ptr<View> ManagedGroup::join_existing(const ip_addr& leader_ip, const int leader_port) {
    cout << "Joining group by contacting node at " << leader_ip << endl;
    tcp::socket leader_socket{leader_ip, leader_port};
//    node_id_t myNodeID;
    int viewID;
    int numMembers;
    //Temporarily disabled because all node IDs are fixed at startup
    //First the leader sends the node ID this client has been assigned
//    bool success = leader_socket.read((char*)&myNodeID,sizeof(myNodeID));
//    assert(success);
    //The leader will send the members of View in the order they're declared
    bool success = leader_socket.read((char*)&viewID,sizeof(viewID));
    assert(success);
    bool success2 = leader_socket.read((char*)&numMembers,sizeof(numMembers));
    assert(success2);
    unique_ptr<View> newView = std::make_unique<View>(numMembers);
    newView->vid = viewID;
    for (int i = 0; i < numMembers; ++i){
        bool success = leader_socket.read((char*)&newView->members[i],
                sizeof(decltype(newView->members)::value_type));
        assert(success);
    }
    //protocol for sending strings: size, followed by string
    //including null terminator
    for (int i = 0; i < numMembers; ++i){
        int str_size{-1};
        bool success = leader_socket.read((char*)&str_size,sizeof(str_size));
        assert(success);
        char str_rec[str_size];
        bool success2 = leader_socket.read(str_rec,str_size);
        assert(success2);
        newView->member_ips[i] = str_rec;
    }
    //Receive failed[], and also calculate nFailed
    for (int i = 0; i < numMembers; ++i){
        bool newView_failed_i;
        bool success = leader_socket.read((char*) &newView_failed_i,
                sizeof(newView_failed_i));
        assert(success);
        newView->failed[i] = newView_failed_i;
        if(newView->failed[i]) newView->nFailed++;
    }

    return newView;
}


void ManagedGroup::receive_join(tcp::socket& client_socket) {
    ip_addr joiner_ip = client_socket.remote_ip;
    using derechoSST = sst::SST<DerechoRow<View::N>>;
    derechoSST& gmsSST = *curr_view->gmsSST;
    if ((gmsSST[curr_view->my_rank].nChanges - gmsSST[curr_view->my_rank].nCommitted) == View::N/2)
    {
        throw std::runtime_error("Too many changes to allow a Join right now");
    }

//    node_id_t largest_id = member_ips_by_id.rbegin()->first;
//    joining_client_id = largest_id + 1;

    size_t next_change = gmsSST[curr_view->my_rank].nChanges % View::N;
    gmssst::set(gmsSST[curr_view->my_rank].changes[next_change], joining_client_id);
    gmssst::set(gmsSST[curr_view->my_rank].joiner_ip, joiner_ip);

    gmssst::increment(gmsSST[curr_view->my_rank].nChanges);

    curr_view->rdmc_sending_group->wedge(); // RDMC finishes sending, then stops sending or receiving in Vc

    gmssst::set(gmsSST[curr_view->my_rank].wedged, true); // True if RDMC has halted new sends and receives in Vc
    gmsSST.put();
}

void ManagedGroup::commit_join(const View &new_view, tcp::socket &client_socket) {
    //Temporarily disabled because all node IDs are globally fixed at startup
//    client_socket.write((char*) &joining_client_id, sizeof(joining_client_id));
    client_socket.write((char*) &new_view.vid, sizeof(new_view.vid));
    client_socket.write((char*) &new_view.num_members, sizeof(new_view.num_members));
    for (auto nodeID : new_view.members) {
        client_socket.write((char*) &nodeID, sizeof(nodeID));
    }
    for (auto & str : new_view.member_ips) {
        //includes null-terminator
        const int str_size = str.size() + 1;
        client_socket.write((char*) &str_size, sizeof(str_size));
        client_socket.write(str.c_str(), str_size);
    }
    for (bool fbool : new_view.failed) {
        client_socket.write((char*) &fbool, sizeof(fbool));
    }
}

/* ------------------------- Static helper methods ------------------------- */

bool ManagedGroup::suspected_not_equal(const View::DerechoSST& gmsSST, const vector<bool> old) {
    for (int r = 0; r < gmsSST.get_num_rows(); r++) {
        for (int who = 0; who < View::N; who++) {
            if (gmsSST[r].suspected[who] && !old[who]) {
                return true;
            }
        }
    }
    return false;
}

void ManagedGroup::copy_suspected(const View::DerechoSST& gmsSST, vector<bool>& old) {
    for(int who = 0; who < gmsSST.get_num_rows(); ++who) {
        old[who] = gmsSST[gmsSST.get_local_index()].suspected[who];
    }
}

bool ManagedGroup::changes_contains(const View::DerechoSST& gmsSST, const node_id_t q) {
    auto& myRow = gmsSST[gmsSST.get_local_index()];
    for (int n = myRow.nCommitted; n < myRow.nChanges; n++) {
        int p_index = n % View::N;
        const node_id_t p(const_cast<node_id_t&>(myRow.changes[p_index]));
        if (p_index < myRow.nChanges && p == q) {
            return true;
        }
    }
    return false;
}

int ManagedGroup::min_acked(const View::DerechoSST& gmsSST, const vector<bool>& failed) {
    int myRank = gmsSST.get_local_index();
    int min = gmsSST[myRank].nAcked;
    for (size_t n = 0; n < failed.size(); n++) {
        if (!failed[n] && gmsSST[n].nAcked < min) {
            min = gmsSST[n].nAcked;
        }
    }

    return min;
}

int ManagedGroup::await_leader_globalMin_ready(const View& Vc) {
    int Leader = Vc.rank_of_leader();
    while (!(*Vc.gmsSST)[Leader].globalMinReady) {
        Leader = Vc.rank_of_leader();
    }
    return Leader;
}

void ManagedGroup::await_meta_wedged(const View& Vc) {
    int cnt = 0;
    for (int n = 0; n < Vc.num_members; n++)
    {
        while (!Vc.failed[n] && !(*Vc.gmsSST)[n].wedged)
        {
            /* busy-wait */
            if (cnt++ % 100 == 0)
            {
                std::cout << std::string("Process ") << Vc.members[Vc.my_rank] << std::string("... loop in AwaitMetaWedged / ") << std::endl;
            }

            std::this_thread::sleep_for(std::chrono::milliseconds(10));
//            Vc.gmsSST->Pull(Vc);
        }
    }
}

void ManagedGroup::deliver_in_order(const View& Vc, int Leader) {
    // Ragged cleanup is finished, deliver in the implied order
    std::string deliveryOrder = std::string("Delivery Order (View ") + std::to_string(Vc.vid) + std::string(" { ");
    for (int n = 0; n < Vc.num_members; n++) {
        deliveryOrder += std::to_string(Vc.members[Vc.my_rank]) + std::string(":0..")
        + std::to_string((*Vc.gmsSST)[Leader].globalMin[n]) + std::string(" ");
    }

    std::cout << deliveryOrder << std::string("}") << std::endl;
}

void ManagedGroup::leader_ragged_edge_cleanup(View& Vc) {
    int myRank = Vc.my_rank;
    int Leader = Vc.rank_of_leader(); // We don't want this to change under our feet
    std::cout << std::string("Running RaggedEdgeCleanup: ") << Vc.ToString() << std::endl;
    bool found = false;
    for (int n = 0; n < Vc.num_members && !found; n++)
    {
        if ((*Vc.gmsSST)[n].globalMinReady)
        {

            gmssst::set((*Vc.gmsSST)[myRank].globalMin, (*Vc.gmsSST)[n].globalMin, Vc.num_members);
            found = true;
        }
    }

    if (!found)
    {
        for (int n = 0; n < Vc.num_members; n++)
        {
            int min = (*Vc.gmsSST)[myRank].nReceived[n];
            for (int r = 0; r < Vc.num_members; r++)
            {
                if (/*!Vc.failed[r] && */min > (*Vc.gmsSST)[r].nReceived[n])
                {
                    min = (*Vc.gmsSST)[r].nReceived[n];
                }
            }

            (*Vc.gmsSST)[myRank].globalMin[n] = min;
        }
    }

    (*Vc.gmsSST)[myRank].globalMinReady = true;
    Vc.gmsSST->put();
    std::cout << std::string("RaggedEdgeCleanup: FINAL = ") << Vc.ToString() << std::endl;

    deliver_in_order(Vc, Leader);
}

void ManagedGroup::follower_ragged_edge_cleanup(View& Vc) {
    std::cout << std::string("Running RaggedEdgeCleanup: ") << Vc.ToString() << std::endl;
    int myRank = Vc.my_rank;
    // Learn the leader's data and push it before acting upon it
    int Leader = Vc.rank_of_leader();
    gmssst::set((*Vc.gmsSST)[myRank].globalMin, (*Vc.gmsSST)[Leader].globalMin, Vc.num_members);
    (*Vc.gmsSST)[myRank].globalMinReady = true;
    Vc.gmsSST->put();
    std::cout << std::string("RaggedEdgeCleanup: FINAL = ") << Vc.ToString() << std::endl;

    deliver_in_order(Vc, Leader);
}

/* ------------------------------------------------------------------------- */

void ManagedGroup::report_failure(const node_id_t who) {
    int r = curr_view->rank_of(who);
    (*curr_view->gmsSST)[curr_view->my_rank].suspected[r] = true;
	int cnt = 0;
	for (r = 0; r < View::N; r++) {
		if ((*curr_view->gmsSST)[curr_view->my_rank].suspected[r]) {
			++cnt;
		}
	}

	if (cnt >= curr_view->num_members / 2)
	{
		throw new std::runtime_error("Potential partitioning event: this node is no longer in the majority and must shut down!");
	}
    curr_view->gmsSST->put();
}

void ManagedGroup::leave() {
    (*curr_view->gmsSST)[curr_view->my_rank].suspected[curr_view->my_rank] = true;
    curr_view->gmsSST->put();
    thread_shutdown = true;
}

 DerechoGroup<View::N>& ManagedGroup::current_derecho_group() {
     return *curr_view->rdmc_sending_group;
 }


} /* namespace derecho */
