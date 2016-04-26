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

#include "managed_group.h"
#include "view_utils.h"
#include "gms_sst_row.h"

namespace derecho {

using std::map;
using std::vector;
using std::make_shared;
using std::make_unique;

ManagedGroup::ManagedGroup(const int gms_port, const ip_addr& my_ip, const ip_addr& leader_ip, long long unsigned int _buffer_size, long long unsigned int _block_size,
                message_callback global_stability_callback, rdmc::send_algorithm _type, unsigned int _window_size) :
        gms_port(gms_port), thread_shutdown(false), background_threads() {
    if (my_ip != leader_ip){
        this->curr_view = join_existing(leader_ip, gms_port);
    } else {
        curr_view = start_group(my_ip);
    }
    curr_view.my_rank = curr_view.rank_of(my_ip);
    
    setup_sst_and_rdmc(_buffer_size, _block_size, global_stability_callback, _type, _window_size);

    background_threads.emplace_back(std::thread{[&](){
        tcp::connection_listener ss(gms_port);
        while (!thread_shutdown)
            pending_joins.locked().access.emplace_back(ss.accept());
    }});
    background_threads.emplace_back(std::thread(&ManagedGroup::monitor_changes,this));
}

ManagedGroup::~ManagedGroup() {
    thread_shutdown = true;
    //force accept to return.
    tcp::socket s{"localhost", gms_port};
    for(auto& thread : background_threads) {
        thread.join();
    }
}

void ManagedGroup::setup_sst_and_rdmc(long long unsigned int buffer_size, long long unsigned int block_size,
        message_callback global_stability_callback, rdmc::send_algorithm type, unsigned int window_size) {
    //ASSUMPTION: The index of an IP address in View::members is the "rank" of the node with that IP address
    map<uint32_t, std::string> ips_by_rank;
    for(int rank = 0; rank < curr_view.num_members; ++rank) {
        ips_by_rank[rank] = curr_view.members[rank];
    }
    rdmc::initialize(ips_by_rank, curr_view.my_rank);
    sst::tcp::tcp_initialize(curr_view.my_rank, ips_by_rank);
    sst::verbs_initialize();
    vector<int> member_ranks(curr_view.num_members);
    for(int i = 0; i < curr_view.num_members; ++i) {
        member_ranks[i] = i;
    }
    curr_view.gmsSST = make_shared<sst::SST<DerechoRow<View::N>>>(member_ranks, curr_view.my_rank);
    curr_view.rdmc_sending_group = make_unique<derecho_group<View::N>>(member_ranks, curr_view.my_rank,
            curr_view.gmsSST, buffer_size, block_size, global_stability_callback, type, window_size);
}

/**
 *
 * @param newView The new view in which to construct an SST and derecho_group
 * @param whichFailed The rank of the node in the old view that failed, if any.
 */
void ManagedGroup::transition_sst_and_rdmc(View& newView, int whichFailed) {
    map<uint32_t, std::string> ips_by_rank;
    for(int rank = 0; rank < newView.num_members; ++rank) {
        ips_by_rank[rank] = newView.members[rank];
    }
    rdmc::initialize(ips_by_rank, newView.my_rank);
    sst::tcp::tcp_initialize(newView.my_rank, ips_by_rank);
    sst::verbs_initialize();
    vector<int> member_ranks(newView.num_members);
    for(int i = 0; i < newView.num_members; ++i) {
        member_ranks[i] = i;
    }
    newView.gmsSST = make_shared<sst::SST<DerechoRow<View::N>>>(member_ranks, newView.my_rank);
    newView.rdmc_sending_group = make_unique<derecho_group<View::N>>(member_ranks, newView.my_rank,
            newView.gmsSST, *curr_view.rdmc_sending_group);

    int m = 0;
    for (int n = 0; n < newView.num_members; n++)
    {
        if (n != whichFailed)
        {
            //the compiler won't upcast these references inside the function call, so I have to declare them as locals.
            GMSTableRow<View::N>& new_row = (*newView.gmsSST)[m++];
            GMSTableRow<View::N>& old_row = (*curr_view.gmsSST)[n];
            gmssst::init_from_existing(new_row, old_row);
        }
    }

}

View ManagedGroup::start_group(const ip_addr& my_ip) {
    View newView;
    newView.vid = 0;
    newView.num_members = 1;
    newView.members[0] = my_ip;
    newView.failed[0] = false;
    newView.IKnowIAmLeader = true;
    return newView;
}

View ManagedGroup::join_existing(const ip_addr& leader_ip, const int leader_port) {
    tcp::socket leader_socket{leader_ip, leader_port};
    View newView;
    bool success = leader_socket.read((char*)&newView.vid,sizeof(newView.vid));
    assert(success);
    constexpr int N = View::N;
    //protocol for sending strings: size, followed by string
    //including null terminator
    for (int i = 0; i < N; ++i){
        int str_size{-1};
        bool success = leader_socket.read((char*)&str_size,sizeof(str_size));
        assert(success);
        char str_rec[str_size];
        bool success2 = leader_socket.read(str_rec,str_size);
        assert(success2);
        newView.members[i] = str_rec;
    }
    for (int i = 0; i < N; ++i){
        bool success = leader_socket.read((char*)&newView.failed[i],
                sizeof(newView.failed));
        assert(success);
    }
    bool success2 = leader_socket.read((char*)&newView.num_members,sizeof(newView.num_members));
    assert(success2);

    return newView;
}


void ManagedGroup::receive_join(tcp::socket& client_socket) {
    ip_addr& joiner_ip = client_socket.remote_ip;
    sst::SST<DerechoRow<View::N>>& gmsSST = *curr_view.gmsSST;
    if ((gmsSST[curr_view.my_rank].nChanges - gmsSST[curr_view.my_rank].nCommitted) == View::N/2)
    {
        throw std::runtime_error("Too many changes to allow a Join right now");
    }

    gmsSST[curr_view.my_rank].changes[gmsSST[curr_view.my_rank].nChanges % View::N] = joiner_ip;
    gmsSST[curr_view.my_rank].nChanges++;

    curr_view.rdmc_sending_group->wedge(); // RDMC finishes sending, then stops sending or receiving in Vc

    gmsSST[curr_view.my_rank].wedged = true; // True if RDMC has halted new sends and receives in Vc
    gmsSST.put();
}

void ManagedGroup::commit_join(const View &new_view, tcp::socket &client_socket) {
    client_socket.write((char*) &new_view.vid, sizeof(new_view.vid));
    for (auto & str : new_view.members) {
        //includes null-terminator
        const int str_size = str.size() + 1;
        client_socket.write((char*) &str_size, sizeof(str_size));
        client_socket.write(str.c_str(), str_size);
    }
    for (bool fbool : new_view.failed) {
        client_socket.write((char*) &fbool, sizeof(fbool));
    }
    client_socket.write((char*) &new_view.num_members, sizeof(new_view.num_members));
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
                std::cout << std::string("Process ") << Vc.members[Vc.my_rank] << std::string("... loop in AwaitMetaWedged / ") << (*Vc.gmsSST)[n].gmsSSTRowInstance << std::endl;
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
        deliveryOrder += Vc.members[Vc.my_rank] + std::string(":0..")
        + std::to_string((*Vc.gmsSST)[Leader].globalMin[n]) + std::string(" ");
    }

    std::cout << deliveryOrder << std::string("}") << std::endl;
}

void ManagedGroup::ragged_edge_cleanup(View& Vc) {
    await_meta_wedged(Vc);
    int myRank = Vc.my_rank;
    // This logic depends on the transitivity of the K1 operator, as discussed last week
    int Leader = Vc.rank_of_leader(); // We don’t want this to change under our feet
    if (Vc.IAmLeader())
    {
        std::cout << std::string("Running RaggedEdgeCleanup: ") << Vc.ToString() << std::endl;
        bool found = false;
//        Vc.gmsSST->Pull(Vc);
        for (int n = 0; n < Vc.num_members && !found; n++)
        {
            if ((*Vc.gmsSST)[n].globalMinReady)
            {
                //copy_n copies in this direction ------------------->
                std::copy_n((*Vc.gmsSST)[n].globalMin, Vc.num_members, (*Vc.gmsSST)[myRank].globalMin);
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
                    if (!Vc.failed[r] && min > (*Vc.gmsSST)[r].nReceived[n])
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
    }
    else
    {
        // Learn the leader’s data and push it before acting upon it
        Leader = await_leader_globalMin_ready(Vc);
        std::copy_n((*Vc.gmsSST)[Leader].globalMin, Vc.num_members, (*Vc.gmsSST)[myRank].globalMin);
        (*Vc.gmsSST)[myRank].globalMinReady = true;
        Vc.gmsSST->put();
    }

    deliver_in_order(Vc, Leader);
}

void ManagedGroup::report_failure(const ip_addr& who) {
    int r = curr_view.rank_of(who);
    (*curr_view.gmsSST)[curr_view.my_rank].suspected[r] = true;
    curr_view.gmsSST->put();
}

void ManagedGroup::leave() {
    (*curr_view.gmsSST)[curr_view.my_rank].suspected[curr_view.my_rank] = true;
    curr_view.gmsSST->put();
    thread_shutdown = true;
}

void ManagedGroup::monitor_changes() {
    std::vector<bool> oldSuspected = std::vector<bool>(View::N);
    while(!thread_shutdown) {
        View& Vc = curr_view;
        int Leader = curr_view.rank_of_leader(), myRank = curr_view.my_rank;
        sst::SST<DerechoRow<View::N>>& gmsSST = *curr_view.gmsSST;

        //This part should be a predicate that GMS registers with its SST
        if (NotEqual(curr_view, oldSuspected)) //This means there's a change in theView.suspected
        {
            // Aggregate suspicions into gmsSST[myRank].Suspected;
            for (int r = 0; r < curr_view.num_members; r++)
            {
                for (int who = 0; who < curr_view.num_members; who++)
                {
                    gmsSST[myRank].suspected[who] |= gmsSST[r].suspected[who];
                }
            }

            for (int q = 0; q < curr_view.num_members; q++)
            {
                if (gmsSST[myRank].suspected[q] && !curr_view.failed[q])
                {
                    if (curr_view.nFailed + 1 >= View::N / 2)
                    {
                        throw std::runtime_error("Majority of a Derecho group simultaneously failed … shutting down");
                    }

                    gmsSST.freeze(q); // Cease to accept new updates from q
                    curr_view.rdmc_sending_group->wedge();
                    gmsSST[myRank].wedged = true; // RDMC has halted new sends and receives in theView
                    curr_view.failed[q] = true;
                    curr_view.nFailed++;

                    if (curr_view.nFailed > curr_view.num_members / 2 || (curr_view.nFailed == curr_view.num_members / 2 && curr_view.num_members % 2 == 0))
                    {
                        throw std::runtime_error("Potential partitioning event: this node is no longer in the majority and must shut down!");
                    }

                    gmsSST.put();
                    if (curr_view.IAmLeader() && !ChangesContains(curr_view, curr_view.members[q])) // Leader initiated
                    {
                        if ((gmsSST[myRank].nChanges - gmsSST[myRank].nCommitted) == View::N)
                        {
                            throw std::runtime_error("Ran out of room in the pending changes list");
                        }

                        gmsSST[myRank].changes[gmsSST[myRank].nChanges % View::N] = curr_view.members[q]; // Reports the failure (note that q NotIn members)
                        gmsSST[myRank].nChanges++;
                        std::cout << std::string("NEW SUSPICION: adding ") << curr_view.members[q] << std::string(" to the CHANGES/FAILED list") << std::endl;
                        gmsSST.put();
                    }
                }
            }
            Copy(curr_view, oldSuspected);
        }

        //Save this to use for sending the client the new view information
        tcp::socket joining_client_socket;

        if(curr_view.IAmLeader() && has_pending_join()) {
            joining_client_socket = pending_joins.locked().access.pop_front();
            receive_join(joining_client_socket);
        }

        //Each of these if statements could also be a predicate, I think

        int M;
        if (myRank == Leader && (M = MinAcked(Vc, Vc.failed)) > gmsSST[myRank].nCommitted)
        {
            gmsSST[myRank].nCommitted = M; // Leader commits a new request
            gmsSST.put();
        }

        if (gmsSST[Leader].nChanges > gmsSST[myRank].nAcked)
        {
            WedgeView(Vc, gmsSST, myRank); // True if RDMC has halted new sends, receives in Vc
            if (myRank != Leader)
            {
                std::copy_n(gmsSST[Leader].changes, View::N, gmsSST[myRank].changes); // Echo (copy) the vector including the new changes
                gmsSST[myRank].nChanges = gmsSST[Leader].nChanges; // Echo the count
                gmsSST[myRank].nCommitted = gmsSST[Leader].nCommitted;
            }

            gmsSST[myRank].nAcked = gmsSST[Leader].nChanges; // Notice a new request, acknowledge it
            gmsSST.put();
        }

        if (gmsSST[Leader].nCommitted > Vc.vid)
        {
            //Not actually necessary right now; gmsSST has no predicates, other than the ones derecho_group installed
//            gmsSST.disable(); // Disables the SST rule evaluation for this SST

            WedgeView(Vc, gmsSST, myRank);
            ip_addr currChangeIP = gmsSST[myRank].changes[Vc.vid % View::N];
            View Vnext;
            Vnext.vid = Vc.vid + 1;
            Vnext.IKnowIAmLeader = Vc.IKnowIAmLeader;
            ip_addr myIPAddr = Vc.members[myRank];
            bool failed;
            int whoFailed = Vc.rank_of(currChangeIP);
            if (whoFailed != -1)
            {
                failed = true;
                Vnext.nFailed = Vc.nFailed - 1;
                Vnext.num_members = Vc.num_members - 1;
            }
            else
            {
                failed = false;
                Vnext.nFailed = Vc.nFailed;
                Vnext.num_members = Vc.num_members;
                Vnext.members[Vnext.num_members++] = currChangeIP;
            }

            int m = 0;
            for (int n = 0; n < Vc.num_members; n++)
            {
                if (n != whoFailed)
                {
                    Vnext.members[m] = Vc.members[n];
                    Vnext.failed[m] = Vc.failed[n];
                    ++m;
                }
            }

            Vnext.who = std::make_shared<ip_addr>(currChangeIP);
            if ((Vnext.my_rank = Vnext.rank_of(myIPAddr)) == -1)
            {
                std::cout << std::string("Some other process reported that I failed.  Process ") << myIPAddr << std::string(" terminating") << std::endl;
                return;
            }

            if (Vnext.gmsSST != nullptr)
            {
                throw std::exception("Overwriting the SST");
            }

//            Vc.gmsSST->Pull(Vc);
            // The intent of these next lines is that we move entirely to Vc+1 and cease to use Vc
            ragged_edge_cleanup(Vc); // Finalize deliveries in Vc

            //Send the view to the newly joined client before we try to do SST and RDMC setup
            if (Vc.IAmLeader() && !failed)
            {
                commit_join(Vnext, joining_client_socket);
            }

            //This will block until everyone responds to SST/RDMC initial handshakes
            transition_sst_and_rdmc(Vnext, whoFailed);
            Vnext.gmsSST->put();

            //Overwrite the current view with the next view, causing it to go "out of scope" and get destroyed
            //I'm pretty sure this is how you do it with value types
            curr_view = std::move(Vnext);
            Vc = curr_view;
            gmsSST = *curr_view.gmsSST;
            curr_view.newView(curr_view); // Announce the new view to the application

            // Finally, some cleanup.  These could be in a background thread
            /* The old RDMC instance will get cleaned up when its destructor is called,
             * which will happen when the old view goes out of scope
             * The old SST instance will get cleaned up when its destructor is called,
             * which will happen when the old view goes out of scope (or after the derecho_group
             * gets destroyed, whichever happpens last, since the view and the derecho_group
             * both have a shared_ptr to the SST)
             */

            // First task with my new view...
            if (IAmTheNewLeader(Vc)) // I’m the new leader and everyone who hasn’t failed agrees
            {
                Merge(Vc, Vc.my_rank); // Create a combined list of Changes
            }
        }
    } //end while(!thread_shutdown)
}

 derecho_group<View::N>& ManagedGroup::current_derecho_group() {
     return *curr_view.rdmc_sending_group;
 }


} /* namespace derecho */
