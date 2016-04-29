/*
 * view.cpp
 *
 *  Created on: Apr 15, 2016
 *      Author: edward
 */

#include <memory>
#include <string>
#include <iostream>

#include "view.h"

namespace derecho {

using std::string;
using std::shared_ptr;

View::View() : View(0) {}

View::View(int num_members) : vid(0), members(num_members), member_ips(num_members),
        failed(num_members), nFailed(0), who(nullptr), num_members(num_members),
        my_rank(0), rdmc_sending_group(nullptr),  gmsSST(nullptr) {}

int View::rank_of_leader() const {
    for(int r = 0; r < my_rank; ++r) {
        if(!failed[r]) {
            return r;
        }
    }
    return -1;
}

int View::rank_of(const ip_addr& who) const {
    for(int rank = 0; rank < num_members; ++rank) {
        if(member_ips[rank] == who) {
            return rank;
        }
    }
    return -1;
}

int View::rank_of(const node_id_t& who) const {
    for(int rank = 0; rank < num_members; ++rank) {
        if(members[rank] == who) {
            return rank;
        }
    }
    return -1;
}

/**
 *
 * @param p The rank of the member from whose perspective the leader should be
 * calculated
 * @return Member p's current belief of who the leader is
 */
int View::rank_of_leader(const uint32_t& p) const {
    for(int r = 0; r < num_members; ++r) {
        if(!(*gmsSST)[p].suspected[r])
            return r;
    }
    return -1;
}

void View::newView(const View& Vc) {
    std::string viewString = Vc.ToString();
    std::cout <<"Process " << Vc.members[Vc.my_rank] << "New view: " << viewString << std::endl;
}


bool View::IAmLeader() const {
    return (rank_of_leader() == my_rank); // True if I know myself to be the leader
}


shared_ptr<node_id_t> View::Joined() const {
    if (who == nullptr) {
        return shared_ptr<node_id_t>();
    }
    for (int r = 0; r < num_members; r++) {
        if (members[r] == *who) {
            return who;
        }
    }
    return shared_ptr<node_id_t>();
}

shared_ptr<node_id_t> View::Departed() const {
    if (who == nullptr) {
        return shared_ptr<node_id_t>();
    }
    for (int r = 0; r < num_members; r++) {
        if (members[r] == *who) {
            return shared_ptr<node_id_t>();
        }
    }
    return who;
}

std::string View::ToString() const {
    string s = std::string("View ") + std::to_string(vid) + string(": MyRank=") + std::to_string(my_rank) + string("... ");
    string ms = " ";
    for (int m = 0; m < num_members; m++) {
        ms += std::to_string(members[m]) + string("  ");
    }

    s += string("Members={") + ms + string("}, ");
    string fs = (" ");
    for (int m = 0; m < num_members; m++) {
        fs += failed[m] ? string(" T ") : string(" F ");
    }

    s += string("Failed={") + fs + string(" }, nFailed=") + std::to_string(nFailed);
    shared_ptr<node_id_t> dep = Departed();
    if (dep != nullptr) {
        s += string(", Departed: ") + std::to_string(*dep);
    }

    shared_ptr<node_id_t> join = Joined();
    if (join != nullptr) {
        s += string(", Joined: ") + std::to_string(*join);
    }

//    s += string("\n") + gmsSST->ToString();
    return s;
}


}
