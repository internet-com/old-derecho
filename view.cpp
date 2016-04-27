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

shared_ptr<ip_addr> View::Joined() const {
    if (who == nullptr) {
        return shared_ptr<ip_addr>();
    }
    for (int r = 0; r < num_members; r++) {
        if (members[r] == *who) {
            return who;
        }
    }
    return shared_ptr<ip_addr>();
}

shared_ptr<ip_addr> View::Departed() const {
    if (who == nullptr) {
        return shared_ptr<ip_addr>();
    }
    for (int r = 0; r < num_members; r++) {
        if (members[r] == *who) {
            return shared_ptr<ip_addr>();
        }
    }
    return who;
}


bool View::IAmLeader() const {
    return (rank_of_leader() == my_rank); // True if I know myself to be the leader
}

std::string View::ToString() const {
    string s = std::string("View ") + std::to_string(vid) + string(": MyRank=") + std::to_string(my_rank) + string("... ");
    string ms = " ";
    for (int m = 0; m < num_members; m++) {
        ms += string(members[m]) + string("  ");
    }

    s += string("Members={") + ms + string("}, ");
    string fs = (" ");
    for (int m = 0; m < num_members; m++) {
        fs += failed[m] ? string(" T ") : string(" F ");
    }

    s += string("Failed={") + fs + string(" }, nFailed=") + std::to_string(nFailed);
    shared_ptr<ip_addr> dep = Departed();
    if (dep != nullptr) {
        s += string(", Departed: ") + *dep;
    }

    shared_ptr<ip_addr> join = Joined();
    if (join != nullptr) {
        s += string(", Joined: ") + *join;
    }

//    s += string("\n") + gmsSST->ToString();
    return s;
}


}
