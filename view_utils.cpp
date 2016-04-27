/*
 * view_utils.cpp
 *
 *  Created on: Apr 25, 2016
 *      Author: edward
 */

#include <vector>
#include <cstring>

#include "view_utils.h"
#include "view.h"

namespace derecho {

bool NotEqual(const View& theView, const std::vector<bool>& old)
{
    for (int r = 0; r < theView.num_members; r++)
    {
        for (int who = 0; who < View::N; who++)
        {
            if ((*theView.gmsSST)[r].suspected[who] && !old[who])
            {
                return true;
            }
        }
    }
    return false;
}

void Copy(const View& Vc, std::vector<bool>& old)
{
    int myRank = Vc.my_rank;
    for (int who = 0; who < Vc.num_members; who++)
    {
        old[who] = Vc.gmsSST->get(myRank).suspected[who];
    }
}

bool ChangesContains(const View& Vc, const ip_addr& q)
{
    auto& myRow = (*Vc.gmsSST)[Vc.my_rank];
    for (int n = myRow.nCommitted; n < myRow.nChanges; n++)
    {
        const ip_addr p(const_cast<cstring&>(myRow.changes[n % View::N]));
        if (!p.empty() && p == q)
        {
            return true;
        }
    }
    return false;
}


int MinAcked(const View& Vc, const bool (&failed)[View::N] ) {
    int myRank = Vc.my_rank;
    int min = (*Vc.gmsSST)[myRank].nAcked;
    for (int n = 0; n < Vc.num_members; n++) {
        if (!failed[n] && (*Vc.gmsSST)[n].nAcked < min) {
            min = (*Vc.gmsSST)[n].nAcked;
        }
    }

    return min;
}

bool IAmTheNewLeader(View& Vc) {
    if (Vc.IKnowIAmLeader) {
        return false; // I am the OLD leader
    }

    for (int n = 0; n < Vc.my_rank; n++) {
        for (int row = 0; row < Vc.my_rank; row++) {
            if (!Vc.failed[n] && !(*Vc.gmsSST)[row].suspected[n]) {
                return false; // I'm not the new leader, or some failure suspicion hasn't fully propagated
            }
        }
    }
    Vc.IKnowIAmLeader = true;
    return true;
}

void Merge(View& Vc, int myRank) {
    // Merge the change lists
    for (int n = 0; n < Vc.num_members; n++) {
        if ((*Vc.gmsSST)[myRank].nChanges < (*Vc.gmsSST)[n].nChanges) {
            gmssst::set((*Vc.gmsSST)[myRank].changes, (*Vc.gmsSST)[n].changes);
            (*Vc.gmsSST)[myRank].nChanges = (*Vc.gmsSST)[n].nChanges;
        }

        if ((*Vc.gmsSST)[myRank].nCommitted < (*Vc.gmsSST)[n].nCommitted) // How many I know to have been committed
        {
            (*Vc.gmsSST)[myRank].nCommitted = (*Vc.gmsSST)[n].nCommitted;
        }
    }
    bool found = false;
    for (int n = 0; n < Vc.num_members; n++) {
        if (Vc.failed[n]) {
            // Make sure that the failed process is listed in the Changes vector as a proposed change
            for (int c = (*Vc.gmsSST)[myRank].nCommitted; c < (*Vc.gmsSST)[myRank].nChanges && !found; c++) {
                if (gmssst::equals((*Vc.gmsSST)[myRank].changes[c % View::N], Vc.members[n].c_str())) {
                    // Already listed
                    found = true;
                }
            }
        } else {
            // Not failed
            found = true;
        }

        if (!found) {
            gmssst::set((*Vc.gmsSST)[myRank].changes[(*Vc.gmsSST)[myRank].nChanges % View::N], Vc.members[n]);
            (*Vc.gmsSST)[myRank].nChanges++;
        }
    }
    Vc.gmsSST->put();
//    Vc.gmsSST->Push(Vc.myRank, Vc.vid);
}

void WedgeView(View& Vc, sst::SST<DerechoRow<View::N>>& gmsSST, int myRank) {
    Vc.rdmc_sending_group->wedge(); // RDMC finishes sending, stops new sends or receives in Vc
    gmsSST[myRank].wedged = true;
}

} //namespace derecho

