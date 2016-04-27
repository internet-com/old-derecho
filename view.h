/*
 * view.h
 *
 *  Created on: Apr 14, 2016
 *      Author: edward
 */

#ifndef VIEW_H_
#define VIEW_H_

#include <cstdint>
#include <memory>

#include "sst/sst.h"
#include "derecho_group.h"
#include "derecho_row.h"

namespace derecho {


class View {
    public:
        /** Upper bound on the number of members that will ever be in any one view. */
        static constexpr int N = 10;

        /** Sequential view ID: 0, 1, ... */
        int vid;
        /** Members in view k */
        ip_addr members[N];
        /** True if members[i] is considered to have failed */
        bool failed[N];
        int nFailed = 0;
        /** Process that joined or departed since the prior view; null if this is the first view */
        std::shared_ptr<ip_addr> who;
        /** Number of members */
        int num_members;
        /** For member p, returns rankOf(p) */
        int my_rank;
        /** RDMC manager object containing one RDMC group for each sender */
        std::unique_ptr<derecho_group<N>> rdmc_sending_group;
        std::shared_ptr<sst::SST<DerechoRow<N>>> gmsSST;

        void newView(const View& Vc);

        /** Returns a pointer to the (IP address of the) member who recently joined,
         * or null if the most recent change was not a join. */
        std::shared_ptr<ip_addr> Joined() const;
        /** Returns a pointer to the (IP address of the) member who recently departed,
         * or null if the most recent change was not a departure. */
        std::shared_ptr<ip_addr> Departed() const;

        int rank_of(const ip_addr& who) const;
        int rank_of_leader() const;
        int rank_of_leader(const uint32_t& p) const;

        bool IKnowIAmLeader = false; // I am the leader (and know it)

        bool IAmLeader() const;

        std::string ToString() const;
};

}



#endif /* VIEW_H_ */
