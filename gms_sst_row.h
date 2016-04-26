/*
 * gms_sst_row.h
 *
 *  Created on: Apr 15, 2016
 *      Author: edward
 */

#ifndef GMS_SST_ROW_H_
#define GMS_SST_ROW_H_

#include <memory>
#include <iterator>
#include <algorithm>
#include <string>

namespace derecho {

using ip_addr = std::string;

template<unsigned int N>
struct GMSTableRow {
        //Fields used only in debugging
        static int rcntr;
        int gmsSSTRowInstance = ++rcntr;
        int gmsSSTRowTime = 0;
        std::shared_ptr<ip_addr> theIPA; // Saves hassle and simplifies debugging to have this handy

        /** View ID associated with this SST */
        int vid = 0;
        static int maxChanges; // Debugging: largest value nChanges ever had
        /** Array of same length as View::members, where each bool represents
         * whether the corresponding member is suspected to have failed */
        bool suspected[N];
        /** An array of nChanges proposed changes to the view (the number of non-
         * empty elements is nChanges). The total number of changes never exceeds
         * N/2. If request i is a Join, changes[i] is not in current View's
         * members. If request i is a Departure, changes[i] is in current View's
         * members. */
        ip_addr changes[N]; //If total never exceeds N/2, why is this N?
        /** How many changes to the view are pending. */
        int nChanges = 0;
        /** How many proposed view changes have reached the commit point. */
        int nCommitted = 0;
        /** How many proposed changes have been seen. Incremented by a member
         * to acknowledge that it has seen a proposed change.*/
        int nAcked = 0;
        /** Local count of number of received messages by sender.  For each
         * sender k, nReceived[k] is the number received (a.k.a. "locally stable"). */
        long long int nReceived[N];
        /** Set after calling rdmc::wedged(), reports that this member is wedged.
         * Must be after nReceived!*/
        bool wedged = false;
        /** Array of how many messages to accept from each sender, K1-able*/
        int globalMin[N];
        /** Must come after GlobalMin */
        bool globalMinReady = false;
};

namespace gmssst {

template<unsigned int N>
void init_from_existing(GMSTableRow<N>& newRow, const GMSTableRow<N>& existingRow) {
    std::copy(std::begin(existingRow.changes), std::end(existingRow.changes), std::begin(newRow.changes));
    newRow.nChanges = existingRow.nChanges;
    newRow.nCommitted = existingRow.nCommitted;
    newRow.nAcked = existingRow.nAcked;
    newRow.gmsSSTRowTime = existingRow.gmsSSTRowTime;
}

template<unsigned int N>
std::string to_string(const GMSTableRow<N>& row) {
    std::string s = std::string(*row.theIPA) + std::string("@ vid=") + std::string(row.vid)
            + std::string("[row-time=") + std::string(row.gmsSSTRowTime) + std::string("]: ");
    std::string tf = " ";
    for (int n = 0; n < N; n++)
    {
        tf += (row.suspected[n]? "T": "F") + std::string(" ");
    }

    s += std::string("Suspected={") + tf + std::string("}, nChanges=") + std::string(row.nChanges)
            + std::string(", nCommitted=") + std::string(row.nCommitted);
    std::string ch = " ";
    for (int n = row.nCommitted; n < row.nChanges; n++)
    {
        ch += row.changes[n % N];
    }
    std::string rs = " ";
    for (int n = 0; n < N; n++)
    {
        rs += row.nReceived[n] + std::string(" ");
    }

    s += std::string(", Changes={") + ch + std::string(" }, nAcked=")
            + row.nAcked + std::string(", nReceived={") + rs + std::string("}");
    std::string gs = " ";
    for (int n = 0; n < row.globalMin.size(); n++)
    {
        gs += row.globalMin[n] + std::string(" ");
    }

    s += std::string(", Wedged = ") + (row.wedged? "T": "F")
            + std::string(", GlobalMin = {") + gs + std::string("}, GlobalMinReady=")
            + std::string(row.globalMinReady) + std::string("\n");
    return s;
}

} //namespace gmssst

} //namespace derecho



#endif /* GMS_SST_ROW_H_ */
