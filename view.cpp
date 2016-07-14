/*
 * view.cpp
 *
 *  Created on: Apr 15, 2016
 *      Author: edward
 */

#include <memory>
#include <string>
#include <iostream>
#include <sstream>
#include <fstream>
#include <iterator>

#include "view.h"
#include "persistence.h"

namespace derecho {

using std::string;
using std::shared_ptr;

View::View() : View(0) {}

View::View(int num_members)
    : vid(0),
      members(num_members),
      member_ips(num_members),
      failed(num_members),
      nFailed(0),
      who(nullptr),
      num_members(num_members),
      my_rank(0),
      rdmc_sending_group(nullptr),
      gmsSST(nullptr) {}

void View::init_vectors() {
    members.resize(num_members);
    member_ips.resize(num_members);
    failed.resize(num_members);
}

int View::rank_of_leader() const {
    for(int r = 0; r < num_members; ++r) {
        if(!failed[r]) {
            return r;
        }
    }
    return -1;
}

int View::rank_of(const ip_addr &who) const {
    for(int rank = 0; rank < num_members; ++rank) {
        if(member_ips[rank] == who) {
            return rank;
        }
    }
    return -1;
}

int View::rank_of(const node_id_t &who) const {
    for(int rank = 0; rank < num_members; ++rank) {
        if(members[rank] == who) {
            return rank;
        }
    }
    return -1;
}

void View::newView(const View &Vc) {
    // I don't know what this is supposed to do in real life. In the C#
    // simulation
    // it just prints to stdout.
    //    std::cout <<"Process " << Vc.members[Vc.my_rank] << " New view: " <<
    //    Vc.ToString() << std::endl;
}

bool View::IAmLeader() const {
    return (rank_of_leader() == my_rank);  // True if I know myself to be the leader
}

bool View::IAmTheNewLeader() {
    if(IKnowIAmLeader) {
        return false;  // I am the OLD leader
    }

    for(int n = 0; n < my_rank; n++) {
        for(int row = 0; row < my_rank; row++) {
            if(!failed[n] && !(*gmsSST)[row].suspected[n]) {
                return false;  // I'm not the new leader, or some failure suspicion hasn't fully propagated
            }
        }
    }
    IKnowIAmLeader = true;
    return true;
}

void View::merge_changes() {
    int myRank = my_rank;
    // Merge the change lists
    for(int n = 0; n < num_members; n++) {
        if((*gmsSST)[myRank].nChanges < (*gmsSST)[n].nChanges) {
            gmssst::set((*gmsSST)[myRank].changes, (*gmsSST)[n].changes);
            gmssst::set((*gmsSST)[myRank].nChanges, (*gmsSST)[n].nChanges);
        }

        if((*gmsSST)[myRank].nCommitted < (*gmsSST)[n].nCommitted)  // How many I know to have been committed
        {
            gmssst::set((*gmsSST)[myRank].nCommitted, (*gmsSST)[n].nCommitted);
        }
    }
    bool found = false;
    for(int n = 0; n < num_members; n++) {
        if(failed[n]) {
            // Make sure that the failed process is listed in the Changes vector as a proposed change
            for(int c = (*gmsSST)[myRank].nCommitted; c < (*gmsSST)[myRank].nChanges && !found; c++) {
                if((*gmsSST)[myRank].changes[c % View::MAX_MEMBERS] == members[n]) {
                    // Already listed
                    found = true;
                }
            }
        } else {
            // Not failed
            found = true;
        }

        if(!found) {
            gmssst::set((*gmsSST)[myRank].changes[(*gmsSST)[myRank].nChanges % View::MAX_MEMBERS],
                        members[n]);
            gmssst::increment((*gmsSST)[myRank].nChanges);
        }
    }
    gmsSST->put();
}

void View::wedge() {
    rdmc_sending_group->wedge();  // RDMC finishes sending, stops new sends or receives in Vc
    gmssst::set((*gmsSST)[my_rank].wedged, true);
    gmsSST->put();
}

shared_ptr<node_id_t> View::Joined() const {
    if(who == nullptr) {
        return shared_ptr<node_id_t>();
    }
    for(int r = 0; r < num_members; r++) {
        if(members[r] == *who) {
            return who;
        }
    }
    return shared_ptr<node_id_t>();
}

shared_ptr<node_id_t> View::Departed() const {
    if(who == nullptr) {
        return shared_ptr<node_id_t>();
    }
    for(int r = 0; r < num_members; r++) {
        if(members[r] == *who) {
            return shared_ptr<node_id_t>();
        }
    }
    return who;
}

std::string View::ToString() const {
    std::stringstream s;
    s << "View " << vid << ": MyRank=" << my_rank << ". " << "Members={";
    for(int m = 0; m < num_members; m++) {
        s << members[m] << "  ";
    }
    s << "}, " << "Failed={";
    for(int m = 0; m < num_members; m++) {
        s << (failed[m] ? " T ": " F ");
    }
    s << " }, nFailed=" << nFailed;
    shared_ptr<node_id_t> dep = Departed();
    if(dep != nullptr) {
        s << ", Departed: " << *dep;
    }
    shared_ptr<node_id_t> join = Joined();
    if(join != nullptr) {
        s << ", Joined: " << *join;
    }

    return s.str();
}

void persist_view(const View& view, const std::string& view_file_name) {
    using namespace std::placeholders;
    //Use the "safe save" paradigm to ensure a crash won't corrupt our saved View file
    std::ofstream view_file_swap(view_file_name + persistence::SWAP_FILE_EXTENSION);
    auto view_file_swap_write = [&](char const * const c, std::size_t n){
        //std::bind(&std::ofstream::write, &view_file_swap, _1, _2);
        view_file_swap.write(c,n);
    };
    mutils::post_object(view_file_swap_write, mutils::bytes_size(view));
    mutils::post_object(view_file_swap_write, view);
    view_file_swap.close();
    if(std::rename((view_file_name + persistence::SWAP_FILE_EXTENSION).c_str(), view_file_name.c_str()) < 0) {
        std::cerr << "Error updating saved-state file on disk! " << strerror(errno) << endl;
    }
}

std::unique_ptr<View> load_view(const std::string& view_file_name) {
    std::ifstream view_file(view_file_name);
    std::ifstream view_file_swap(view_file_name + persistence::SWAP_FILE_EXTENSION);
    std::unique_ptr<View> view;
    std::unique_ptr<View> swap_view;
    //The expected view file might not exist, in which case we'll fall back to the swap file
    if(view_file.good()) {
        //Each file contains the size of the view (an int copied as bytes),
        //followed by a serialized view
        std::size_t size_of_view;
        view_file.read((char*) &size_of_view, sizeof(size_of_view));
        char buffer[size_of_view];
        view_file.read(buffer, size_of_view);
        //If the view file doesn't contain a complete view (due to a crash
        //during writing), the read() call will set failbit
        if(!view_file.fail()) {
            view = mutils::from_bytes<View>(nullptr, buffer);
        }
    }
    if(view_file_swap.good()) {
        std::size_t size_of_view;
        view_file_swap.read((char*) &size_of_view, sizeof(size_of_view));
        char buffer[size_of_view];
        view_file_swap.read(buffer, size_of_view);
        if(!view_file_swap.fail()) {
            swap_view = mutils::from_bytes<View>(nullptr, buffer);
        }
    }
    if(swap_view == nullptr ||
            (view != nullptr && view->vid >= swap_view->vid)) {
        return view;
    } else {
        return swap_view;
    }
}

std::ostream& operator<<(std::ostream& stream, const View& view) {
    stream << view.vid << std::endl;
    std::copy(view.members.begin(), view.members.end(), std::ostream_iterator<node_id_t>(stream, " "));
    stream << std::endl;
    std::copy(view.member_ips.begin(), view.member_ips.end(), std::ostream_iterator<ip_addr>(stream, " "));
    stream << std::endl;
    for(const auto& fail_val : view.failed) {
        stream << (fail_val ? "T" : "F") << " ";
    }
    stream << std::endl;
    stream << view.nFailed << endl;
    stream << view.num_members << endl;
    stream << view.my_rank << endl;
    return stream;
}
}

