#include <iostream>
#include <fstream>
#include <vector>
#include <map>
#include <time.h>
#include <memory>

#include "../derecho_group.h"
#include "../derecho_caller.h"
#include "../managed_group.h"
#include "../view.h"
#include "block_size.h"
#include "../rdmc/util.h"

static const int GMS_PORT = 12345;

using std::vector;
using std::map;
using std::string;
using std::cout;
using std::endl;

using derecho::DerechoGroup;
using derecho::DerechoRow;

uint32_t node_rank;
uint32_t num_nodes;

int count = 0;

struct test1_str {
    int state;
    int read_state(bool t) {
        cout << "Returning state, it is: " << state << endl;
        return state;
    }
    bool change_state(int new_state) {
        cout << "Previous state was: " << state << endl;
        state = new_state;
        cout << "Current state is: " << state << endl;
        return true;
    }

    template <typename Dispatcher>
    auto register_functions(Dispatcher &d) {
        return d.register_functions(this, &test1_str::read_state,
                                    &test1_str::change_state);
    }
};

int main(int argc, char *argv[]) {
    srand(time(NULL));

    uint32_t server_rank = 0;

    map<uint32_t, std::string> node_addresses;

    query_addresses(node_addresses, node_rank);
    num_nodes = node_addresses.size();

    vector<uint32_t> members(num_nodes);
    for(uint32_t i = 0; i < num_nodes; ++i) {
        members[i] = i;
    }

    long long unsigned int max_msg_size = 100;
    long long unsigned int block_size = get_block_size(max_msg_size);
    // int num_messages = 10;

    auto stability_callback = [](int sender_id, long long int index, char *buf,
                                 long long int msg_size) {};

    Dispatcher<test1_str> group_handlers(node_rank, std::make_tuple());

    derecho::ManagedGroup<decltype(group_handlers)> managed_group(
        GMS_PORT, node_addresses, node_rank, server_rank, max_msg_size,
        {stability_callback, {}}, std::move(group_handlers),
        {[](vector<derecho::node_id_t> new_members,
            vector<derecho::node_id_t> old_members) {
            cout << "New members are : " << endl;
            for(auto n : new_members) {
                cout << n << " ";
            }
            cout << endl;
            cout << "Old members were :" << endl;
            for(auto o : old_members) {
                cout << o << " ";
            }
            cout << endl;
        }},
        block_size);

    cout << "Finished constructing/joining ManagedGroup" << endl;
    
    // other nodes (first two) change each other's state
    if(node_rank != 2) {
      cout << "Changing each other's state to 35" << endl;
      auto fut = managed_group.template orderedQuery<test1_str, 0>({1-node_rank}, 35);
      auto &rmap = fut.get();
      cout << "Obtained a reply map" << endl;
      for(auto it = rmap.begin(); it != rmap.end(); ++it) {
	try {
	  cout << "Reply from node " << it->first << ": "
	       << it->second.get() << endl;
	} catch(const std::exception &e) {
	  cout << e.what() << endl;
	}
      }
    }

    while(managed_group.get_members().size() < num_nodes) {
    }
    
    // all members verify every node's state
    cout << "Reading everyone's state" << endl;
    auto fut = managed_group.template orderedQuery<test1_str, 0>({}, true);
    auto &rmap = fut.get();
    cout << "Obtained a reply map" << endl;
    for(auto it = rmap.begin(); it != rmap.end(); ++it) {
        try {
            cout << "Reply from node " << it->first << ": " <<
            it->second.get()
                 << endl;
        } catch(const std::exception &e) {
            cout << e.what() << endl;
        }
    }
    cout << "Done" << endl;
    cout << "Reached here" << endl;
    // wait forever
    while(true) {
    }
}
