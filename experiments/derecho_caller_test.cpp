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

struct test1_str{
	int test1 (string str) {
		cout << str << endl;
		count++;
		if (node_rank == 3 && count == 2) {
			cout << "Exiting" << endl;
			exit(0);
		}
		return 19954;
	}
	
	template<typename Dispatcher>
	auto register_functions(Dispatcher &d){
		return d.register_functions(
			this, &test1_str::test1);
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

    auto stability_callback =
        [](int sender_id, long long int index, char *buf,
           long long int msg_size) {};
	
	Dispatcher<test1_str> group_handlers(node_rank,std::make_tuple());

    derecho::ManagedGroup<decltype(group_handlers)> managed_group(
        GMS_PORT, node_addresses, node_rank, server_rank, max_msg_size,
        stability_callback, std::move(group_handlers),
        {[](vector<derecho::node_id_t> new_members, vector<derecho::node_id_t> old_members) {
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

    while(managed_group.get_members().size() < num_nodes) {
    }
    auto members_order = managed_group.get_members();
    cout << "The order of members is :" << endl;
    for(auto id : members_order) {
        cout << id << " ";
    }
    cout << endl;

    // string str = "Here is a message";
    // auto fut = managed_group.template orderedQuery<0>({}, str);
    // auto& rmap = fut.get();
    // cout << "Obtained a reply map" << endl;
    // for (auto it = rmap.begin(); it != rmap.end(); ++it) {
    //   try {
    // 	cout << "Reply from node " << it->first << ": " << it->second.get() << endl;
    //   }
    //   catch (const std::exception &e) {
    // 	cout << e.what() << endl;
    //   }
    // }
    
    // int a = rmap.get(0);
    // cout << "Reply from node 0: " << a << endl;
    // int b = rmap.get(1);
    // cout << "Reply from node 1: " << b << endl;
    // auto fut = managed_group.template p2pQuery<0>(1 - node_rank, node_rank,
    // index, buf, msg_size);

    cout << "Done" << endl;

    while (true) {

    }
    // cout << "Obtained value: " << fut.get(0) << endl;
    // cout << "Obtained value: " << fut.get(1) << endl;
    // cout << "Obtained value: " << fut.get(1 - node_rank) << endl;
}
