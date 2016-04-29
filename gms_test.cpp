/*
 * gms_test.cpp
 *
 *  Created on: Apr 26, 2016
 *      Author: edward
 */

#include <iostream>
#include <string>
#include <cstdlib>
#include <map>


using std::string;
using std::cin;
using std::cout;
using std::endl;
using std::map;

#include "derecho_group.h"
#include "experiments/block_size.h"
#include "rdmc/util.h"
#include "managed_group.h"
#include "view.h"

static const int GMS_PORT = 12345;

int main (int argc, char *argv[]) {

    if(argc < 2) {
        cout << "Error: Expected leader's node ID as the first argument." << endl;
        return -1;
    }
    uint32_t server_rank = std::atoi(argv[1]);

    uint32_t node_rank;
    uint32_t num_nodes;

    map<uint32_t, std::string> node_addresses;

    query_addresses(node_addresses, node_rank);
    num_nodes = node_addresses.size();
    long long unsigned int max_msg_size = 100;
    long long unsigned int block_size = 10;

    int num_messages = 1000;

    auto stability_callback = [] (int sender_id, long long int index, char *buf, long long int msg_size) {cout << "Sender: " << sender_id << ", index: " << index << endl;};


    derecho::ManagedGroup managed_group(GMS_PORT, node_addresses, node_rank, server_rank, max_msg_size, stability_callback, block_size);

    for (int i = 0; i < num_messages; ++i) {
        auto& g = managed_group.current_derecho_group();
        // random message size between 1 and 100
        unsigned int msg_size = (rand()%7 + 2) * 10;
        char *buf = g.get_position (msg_size);
        while (!buf) {
            buf= g.get_position (msg_size);
        }
        for (unsigned int j = 0; j < msg_size; ++j) {
            buf[j] = 'a'+i;
        }
        g.send();
    }
    while (true) {
    }


    managed_group.leave();
}
