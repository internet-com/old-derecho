/*
 * gms_test.cpp
 *
 *  Created on: Apr 26, 2016
 *      Author: edward
 */

#include <iostream>
#include <string>


using std::string;
using std::cin;
using std::cout;
using std::endl;

#include "derecho_group.h"
#include "experiments/block_size.h"
#include "managed_group.h"
#include "view.h"

static const int GMS_PORT = 12345;

int main (int argc, char *argv[]) {

    if(argc < 2) {
        cout << "You must provide at least one IP address. Local IP is required, server's IP is optional." << endl;
        return -1;
    }

    string my_ip(argv[1]);
    string server_ip(my_ip);
    if(argc > 2) {
        server_ip = string(argv[2]);
    }

    long long unsigned int msg_size;
    cout << "Enter the message size to test with: " ;
    cin >> msg_size;

    long long unsigned int block_size = get_block_size (msg_size);
    long long unsigned int buffer_size = msg_size * 10;
    cout << "Parameters: " << endl << "buffer_size=" << buffer_size << ", block_size=" << block_size << ", msg_size=" << msg_size << endl;
    int num_messages = 1000;


    int num_nodes = 8;
    bool done = false;
    auto stability_callback = [&num_messages, &done, &num_nodes] (int sender_id, long long int index, char *buf, long long int msg_size) {
        cout << "In stability callback; sender = " << sender_id << ", index = " << index << endl;
        if (index == num_messages-1 && sender_id == (int)num_nodes-1) {
            done = true;
        }
    };

    derecho::ManagedGroup managed_group(GMS_PORT, my_ip, server_ip, msg_size, stability_callback, block_size);

    for (int i = 0; i < num_messages; ++i) {
        derecho::DerechoGroup<derecho::View::N>& g = managed_group.current_derecho_group();
        char* buf = g.get_position (msg_size);
        while (!buf) {
            buf = g.get_position (msg_size);
        }
        for (unsigned int j = 0; j < msg_size; ++j) {
            buf[j] = 'a'+i;
        }
        g.send();
    }
    while (!done) {

    }

    managed_group.leave();
}
