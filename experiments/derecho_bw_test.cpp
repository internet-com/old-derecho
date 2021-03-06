#include <iostream>
#include <fstream>
#include <vector>
#include <map>
#include <time.h>
#include <memory>

#include "../derecho_group.h"
#include "../managed_group.h"
#include "../view.h"
#include "block_size.h"
#include "../rdmc/util.h"
#include "aggregate_bandwidth.h"
#include "log_results.h"

static const int GMS_PORT = 12345;

using std::vector;
using std::map;
using std::cout;
using std::endl;

using namespace derecho;

int count = 0;

struct test1_str {
    int test1(string str) {
        cout << str << endl;
        count++;
        if(count == 2) {
            cout << "Exiting" << endl;
            exit(0);
        }
        return 19954;
    }

    template <typename Dispatcher>
    auto register_functions(Dispatcher &d) {
        return d.register_functions(this, &test1_str::test1);
    }
};

int main(int argc, char *argv[]) {
    srand(time(NULL));

    uint32_t server_rank = 0;
    uint32_t node_rank;
    uint32_t num_nodes;

    map<uint32_t, std::string> node_addresses;

    query_addresses(node_addresses, node_rank);
    num_nodes = node_addresses.size();

    vector<uint32_t> members(num_nodes);
    for(uint32_t i = 0; i < num_nodes; ++i) {
        members[i] = i;
    }

    long long unsigned int max_msg_size = atoll(argv[1]);
    long long unsigned int block_size = get_block_size(max_msg_size);
    int num_senders_selector = atoi(argv[2]);
    int num_messages = 1000;
    max_msg_size -= 16;

    bool done = false;
    auto stability_callback = [
        &num_messages,
        &done,
        &num_nodes,
        num_senders_selector,
        num_last_received = 0u
    ](int sender_id, long long int index, char *buf,
      long long int msg_size) mutable {
        cout << "In stability callback; sender = " << sender_id
             << ", index = " << index << endl;
        if(num_senders_selector == 0) {
            if(index == num_messages - 1 && sender_id == (int)num_nodes - 1) {
                done = true;
            }
        } else if(num_senders_selector == 1) {
            if(index == num_messages - 1) {
                ++num_last_received;
            }
            if(num_last_received == num_nodes / 2) {
                done = true;
            }
        } else {
            if(index == num_messages - 1) {
                done = true;
            }
        }
    };

    Dispatcher<test1_str> group_handlers(node_rank, std::make_tuple());
    derecho::ManagedGroup<decltype(group_handlers)> managed_group(
        GMS_PORT, node_addresses, node_rank, server_rank, max_msg_size,
        derecho::CallbackSet{stability_callback, nullptr},
        std::move(group_handlers), {}, block_size);

    cout << "Finished constructing/joining ManagedGroup" << endl;

    while(managed_group.get_members().size() < num_nodes) {
    }
    auto members_order = managed_group.get_members();
    cout << "The order of members is :" << endl;
    for(auto id : members_order) {
        cout << id << " ";
    }
    cout << endl;

    auto send_all = [&]() {
        for(int i = 0; i < num_messages; ++i) {
            cout << "Asking for a buffer" << endl;
            char *buf = managed_group.get_sendbuffer_ptr(max_msg_size);
            while(!buf) {
                buf = managed_group.get_sendbuffer_ptr(max_msg_size);
            }
            cout << "Obtained a buffer, sending" << endl;	    
            managed_group.send();
        }
    };
    auto send_one = [&]() {
        char *buf = managed_group.get_sendbuffer_ptr(1, num_messages);
        while(!buf) {
            buf = managed_group.get_sendbuffer_ptr(1, num_messages);
        }
        managed_group.send();
    };

    struct timespec start_time;
    // start timer
    clock_gettime(CLOCK_REALTIME, &start_time);
    if(num_senders_selector == 0) {
        send_all();
    } else if(num_senders_selector == 1) {
        if(node_rank > (num_nodes - 1) / 2) {
            send_all();
        } else {
            send_one();
        }
    } else {
        if(node_rank == num_nodes - 1) {
            // cout << "Sending all messages" << endl;
            send_all();
        } else {
            // cout << "Sending one message" << endl;
            send_one();
        }
    }
    while(!done) {
    }
    struct timespec end_time;
    clock_gettime(CLOCK_REALTIME, &end_time);
    long long int nanoseconds_elapsed =
        (end_time.tv_sec - start_time.tv_sec) * (long long int)1e9 +
        (end_time.tv_nsec - start_time.tv_nsec);
    double bw;
    if(num_senders_selector == 0) {
        bw = (max_msg_size * num_messages * num_nodes + 0.0) /
             nanoseconds_elapsed;
    } else if(num_senders_selector == 1) {
        bw = (max_msg_size * num_messages * (num_nodes / 2) + 0.0) /
             nanoseconds_elapsed;
    } else {
        bw = (max_msg_size * num_messages + 0.0) / nanoseconds_elapsed;
    }
    double avg_bw = aggregate_bandwidth(members, node_rank, bw);
    log_results(num_nodes, num_senders_selector, max_msg_size, avg_bw,
                "data_derecho_bw");

    managed_group.barrier_sync();
    std::string log_filename =
        (std::stringstream() << "events_node" << node_rank << ".csv").str();
    std::ofstream logfile(log_filename);
    managed_group.print_log(logfile);
    managed_group.leave();
    cout << "Finished destroying managed_group" << endl;
}
