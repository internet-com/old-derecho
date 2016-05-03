
#include "derecho_group.h"
#include "managed_group.h"
#include "rdmc/util.h"
#include "view.h"

#include <chrono>
#include <cstdlib>
#include <iostream>
#include <map>
#include <string>
#include <thread>
#include <vector>

using namespace std;
using namespace std::chrono_literals;


const int GMS_PORT = 12345;
const uint64_t SECOND = 1000000000ull;
const size_t message_size = 200000000;
const size_t block_size = 1000000;

uint32_t num_nodes, node_rank;
map<uint32_t, std::string> node_addresses;

unsigned int message_number = 0;
vector<uint64_t> message_times;
uint64_t start_time;

shared_ptr<derecho::ManagedGroup> managed_group;

void stability_callback(int sender_id, long long int index, char *data, long long int size){
	message_times.push_back(get_time());

	while(!managed_group) {

	}

	unsigned int n = managed_group->get_members().size();
	if(message_number >= n){
		unsigned int dt = message_times.back() - message_times[message_number - n];
		cout << (get_time() - start_time) * 1e-9 << ", " << (message_size * n * 8.0) / dt << endl;
	}

	++message_number;
}

void send_messages(uint64_t duration){
	uint64_t end_time = get_time() + duration;
	while(get_time() < end_time){
		char* buffer = managed_group->get_sendbuffer_ptr(message_size);
		if(buffer){
			memset(buffer, rand() % 256, message_size);
//			cout << "Send function call succeeded at the client side" << endl;
			managed_group->send();
		}
	}
}

int main (int argc, char *argv[]) {
	query_addresses(node_addresses, node_rank);
	num_nodes = node_addresses.size();
    derecho::ManagedGroup::global_setup(node_addresses, node_rank);
	cout << endl << endl;

	start_time = get_time();
	if(node_rank == num_nodes - 1){
		cout << "Sleeping for 10 seconds..." << endl;
		std::this_thread::sleep_for(10s);
		cout << "Connecting to group" << endl;
		managed_group = make_shared<derecho::ManagedGroup>(GMS_PORT, node_addresses, node_rank, 0, message_size, stability_callback, block_size);
		cout << "About to start sending" << endl;
		send_messages(10 * SECOND);
		managed_group->log_event("About to exit");
		managed_group->print_log();
		exit(0);
	}else{
		managed_group = make_shared<derecho::ManagedGroup>(GMS_PORT, node_addresses, node_rank, 0, message_size, stability_callback, block_size);
		cout << "Created group, waiting for others to join." << endl;
		while(managed_group->get_members().size() < (num_nodes-1)) {
			std::this_thread::sleep_for(1ms);
		}
		send_messages(30 * SECOND);
		// managed_group->barrier_sync();
		managed_group->print_log();
		std::this_thread::sleep_for(5s);
		managed_group->leave();
	}
}
