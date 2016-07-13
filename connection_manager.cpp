#include "connection_manager.h"

#include <iostream>
#include <cassert>

namespace tcp {
bool all_tcp_connections::add_connection(const node_id_t my_id,
                                         const node_id_t other_id,
                                         const ip_addr_t& other_ip) {
    if(other_id < my_id) {
        try {
	  std::cout << "Changing sockets, key=" << other_id << std::endl;
            sockets[other_id] = socket(other_ip, port);
        } catch(exception) {
            std::cerr << "WARNING: failed to node " << other_id << " at "
                      << other_ip << ":" << port << std::endl;
            return false;
        }

        uint32_t remote_id = 0;
        if(!sockets[other_id].exchange(my_id, remote_id)) {
            std::cerr << "WARNING: failed to exchange rank with node "
                      << other_id << " at " << other_ip << ":" << port
                      << std::endl;
            sockets.erase(other_id);
            return false;
        } else if(remote_id != other_id) {
            std::cerr << "WARNING: node at " << other_ip << ":" << port
                      << " replied with wrong id (expected " << other_id
                      << " but got " << remote_id << ")" << std::endl;

            sockets.erase(other_id);
            return false;
        }
        return true;
    } else if(other_id > my_id) {
        try {
            socket s = conn_listener->accept();

            uint32_t remote_id = 0;
            if(!s.exchange(my_id, remote_id)) {
                std::cerr << "WARNING: failed to exchange id with node"
                          << std::endl;
                return false;
            } else {
	      std::cout << "Changing sockets, key=" << remote_id << std::endl;
                sockets[remote_id] = std::move(s);
                return true;
            }
        } catch(exception) {
            std::cerr << "Got error while attempting to listing on port"
                      << std::endl;
            return false;
        }
    }

    return false;
}

void all_tcp_connections::establish_node_connections(
    const node_id_t my_id, const std::map<node_id_t, ip_addr_t>& ip_addrs) {
    conn_listener = std::make_unique<connection_listener>(port);
    
    for(auto it = ip_addrs.begin(); it != ip_addrs.end(); it++) {
        if(it->first != my_id) {
            if(!add_connection(my_id, it->first, it->second)) {
                std::cerr << "WARNING: failed to connect to node " << it->first
                          << " at " << it->second << std::endl;
            }
        }
    }
}

all_tcp_connections::all_tcp_connections(
    node_id_t my_id, const std::map<node_id_t, ip_addr_t>& ip_addrs,
    uint32_t _port)
    : port(_port) {
    establish_node_connections(my_id, ip_addrs);
}

bool all_tcp_connections::tcp_write(node_id_t node_id, char const* buffer,
                                    size_t size) {
        const auto it = sockets.find(node_id);
        assert(it != sockets.end());
        return it->second.write(buffer, size);
}

bool all_tcp_connections::tcp_read(node_id_t node_id, char* buffer,
                                   size_t size) {
    const auto it = sockets.find(node_id);
    assert(it != sockets.end());
    return it->second.read(buffer, size);
}
}
