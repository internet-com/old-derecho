#pragma once

#include "rdmc/connection.h"

#include <map>
#include <mutex>

namespace tcp {
using ip_addr_t = std::string;
using node_id_t = uint32_t;
class all_tcp_connections {
    std::mutex sockets_mutex;

    const uint32_t port;
    std::unique_ptr<connection_listener> conn_listener;
    std::map<node_id_t, socket> sockets;
    bool add_connection(const node_id_t my_id,
                                             const node_id_t other_id,
                                             const ip_addr_t& other_ip);
    void establish_node_connections(
        node_id_t my_id, const std::map<node_id_t, ip_addr_t>& ip_addrs);

public:
    all_tcp_connections(node_id_t my_id,
                        const std::map<node_id_t, ip_addr_t>& ip_addrs,
                        uint32_t _port);
    void destroy();
    bool tcp_write(node_id_t node_id, char const* buffer, size_t size);
    bool tcp_read(node_id_t node_id, char* buffer, size_t size);
    int32_t probe_all();
};
}
