/*
 * persistence.h
 *
 *  Created on: Jun 20, 2016
 *      Author: edward
 */

#pragma once

#include "derecho_row.h"

namespace derecho {

namespace persistence {


struct message {
        char *data;
        uint64_t length;

        uint32_t sender;
        uint64_t index;
};


struct __attribute__((__packed__)) header {
        uint8_t magic[8];
        uint32_t version;
};

struct __attribute__((__packed__)) message_metadata {
        uint32_t sender;
        uint32_t padding;

        uint64_t index;

        uint64_t offset;
        uint64_t length;
};

} // namespace persistence
} // namespace derecho
