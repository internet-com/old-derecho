/**
 * @file log_tail_length.cpp
 * A small executable that calculates the length, in bytes, of the tail of a
 * Derecho log file, from the provided message number to the end. Since message
 * numbers are an ordered pair (sender, index), the message number is provided
 * as two (whitespace-separated) input arguments.
 * @date Aug 1, 2016
 * @author edward
 */

#include <iostream>
#include <fstream>
#include <string>
#include <cstdlib>
#include <cstdint>

#include "persistence.h"
#include "serialization/SerializationSupport.hpp"

using namespace derecho::persistence;

int main(int argc, char* argv[]) {
    if(argc < 4) {
        std::cout << "Usage: latest_logged_message <filename> <sender> <index>" << std::endl;
        return 1;
    }
    std::string filename(argv[1]);
    uint32_t target_sender = std::atoi(argv[2]);
    uint32_t target_index = std::atoi(argv[3]);

    uint64_t target_offset = 0;
    uint64_t target_size = 0;
    std::ifstream metadata_file(filename + METADATA_EXTENSION);
    message_metadata dummy_for_size;
    std::size_t size_of_metadata = mutils::bytes_size(dummy_for_size);
    char buffer[size_of_metadata];
    //Read the header to get past it
    header file_header;
    metadata_file.read((char*)&file_header, sizeof(file_header));
    //Scan through the metadata blocks until we find the one with the right sequence number
    uint32_t sender = 0, index = 0;
    while(!(sender == target_sender && index == target_index) && metadata_file) {
        metadata_file.read(buffer, size_of_metadata);
        auto metadata = mutils::from_bytes<message_metadata>(nullptr, buffer);
        sender = metadata->sender;
        index = metadata->index;
        target_offset = metadata->offset;
        target_size = metadata->length;
    }
    //Get the size of the log file, so we can subtract from it. Annoyingly complicated.
    std::streamoff log_size = 0;
    std::ifstream logfile(filename, std::ios::binary);
    log_size = logfile.tellg();
    logfile.seekg(0, std::ios::end);
    log_size = logfile.tellg() - log_size;
    logfile.close();

    auto endoftarget = target_offset + target_size;
    auto distance = log_size - endoftarget;
    std::cout << distance << std::endl;
    return 0;
}
