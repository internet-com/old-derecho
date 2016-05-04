/*
 * logger.cpp
 *
 *  Created on: May 3, 2016
 *      Author: edward
 */

#include "logger.h"
#include "managed_group.h"

namespace derecho {

std::chrono::high_resolution_clock::time_point program_start_time;

namespace util {

void Logger::log_event(std::string event_text) {
    std::lock_guard<std::mutex> lock(log_mutex);
    auto currtime = std::chrono::high_resolution_clock::now();
    times[counter] = std::chrono::duration_cast<std::chrono::microseconds>(currtime-derecho::program_start_time).count();
    events[counter] = event_text;
    counter++;
}

void Logger::log_event(const std::stringstream& event_text){
    log_event(event_text.str());
}

} /* namespace util */
} /* namespace derecho */
