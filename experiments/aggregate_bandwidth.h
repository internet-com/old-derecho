#ifndef AGGREGATE_BANDWIDTH_H
#define AGGREGATE_BANDWIDTH_H

#include <vector>

struct Result {
  double bw;
};
double aggregate_bandwidth (std::vector<int> members, uint32_t node_rank, double bw);
#endif
