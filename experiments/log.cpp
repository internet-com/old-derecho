#include "log.h"

#include <cassert>

ofstream fout;

string print(enum MSG_STAGES stage) {
  switch (stage) {
  case CONCEIVED_AT_CLIENT: return "CONCEIVED_AT_CLIENT";
  case BEING_GENERATED_AT_CLIENT: return "BEING_GENERATED_AT_CLIENT";
  case SENT_AT_CLIENT: return "SENT_AT_CLIENT";
  case SEND_CALLED_FROM_LOCAL_NODE: return "SEND_CALLED_FROM_LOCAL_NODE";
  case RECEIVED_AT_LOCAL_NODE: return "RECEIVED_AT_LOCAL_NODE";
  case DELIVERED_AT_LOCAL_NODE: return "DELIVERED_AT_LOCAL_NODE";
  default: assert(false);
  }
};

void print(list<msg_status_log> &trace, int my_rank) {
  // for (auto tr : trace) {
  //   tr.print();
  // }
  
  long long int sum[10], num[10];
  for (auto tr : trace) {
    
  }
}

msg_status_log::msg_status_log (enum MSG_STAGES _stage, int _sender_rank, long long int _index) {
  struct timespec cur_time;
  clock_gettime(CLOCK_REALTIME, &cur_time);
  time = cur_time.tv_sec*(long long int)1e9 + cur_time.tv_nsec;
  stage = _stage;
  sender_rank = _sender_rank;
  index = _index;
}

void msg_status_log::print () {
  fout << ::print(stage) << " " << sender_rank << " " << index << " " << time << std::endl;
}

bool compare(const msg_status_log &a, const msg_status_log &b) {
  return a.time < b.time;
}

void set_file (std::string filename) {
  fout.open(filename);
}
