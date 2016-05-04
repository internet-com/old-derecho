#include "log.h"

#include <map>

#include <cassert>

using std::cout;
using std::endl;

std::ofstream fout;

std::string print(enum MSG_STAGES stage) {
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

void print(std::list<msg_status_log> &trace, int my_rank) {
  // for (auto tr : trace) {
  //   tr.print();
  // }
  
  std::vector<long long int> sum(10, 0), num(10, 0);
  std::map<std::pair<int, long long int>, long long int> msg_data;
  for (auto tr : trace) {
    auto p = std::pair <int, long long int> (tr.sender_rank, tr.index);
    if (tr.stage == CONCEIVED_AT_CLIENT) {
      msg_data[p] = tr.time;
    }
    else if (tr.stage == RECEIVED_AT_LOCAL_NODE && tr.sender_rank != my_rank) {
      msg_data[p] = tr.time;
    }
    else {
      assert(msg_data.find(p) != msg_data.end());
      sum[tr.stage] += tr.time-msg_data[p];
      num[tr.stage]++;
      msg_data[p] = tr.time;

      if (tr.stage == DELIVERED_AT_LOCAL_NODE) {
	msg_data.erase(p);
      }
    }
  }

  cout << sum[BEING_GENERATED_AT_CLIENT]/num[BEING_GENERATED_AT_CLIENT] << endl;
  cout << sum[SENT_AT_CLIENT]/num[SENT_AT_CLIENT] << endl;
  cout << sum[SEND_CALLED_FROM_LOCAL_NODE]/num[SEND_CALLED_FROM_LOCAL_NODE] << endl;
  cout << sum[RECEIVED_AT_LOCAL_NODE]/num[RECEIVED_AT_LOCAL_NODE] << endl;
  cout << sum[DELIVERED_AT_LOCAL_NODE]/num[DELIVERED_AT_LOCAL_NODE] << endl;
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
