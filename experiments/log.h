#pragma once

#include <time.h>
#include <stdint.h>
#include <iostream>
#include <fstream>
#include <list>
#include <vector>

enum MSG_STAGES {
  CONCEIVED_AT_CLIENT,
  BEING_GENERATED_AT_CLIENT,
  SENT_AT_CLIENT,
  SEND_CALLED_FROM_LOCAL_NODE,
  RECEIVED_AT_LOCAL_NODE,
  DELIVERED_AT_LOCAL_NODE
};

class msg_status_log {
public:
  // in nanoseconds
  long long int time;
  enum MSG_STAGES stage;
  int sender_rank;
  long long int index;

  msg_status_log (enum MSG_STAGES _stage, int _sender_rank, long long int _index);
  void print();
};

bool compare(const msg_status_log &a, const msg_status_log &b);

void set_file (std::string filename);

void print(std::list<msg_status_log> &trace, int my_rank);
