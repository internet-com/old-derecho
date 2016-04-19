#ifndef LOG_RESULTS_H
#define LOG_RESULTS_H

#include <fstream>

template <class params>
void log_results (params t, std::string filename) {
  std::ofstream fout;
  fout.open(filename, std::ofstream::app);
  t.print(fout);
  fout.close();
}

void log_results (long long unsigned int msg_size, double avg_bw, std::string filename) {
  std::ofstream fout;
  fout.open(filename, std::ofstream::app);
  fout << msg_size << " " << avg_bw << std::endl;
  fout.close();
}
#endif
