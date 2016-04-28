#include <fcntl.h>
#include <unistd.h>
#include <iostream>
#include <cstdio>
#include <time.h>
#include <memory>

using std::cout;
using std::cin;
using std::endl;

int main () {
  srand(time(NULL));
  long long unsigned buffer_size = 10000000;
  std::unique_ptr<char[]> buffer(new char[buffer_size]);
  char *buf = buffer.get();
  for (int j = 0; j < buffer_size; ++j) {
    buf[j] = rand ()%26 + 'a';
  }
  long long int num_messages = 4000;
  int fd = open("messages", O_WRONLY | O_CREAT, 0777);
  struct timespec start_time;
  // start timer
  clock_gettime(CLOCK_REALTIME, &start_time);
  for (int i = 0; i < num_messages; ++i) {
    write (fd, buf, buffer_size);
  }
  struct timespec end_time;
  clock_gettime(CLOCK_REALTIME, &end_time);
  close(fd);
  long long int nanoseconds_elapsed = (end_time.tv_sec-start_time.tv_sec)*(long long int)1e9 + (end_time.tv_nsec-start_time.tv_nsec);
  double bw = (buffer_size * (long long int) num_messages * (long long int) 8 + 0.0)/nanoseconds_elapsed;
  cout << bw << endl;
}
