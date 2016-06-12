
#include "filewriter.h"

#include <cstring>
#include <thread>
#include <iostream>
#include <fstream>

using std::mutex;
using std::unique_lock;
using std::ofstream;

namespace derecho {

const uint8_t MAGIC_NUMBER[8] = {'D', 'E', 'R', 'E', 'C', 'H', 'O', 29};

FileWriter::FileWriter(std::function<void(message)> _message_written_upcall,
                       std::string filename)
    : message_written_upcall(_message_written_upcall),
      exit(false),
      writer_thread(&FileWriter::perform_writes, this, filename),
      callback_thread(&FileWriter::issue_callbacks, this) {}

FileWriter::~FileWriter() {
    {
        //must hold both mutexes to change exit, since either thread could be about to read it before calling wait()
        unique_lock<mutex> writes_lock(pending_writes_mutex);
        unique_lock<mutex> callbacks_lock(pending_callbacks_mutex);
        exit = true;
    }
    pending_callbacks_cv.notify_all();
    pending_writes_cv.notify_all();
    if(writer_thread.joinable())
        writer_thread.join();
    if(callback_thread.joinable())
        callback_thread.join();
}

void FileWriter::perform_writes(std::string filename) {
//    std::cout << "perform_writes thread forked" << std::endl;
  ofstream data_file(filename);
  ofstream metadata_file(filename + ".metadata");

  unique_lock<mutex> writes_lock(pending_writes_mutex);

  uint64_t current_offset = 0;

  header h;
  memcpy(h.magic, MAGIC_NUMBER, sizeof(MAGIC_NUMBER));
  h.version = 0;
  metadata_file.write((char *)&h, sizeof(h));

  while (!exit) {
    pending_writes_cv.wait(writes_lock);

    if (!pending_writes.empty()) {
      message m = pending_writes.front();
      pending_writes.pop();
//      std::cout << "About to write message " << m.index << " from sender " << m.sender << std::endl;

      message_metadata metadata;
      metadata.sender = m.sender;
      metadata.index = m.index;
      metadata.offset = current_offset;
      metadata.length = m.length;

      data_file.write(m.data, m.length);
      metadata_file.write((char *)&metadata, sizeof(metadata));

      data_file.flush();
      metadata_file.flush();

      current_offset += m.length;

      {
        unique_lock<mutex> callbacks_lock(pending_callbacks_mutex);
//        std::cout << "Binding a callback" << std::endl;
        pending_callbacks.push(std::bind(message_written_upcall, m));
      }
      pending_callbacks_cv.notify_all();
    }
  }
}

void FileWriter::issue_callbacks() {
//    std::cout << "issue_callbacks thread forked" << std::endl;
  unique_lock<mutex> lock(pending_callbacks_mutex);

  while (!exit) {
    pending_callbacks_cv.wait(lock);


    if (!pending_callbacks.empty()) {
//        std::cout << "FileWriter woke up and is about to issue a callback" << std::endl;
      auto callback = pending_callbacks.front();
      pending_callbacks.pop();
      callback();
    }
  }
}

void FileWriter::write_message(message m) {
  {
    unique_lock<mutex> lock(pending_writes_mutex);
    pending_writes.push(m);
//    std::cout << "Message submitted for writing: " << m.index << " from sender " << m.sender << std::endl;
  }
  pending_writes_cv.notify_all();
}

}
