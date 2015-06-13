#ifndef _DATABUS_UTIL_H
#define _DATABUS_UTIL_H 1

#include <list>
#include <mutex>
#include <condition_variable>
#include <iostream>

namespace databus {
  template <typename T>
  class List {
   public:
    List() {}
    void push_back(const T& val) {
      std::lock_guard<std::mutex> lk(mutex_);
      list_.push_back(val);
      cv_.notify_one();
    }

    T pop_front() {
      std::unique_lock<std::mutex> lk(mutex_);
      cv_.wait(lk, [this] { return !list_.empty(); });
      T it = list_.front();
      list_.pop_front();
      return it;
    }

    bool empty() {
      std::lock_guard<std::mutex> lk(mutex_);
      return list_.empty();
    }

    size_t size() {
      std::lock_guard<std::mutex> lk(mutex_);
      return list_.size();
    }

   private:
    std::list<T> list_;
    std::mutex mutex_;
    std::condition_variable cv_;
  };

  template <typename T>
  void ReportList(List<T>& list, const char* name) {
    std::cout << name << list.size() << std::endl;
  }
}

#endif
