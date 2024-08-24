/**
 * @file destroy_manager.h
 */
#pragma once

#include <thread>
#include <map>
#include <functional>
#include <atomic>
#include <deque>
#include <mutex>
#include <condition_variable>
#include <chrono>

namespace yakushima {

class alignas(CACHE_LINE_SIZE) manager {
public:
    class worker;

    class alignas(CACHE_LINE_SIZE) barrier {
      public:
        explicit barrier(manager* m) : barrier(m, nullptr) {}
        barrier(manager* m, barrier* b) : manager_(m), worker_(manager_->current_worker()), parent_(b) {}

        void push(std::function<void(void)>&& func) {
            fork();
            manager_->next_worker()->push(this, std::move(func));
        }
        void join() {
            auto s = status_.fetch_sub(1) - 1;
            if (parent_) {
                if (s == WAITING) {
                    worker_->push(this);
                }
            } else {
                if (s == 0) {
                    std::unique_lock<std::mutex> lock(mtx_);
                    cond_.notify_one();
                }
            }
        }
        void wait() {
            if (parent_) {
                if (status_.fetch_add(WAITING) != 0) {
                    worker_->dispatch(this);
                }
            } else {
                std::unique_lock<std::mutex> lock(mtx_);
                cond_.wait(lock, [this](){ return status_.load() == 0; });
            }
        }

      private:
        manager* manager_;
        worker* worker_;
        barrier* parent_;

        std::atomic_uint status_{0};
        static constexpr uint WAITING = 0x80000000;

        // for the source thread
        std::mutex mtx_{};
        std::condition_variable cond_{};

        inline void fork() {
            status_.fetch_add(1);
        }

        friend class manager;
        friend class worker;
    };

    class alignas(CACHE_LINE_SIZE) worker {
    public:
        worker() = default;

        void operator()() {
            dispatch();
        }

      private:
        std::deque<std::pair<barrier*, std::function<void(void)>>> tasks_{};
        std::deque<barrier*> completed_barriers_{};

        std::mutex mtx_{};
        std::condition_variable cond_{};
        bool terminate_{};

        void dispatch(barrier* b = nullptr) {
            while (true) {
                std::unique_lock<std::mutex> lock(mtx_);
                cond_.wait(lock, [this](){ return has_work() || terminate_; });
                lock.unlock();
                if (terminate_) {
                    return;
                }
                if (!tasks_.empty()) {
                    std::unique_lock<std::mutex> lock(mtx_);
                    auto& task = tasks_.front();
                    barrier* task_barrier= task.first;
                    auto task_func = task.second;
                    tasks_.pop_front();
                    lock.unlock();
                    task_func();
                    task_barrier->join();
                }
                if (!completed_barriers_.empty()) {
                    std::unique_lock<std::mutex> lock(mtx_);
                    for (auto it = completed_barriers_.begin(); it != completed_barriers_.end(); it++) {
                        if (b == *it) {
                            completed_barriers_.erase(it);
                            return;
                        }
                    }
                }
            }
        }

        void push(barrier* b, std::function<void(void)>&& func) {
            std::unique_lock<std::mutex> lock(mtx_);
            tasks_.emplace_front(std::make_pair(b, func));
            cond_.notify_one();
        }
        void push(barrier* b) {
            std::unique_lock<std::mutex> lock(mtx_);
            completed_barriers_.emplace_front(b);
            cond_.notify_one();
        }

        bool terminate() {
            if (has_work()) {
                return false;
            }
            terminate_ = true;
            std::unique_lock<std::mutex> lock(mtx_);
            cond_.notify_one();
            return true;
        }
        bool has_work() {
            return !tasks_.empty() || !completed_barriers_.empty();
        }

        friend class manager;
        friend class barrier;
    };

// manager
    manager() = delete;
    explicit manager(std::size_t size) : size_{size} {
        for (std::size_t i = 0; i < size_; i++) {
            workers_.emplace_back(std::make_unique<worker>());
            threads_.emplace_back(std::thread(std::ref(*workers_.back())));
            indexes_.insert(std::make_pair(threads_.back().get_id(), i));
        }
    }
    ~manager() {
        for (auto&& w: workers_) {
            while(!w->terminate());
        }
        for (auto&& t: threads_) {
            t.join();
        }
    }

    manager(manager const&) = delete;
    manager(manager&&) = delete;
    manager& operator = (manager const&) = delete;
    manager& operator = (manager&&) = delete;

    worker* next_worker() {
        return workers_.at((index_.fetch_add(1)) % size_).get();
    }
    worker* current_worker() {
        if( auto it = indexes_.find(std::this_thread::get_id()); it != indexes_.end()) {
            return workers_.at(it->second).get();
        }
        return nullptr;
    }

private:
    std::size_t size_;
    std::vector<std::unique_ptr<worker>> workers_{};
    std::vector<std::thread> threads_{};
    std::map<std::thread::id, std::size_t> indexes_{};
    std::atomic_ulong index_{};
};

using barrier = manager::barrier;

} // namespace yakushima
