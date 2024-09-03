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

#include <boost/thread.hpp>

namespace yakushima {

class alignas(CACHE_LINE_SIZE) manager {
public:
    class worker;

    class alignas(CACHE_LINE_SIZE) barrier {
      public:
        explicit barrier(manager* m) : manager_(m), worker_(manager_->get_worker()) {
        }

        void push(std::function<void(void)>&& func) {
            fork();
            auto* next_worker = manager_->next_worker();
            if (next_worker->has_room()) {
                next_worker->push(this, std::move(func));
            } else {
                worker_->push(this, std::move(func));
            }
        }
        void join() {
            auto s = status_.fetch_sub(1) - 1;
            if (s == WAITING) {
                worker_->push(this);
            }
        }
        void wait() {
            if (auto s = status_.fetch_add(WAITING); s != 0) {
                if (!worker_) { std::abort(); }
                if (dispatched_) { std::abort(); }
                dispatched_ = true;
                worker_->dispatch(this);
            }
        }

    private:
        manager* manager_;
        worker* worker_;
        bool dispatched_{false};

        std::atomic_uint status_{0};
        static constexpr uint WAITING = 0x80000000;

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
        std::size_t nest_{};
        bool waiting_{};
        bool terminate_{};

        static constexpr std::size_t NEST_HARD_LIMIT = 64;
        static constexpr std::size_t NEST_SOFT_LIMIT = 32;

        void dispatch(barrier* b = nullptr) {
            nest_++;
            while (true) {
                std::unique_lock<std::mutex> lock(mtx_);
                if (!has_work() && !terminate_) {
                    waiting_ = true;
                    cond_.wait(lock, [this](){ return has_work() || terminate_; });
                    waiting_ = false;
                }
                lock.unlock();
                if (terminate_) {
                    return;
                }
                if (!completed_barriers_.empty()) {
                    std::unique_lock<std::mutex> lock(mtx_);
                    for (auto it = completed_barriers_.begin(); it != completed_barriers_.end(); it++) {
                        if (b == *it) {
                            completed_barriers_.erase(it);
                            nest_++;
                            return;
                        }
                    }
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
            }
        }

        void push(barrier* b, std::function<void(void)>&& func) {
            std::unique_lock<std::mutex> lock(mtx_);
            tasks_.emplace_front(std::make_pair(b, std::move(func)));
            cond_.notify_one();
        }
        void push(barrier* b) {
            std::unique_lock<std::mutex> lock(mtx_);
            completed_barriers_.emplace_front(b);
            cond_.notify_one();
        }

        [[nodiscard]] bool has_room() const {
            if (nest_ >= NEST_HARD_LIMIT) {
                return false;
            }
            if (nest_ < NEST_SOFT_LIMIT) {
                return true;
            }
            return waiting_;
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
    manager() : manager(boost::thread::physical_concurrency() - 1) {};  // FIXME how to obtain the number of CPU core
    explicit manager(std::size_t size) : size_{size} {
        for (std::size_t i = 0; i < size_; i++) {
            workers_.emplace_back(std::make_unique<worker>());
            threads_.emplace_back(std::thread(std::ref(*workers_.back())));
            indexes_.insert(std::make_pair(threads_.back().get_id(), i));
        }
        workers_.emplace_back(std::make_unique<worker>());
        indexes_.insert(std::make_pair(std::this_thread::get_id(), size_));
        size_++;
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
    worker* get_worker(std::thread::id id = std::this_thread::get_id()) {
        if( auto it = indexes_.find(id); it != indexes_.end()) {
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
