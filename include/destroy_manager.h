/**
 * @file destroy_manager.h
 */
#pragma once

#include <atomic>
#include <thread>
#include <vector>
#include <deque>
#include <functional>
#include <mutex>
#include <condition_variable>

#include <boost/fiber/all.hpp>
#include <boost/fiber/detail/thread_barrier.hpp>

namespace yakushima {

class destroy_barrier {
public:
    destroy_barrier() : destroy_barrier([](){}) {
    }
    explicit destroy_barrier(const std::function<void(void)> f) : cleanup_(f) {
    }
    void wait() {
        std::unique_lock<std::mutex> lock(mtx_);
        cond_.wait(lock, [this](){ return 0 == job_count_; });
    }
    void begin() {
        job_count_++;
    }
    void end() {
        job_count_--;
        if (job_count_ == 0) {
            cleanup_();
            cond_.notify_all();
        }
    }

private:
    std::function<void(void)> cleanup_;
    std::atomic_uint job_count_{};
    mutable std::mutex mtx_{};
    mutable boost::fibers::condition_variable_any cond_{};
};

class destroy_manager {
public:
    constexpr static size_t max_threads = 4;
    explicit destroy_manager(bool inactive) : inactive_(inactive) {
        if (!inactive_) {
            num_threads_ = std::thread::hardware_concurrency() < max_threads ? std::thread::hardware_concurrency() : max_threads;
            boost::fibers::use_scheduling_algorithm< boost::fibers::algo::work_stealing >(num_threads_);
            if (num_threads_ > 1) {
                threads_.reserve(num_threads_ - 1);
                for (std::size_t i = 0; i < num_threads_ - 1; i++) {
                    threads_.emplace_back(std::thread(std::ref(*this)));
                }
            } else {
                inactive_ = true;
            }
        }
    }
    destroy_manager() : destroy_manager(false) {
    }
    ~destroy_manager() {
        if (!inactive_) {
            {
                std::unique_lock<std::mutex> lock(mtx_);
                finished_.store(true);
                lock.unlock();
                cond_.notify_all();
            }
            for( auto&& e: threads_) {
                e.join();
            }
        }
    }

    destroy_manager(destroy_manager const&) = delete;
    destroy_manager(destroy_manager&&) = delete;
    destroy_manager& operator = (destroy_manager const&) = delete;
    destroy_manager& operator = (destroy_manager&&) = delete;

    void put(const std::function<void(void)>& f) {
        if (inactive_) {
            f();
            return;
        }
        boost::fibers::fiber([this, f](){
            fiber_count_++;
            f();
            if ( 0 == --fiber_count_) {
                std::unique_lock<std::mutex> lock(mtx_);
                if ( 0 == --fiber_count_) {
                    lock.unlock();
                    cond_.notify_all();
                }
            }
        }).detach();
    }

    void operator()() {
        boost::fibers::use_scheduling_algorithm< boost::fibers::algo::work_stealing >(num_threads_);
        std::unique_lock<std::mutex> lock(mtx_);
        cond_.wait(lock, [this](){ return 0 == fiber_count_.load() && finished_.load(); });
    }

private:
    bool inactive_;
    mutable std::mutex mtx_{};
    mutable boost::fibers::condition_variable_any cond_{};
    std::vector<std::thread> threads_{};
    std::atomic_bool finished_{};
    std::atomic_uint fiber_count_{0};
    std::size_t num_threads_{};
};

} // namespace yakushima
