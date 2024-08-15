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

namespace yakushima {

class destroy_barrier {
public:
    destroy_barrier() : destroy_barrier([](){}) {
    }
    explicit destroy_barrier(const std::function<void(void)> f) : cleanup_(f) {
    }
    void wait() {
        std::unique_lock<std::mutex> lock(mtx_);
        while(true) {
            if (job_count_ == 0) {
                return;
            }
            cond_.wait(lock, [this](){ return job_count_ == 0; });
        }
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
    mutable std::condition_variable cond_{};
};

class destroy_manager {
public:
    explicit destroy_manager(bool inactive) : inactive_(inactive) {
        if (!inactive_) {
            std::size_t max = std::thread::hardware_concurrency();
            if (max > 1) {
                threads_.reserve(max);
                for (std::size_t i = 0; i < max; i++) {
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
        std::unique_lock<std::mutex> lock(mtx_);
        works_.emplace_back(f);
        cond_.notify_one();
    }

    void operator()() {
        while (true) {
            std::unique_lock<std::mutex> lock(mtx_);
            cond_.wait(lock, [this](){ return !works_.empty() || finished_.load(); });
            if (works_.empty() && finished_.load()) {
                return;
            }
            if (!works_.empty()) {
                auto f = works_.front();
                works_.pop_front();
                lock.unlock();
                f();
            }
        }
    }

private:
    bool inactive_;
    mutable std::mutex mtx_{};
    mutable std::condition_variable cond_{};
    std::vector<std::thread> threads_{};
    std::deque<std::function<void(void)>> works_{};
    std::atomic_bool finished_{};
};

} // namespace yakushima
