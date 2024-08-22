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

class alignas(CACHE_LINE_SIZE) manager {
public:
    class worker;

    class alignas(CACHE_LINE_SIZE) barrier {
      public:
        barrier(manager& m) : manager_(m), worker_(manager_.get_worker()), parent_(nullptr) {
        }
        barrier(manager& m, barrier& b) : manager_(m), worker_(manager_.get_worker()), parent_(&b) {
        }

        void fork() {
            forks_++;
        }
        void join() {
            joins_++;
            if (parent_) {
                worker_->notify();
            } else {
                if (ready()) {
                    std::unique_lock<std::mutex> lock(mtx_);
                    cond_.notify_one();
                }
            }
        }
        void wait() {
            if (parent_) {
                manager_.get_worker()->push(this);
                base_fiber_ = std::move(worker_fiber_).resume();
                if (forks_ != joins_) {
                    std::abort();
                }
            } else {
                std::unique_lock<std::mutex> lock(mtx_);
                cond_.wait(lock, [this](){ return forks_ == joins_; });
                lock.unlock();
            }
        }
        bool ready() {
            return forks_.load() == joins_.load();
        }
        boost::context::fiber fiber() {
            return std::move(base_fiber_);
        }

    private:
        manager& manager_;
        worker* worker_;
        barrier* parent_;

        std::atomic_uint forks_{0};
        std::atomic_uint joins_{0};

        // for the first thread
        std::mutex mtx_{};
        std::condition_variable cond_{};

        boost::context::fiber base_fiber_{};
        boost::context::fiber worker_fiber_{
            [this](boost::context::fiber && f) {
                base_fiber_ = std::move(f);
                manager_.dispatch();
                return std::move(f);
            }
        };
        void fork(const std::function<void(void)> f) {
            forks_++;
            manager_.push(*this, f);
        }

        friend class manager;
        friend class worker;
    };

    class alignas(CACHE_LINE_SIZE) worker {
    public:
        worker() = delete;
        explicit worker(std::size_t id) : id_(id) {}

        void operator()() {
            worker_fiber_ = std::move(worker_fiber_).resume();
        }

    private:
        std::size_t id_;
        std::deque<boost::context::fiber> fibers_{};
        std::deque<barrier*> barriers_{};

        // for termination
        std::mutex mtx_{};
        std::condition_variable cond_{};
        bool terminate_{};
        barrier* current_barrier_{};

        boost::context::fiber base_fiber_{};
        boost::context::fiber worker_fiber_{
            [this](boost::context::fiber && f) {
                base_fiber_ = std::move(f);
                dispatch();
                return std::move(f);
            }
        };

        void push(boost::context::fiber&& f) {
            std::unique_lock<std::mutex> lock(mtx_);
            fibers_.emplace_back(std::move(f));
            lock.unlock();
            notify();
        }
        void push(barrier* b) {
            std::unique_lock<std::mutex> lock(mtx_);
            barriers_.emplace_front(b);
        }
        void dispatch() {
            while (true) {
                std::unique_lock<std::mutex> lock(mtx_);
                cond_.wait(lock, [this](){ return has_work() || terminate_; });
                lock.unlock();
                if (terminate_) {
                    base_fiber_ = std::move(base_fiber_).resume();
                }
                if (!barriers_.empty()) {
                    barrier* b{};
                    std::unique_lock<std::mutex> lock(mtx_);
                    for (auto it = barriers_.begin(); it != barriers_.end(); it++) {
                        if ((*it)->ready()) {
                            b = *it;
                            barriers_.erase(it);
                            break;
                        }
                    }
                    lock.unlock();
                    if (b) {
                        worker_fiber_ = std::move(b->fiber()).resume();
                    }
                }
                if (!fibers_.empty()) {
                    std::unique_lock<std::mutex> lock(mtx_);
                    auto f = std::move(fibers_.front());
                    fibers_.pop_front();
                    lock.unlock();
                    f = std::move(f).resume();
                }
            }
        }
        void notify() {
            std::unique_lock<std::mutex> lock(mtx_);
            cond_.notify_one();
        }
        bool terminate() {
            if (has_work()) {
                return false;
            }
            terminate_ = true;
            notify();
            return true;
        }
        bool has_work() {
            if (!fibers_.empty()) {
                return true;
            }
            for (auto it = barriers_.begin(); it != barriers_.end(); it++) {
                if ((*it)->ready()) {
                    return true;
                }
            }
            return false;
        }

        friend class manager;
        friend class barrier;
    };

// manager
    manager() = delete;
    ~manager() {
        for (auto&& w: workers_) {
            while(!w->terminate());
        }
        for (auto&& t: threads_) {
            t.join();
        }
    }
    explicit manager(std::size_t size) : size_{size} {
        for (std::size_t i = 0; i < size_; i++) {
            workers_.emplace_back(std::make_unique<worker>(i));
            threads_.emplace_back(std::thread(std::ref(*workers_.back())));
            indexes_.insert(std::make_pair(threads_.back().get_id(), i));
        }
    }
    void push(barrier& b, const std::function<void(void)> func) {
        auto i = index_.load();
        while(true) {
            if (index_.compare_exchange_strong(i, (i < (size_ - 1)) ? (i + 1) : 0)) {
                break;
            }
        }
        b.fork();
        auto& w = workers_.at(i);
        w->push(boost::context::fiber{
                [&b, func](boost::context::fiber && f) {
                    func();
                    b.join();
                    return std::move(f);
                }}
        );
    }
    void dispatch() {
        workers_.at(indexes_.at(std::this_thread::get_id()))->dispatch();
    }
    worker* get_worker() {
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
