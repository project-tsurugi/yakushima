/**
 * @file concurrent_queue.h
 */

#include <tbb/concurrent_queue.h>

namespace yakushima {

template<class T>
class concurrent_queue { // NOLINT : about constructor
                         // Begin : intel tbb
public:
    bool empty() { return queue_.empty(); }

    void push(const T& elem) { queue_.push(elem); }

    bool try_pop(T& result) { return queue_.try_pop(result); }

private:
    tbb::concurrent_queue<T> queue_;
    // End : intel tbb
};

} // namespace yakushima