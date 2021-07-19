#pragma once

namespace yakushima {

#ifndef YAKUSHIMA_EPOCH_TIME

// Resource management frequency [ms].
#define YAKUSHIMA_EPOCH_TIME 40

#endif

#ifndef YAKUSHIMA_MAX_PARALLEL_SESSIONS

// Max number of workers who can perform operations in parallel.
#define YAKUSHIMA_MAX_PARALLEL_SESSIONS 300

#endif

} // namespace yakushima