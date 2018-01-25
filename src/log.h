#include <sys/types.h>
#include <sys/syscall.h>
#include <unistd.h>
#include <time.h>
#include "defines.h"

extern int log_level;

#define log(level, level_name, fmt, ...) \
        if (level >= log_level) { \
            char time_buf[20];\
            time_t now = time (0);\
            strftime (time_buf, sizeof(time_buf), "%Y-%m-%d %H:%M:%S", localtime (&now));\
            fprintf(stderr, "[%d] [%s] %s %s:%d (%s): " fmt "\n", syscall(__NR_gettid), level_name, time_buf, strrchr("/" __FILE__, '/') + 1, __LINE__, __func__, ##__VA_ARGS__); \
        }

#define log_debug(...) log(0, "dbg", ##__VA_ARGS__)
#define log_info(...) log(1, "nfo", ##__VA_ARGS__)
#define log_error(...) log(2, "err", ##__VA_ARGS__)
