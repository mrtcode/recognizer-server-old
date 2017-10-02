#ifndef RECOGNIZER_SERVER_LOG_H
#define RECOGNIZER_SERVER_LOG_H

#include "time.h"

#define log(level, level_name, fmt, ...) \
        if (level >= 0) { \
            char time_buf[20];\
            time_t now = time (0);\
            strftime (time_buf, sizeof(time_buf), "%Y-%m-%d %H:%M:%S", localtime (&now));\
            fprintf(stderr, "[%s] %s %s:%d (%s): " fmt "\n", level_name, time_buf, strrchr("/" __FILE__, '/') + 1, __LINE__, __func__, ##__VA_ARGS__); \
        }

#define log_debug(...) log(0, "dbg", ##__VA_ARGS__)
#define log_info(...) log(1, "nfo", ##__VA_ARGS__)
#define log_error(...) log(2, "err", ##__VA_ARGS__)

#endif //RECOGNIZER_SERVER_LOG_H
