#include "log_manager.h"

#include "global.h"

#include "log_buffer.h"

#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>

#include <string>


LogManager::LogManager()
{   
    log_buffer_num_ = g_log_buffer_num;
    log_buffers_ = new LogBuffer* [log_buffer_num_];
    for (uint64_t i = 0; i < log_buffer_num_; i++)
    {
        std::string log_file_path = log_file_paths.at(i);
        uint64_t    log_fd = open(log_file_path.c_str(), O_TRUNC | O_WRONLY | O_CREAT, 0644);

        printf("log_fd: %ld\n", log_fd);

        log_buffers_[i] = new LogBuffer(log_fd, i);
    }
}

LogManager::~LogManager()
{

}


LogBuffer* LogManager::GetLogBuffer(LogBufID log_buf_id)
{
    return log_buffers_[log_buf_id];
}


LogBufID LogManager::GetLogBufID(ThreadID thread_id, DBThreadType thread_type)
{
    LogBufID log_buf_id = 0;
    switch (thread_type)
    {
    case TXN_THREAD_T:
        {
            log_buf_id = thread_id % log_buffer_num_;
            break;
        }
    case LOGGER_THREAD_T:
        {
            log_buf_id = thread_id - g_txn_thread_num;
            break;
        }
    default:
        break;
    }

    return log_buf_id;
}

