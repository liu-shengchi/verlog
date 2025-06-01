#include "log_manager.h"

#include "global.h"

#include "log_buffer.h"
#include "serial_log.h"

#include "tpcc_schema.h"
#include "tuple.h"

#include "db_latch.h"

#include "string.h"

#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>

using namespace std;


LogManager::LogManager()
{
    log_buffer_num_ = g_log_buffer_num;
    for (uint64_t i = 0; i < log_buffer_num_; i++)
    {
        std::string log_file_path = log_file_paths.at(i);
        // uint64_t    log_fd = open(log_file_path.c_str(), O_TRUNC | O_RDWR | O_CREAT | O_DIRECT, 0644);
        uint64_t    log_fd = open(log_file_path.c_str(), O_TRUNC | O_RDWR | O_CREAT, 0644);

        printf("log_fd: %ld\n", log_fd);

        log_buffers_[i] = new LogBuffer(log_fd, i);
        COMPILER_BARRIER
        log_buf_start_ptr_[i] = log_buffers_[i]->buffer_mem_ptr_;
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
            log_buf_id = (thread_id - txn_thread_id_offset) % log_buffer_num_;
            break;
        }
    case LOGGER_THREAD_T:
        {
            log_buf_id = thread_id - logger_thread_id_offset;
            break;
        }
    default:
        break;
    }

    return log_buf_id;
}
