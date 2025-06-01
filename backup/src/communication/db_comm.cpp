#include "db_comm.h"

#include <string.h>
#include <stdlib.h>
#include <unistd.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <sys/socket.h>


int DBComm::CreateSocketListen()
{
    uint64_t sock;
    struct sockaddr_in my_address;
    uint64_t on = 1;
    /* Socket Initialization */
    memset(&my_address,0,sizeof(my_address));
    my_address.sin_family      = AF_INET;
    my_address.sin_addr.s_addr = htonl(INADDR_ANY);
    my_address.sin_port        = htons(server_port);

    if ((sock = socket(PF_INET, SOCK_STREAM, IPPROTO_IP)) < 0) {
        //assert(1);
    }

    if ((setsockopt(sock, SOL_SOCKET, SO_REUSEADDR, &on, sizeof(on))) < 0) {
        //assert(1);
    }

    if (bind(sock, (struct sockaddr*)&my_address, sizeof(struct sockaddr)) < 0) {
        //assert(1);
    }

    if (listen(sock, 5))
    {
        printf("listen failed!\n");
    }

    return sock;  
}


int DBComm::AcceptSocketConnect(int sock)
{
    struct sockaddr_in client_address;
    uint64_t    fd;
    // struct timespec start, end;
    socklen_t sin_size = sizeof(struct sockaddr_in);
    while (true)
    {
        fd = accept(sock, (struct sockaddr *)&client_address, &sin_size);
        if (fd == -1)    //无效的socket链接
        {
            continue;
        }

        break;
    }

    return fd;
}


int DBComm::ConnectSocket(std::string remote_ip, uint32_t remote_port)
{
    uint64_t sock;
    struct sockaddr_in remote_address;
    struct timeval timeout = {3, 0};

    memset(&remote_address, 0, sizeof(remote_address));
    remote_address.sin_family = AF_INET;
    //socket链接，服务端ip地址、port端口号
    inet_aton(remote_ip.c_str(), (struct in_addr*)&remote_address.sin_addr);//socket连接 ip地址
    remote_address.sin_port = htons(remote_port);   //socket连接 port端口
    
    if ((sock = socket(PF_INET, SOCK_STREAM, 0)) < 0)
    {
        return -1;
    }
    
    setsockopt(sock, SOL_SOCKET, SO_RCVTIMEO, (const char*)&timeout, sizeof(timeout));
    while (connect(sock, (struct sockaddr*)&remote_address, sizeof(struct sockaddr)) != 0)
    {
        usleep(1000);//连接其他的server
    }

    return sock;
}
