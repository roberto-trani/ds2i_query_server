#ifndef INDEX_PARTITIONING_SOCKET_EXCEPTION_HPP
#define INDEX_PARTITIONING_SOCKET_EXCEPTION_HPP

#include <stdexcept>


namespace query_server {
    class SocketException : public std::runtime_error {
    public:
        SocketException(const char *message) : std::runtime_error(message) {}
    };

    class SocketConnectionClosedByPeerException : public SocketException {
    public:
        SocketConnectionClosedByPeerException() : SocketException("Connection closed by peer Exception") {}
    };

    class SocketMessageSizeException : public SocketException {
    public:
        SocketMessageSizeException() : SocketException("Message size Exception") {}
    };
}

#endif //INDEX_PARTITIONING_SOCKET_EXCEPTION_HPP
