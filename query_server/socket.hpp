#ifndef INDEX_PARTITIONING_SOCKET_HPP
#define INDEX_PARTITIONING_SOCKET_HPP

#include <boost/asio.hpp>
#include <boost/bind.hpp>
#include <cstdlib>
#include <iostream>

#include "socket_exception.hpp"


namespace query_server {
    using boost::asio::ip::tcp;

    class SocketServer;

    class Socket {
    public:
        unsigned int receive_message(void *buffer, std::size_t buffer_size) {
            unsigned int message_size;

            this->read_n_bytes(&message_size, sizeof(unsigned int));

            if (message_size > buffer_size) {
                throw SocketMessageSizeException();
            }

            this->read_n_bytes(buffer, message_size);
            return message_size;
        }

        void send_message(const void *data, unsigned int size_in_bytes) {
            this->write_n_bytes(&size_in_bytes, sizeof(unsigned int));
            this->write_n_bytes(data, size_in_bytes);
        }


        unsigned int receive_message(std::stringstream &ss) {
            unsigned int message_size;

            this->read_n_bytes(&message_size, sizeof(unsigned int));
            this->read_n_bytes(ss, message_size);
            return message_size;
        }

        void send_message(const std::string &str) {
            unsigned long message_size = str.size();
            if (message_size > static_cast<unsigned int>(-1)) {
                throw SocketMessageSizeException();
            }
            this->send_message(str.c_str(), static_cast<unsigned int>(message_size));
        }

        void close() {
            this->_sock.close();
        }

        void shutdown() {
            this->_sock.shutdown(tcp::socket::shutdown_both);
        }

    protected:
        Socket(boost::asio::io_service *io_service, const char *ip, unsigned short port) : _sock(*io_service) {
            tcp::endpoint endpoint = Socket::get_endpoint(ip, port);
            tcp::resolver resolver(*io_service);
            boost::asio::connect(this->_sock, resolver.resolve(endpoint));
        }

        Socket(boost::asio::io_service *io_service, tcp::acceptor &acceptor) : _sock(*io_service) {
            acceptor.accept(this->_sock);
        }

        static
        tcp::endpoint get_endpoint(const char *ip, unsigned short port) {
            return tcp::endpoint(boost::asio::ip::address::from_string(ip), port);
        }

        friend class SocketServer;

    private:
        static inline
        void
        custom_throw_error(boost::system::system_error const& e) {
            namespace error_ns = boost::asio::error;
            const boost::system::error_code &error = e.code();

            if (error == error_ns::eof || error == error_ns::connection_reset || error == error_ns::broken_pipe ||
                error == error_ns::not_connected)
                throw SocketConnectionClosedByPeerException(); // Connection closed cleanly by peer.
            throw SocketException(e.what()); // Some other error.
        }

        void
        read_n_bytes(
                void *buffer,
                std::size_t size_in_bytes
        ) {
            try {
                boost::asio::read(this->_sock, boost::asio::buffer(buffer, size_in_bytes));
            } catch (boost::system::system_error const& e) {
                custom_throw_error(e);
            } catch ( ... ) {
                throw SocketException("Unhandled exception in boost::asio::read");
            }
        }

        void
        read_n_bytes(
                std::stringstream &ss,
                std::size_t size_in_bytes
        ) {
            try {
                boost::asio::streambuf streambuf;
                boost::asio::read(this->_sock, streambuf, boost::asio::transfer_exactly(size_in_bytes));

                ss << &streambuf;
            } catch (boost::system::system_error const& e) {
                custom_throw_error(e);
            } catch ( ... ) {
                throw SocketException("Unhandled exception in boost::asio::read");
            }
        }

        void
        write_n_bytes(
                const void *data,
                std::size_t size_in_bytes
        ) {
            try {
                boost::asio::write(this->_sock, boost::asio::buffer(data, size_in_bytes));
            } catch (boost::system::system_error const& e) {
                custom_throw_error(e);
            } catch ( ... ) {
                throw SocketException("Unhandled exception in boost::asio::write");
            }
        }

    private:
        tcp::socket _sock;
    };


    class SocketServer {
    public:
        SocketServer(boost::asio::io_service *io_service, const char *ip, unsigned short port)
                : _io_service(io_service),
                  _acceptor(tcp::acceptor(*this->_io_service, Socket::get_endpoint(ip, port))) {}

        Socket *acceptConnection() {
            return new Socket(this->_io_service, this->_acceptor);
        }

        void close() {
            this->_acceptor.close();
        }

    private:
        boost::asio::io_service *_io_service;
        tcp::acceptor _acceptor;
    };


    class SocketClient : public Socket {
    public:
        SocketClient(boost::asio::io_service *io_service, const char *ip, unsigned short port)
                : Socket(io_service, ip, port) {}
    };
}

#endif //INDEX_PARTITIONING_SOCKET_HPP
