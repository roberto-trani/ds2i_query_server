#ifndef INDEX_PARTITIONING_QUERY_PARSER_EXCEPTION_HPP
#define INDEX_PARTITIONING_QUERY_PARSER_EXCEPTION_HPP

#include <stdexcept>


/**
 * Exception raised during the parsing. It contains information about the the error
 */
class QueryParserException : public std::runtime_error {
public:
    /**
     * Constructs a %QueryParserException which should be used only by the parser
     * @param __arg The string with the information about the error
     */
    QueryParserException ( const std::string &__arg ) : std::runtime_error ( __arg ) {}

    /**
     * %QueryParserException destructor
     */
    virtual ~QueryParserException () {}
};

#endif //INDEX_PARTITIONING_QUERY_PARSER_EXCEPTION_HPP
