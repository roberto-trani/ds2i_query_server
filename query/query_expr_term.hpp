#ifndef INDEX_PARTITIONING_QUERY_EXPR_TERM_HPP
#define INDEX_PARTITIONING_QUERY_EXPR_TERM_HPP

#include <ostream>
#include <string>


namespace query {
    /**
     * Query Term
     */
    class QueryExprTerm {
    public:
        /**
         * The term lexeme
         */
        std::string lexeme;

        /**
         * The position of the term inside the query
         */
        unsigned short queryPos;

    public:
        /**
         * Create a new QETerm_c without initializing it
         */
        QueryExprTerm() {}

        /**
         * Create a new QETerm_c copying the given one
         * @param other The term to copy
         */
        QueryExprTerm(const QueryExprTerm &other) {
            this->operator=(other);
        }

        /**
         * Create a new QETerm_c moving the given one inside it
         * @param other The term to move inside the built one
         */
        QueryExprTerm(QueryExprTerm &&other) {
            this->operator=(std::move(other));
        }

        /**
         * Create a new QETerm_c with the specified weight and representing the given query position
         * @param lexeme The term lexeme
         * @param fWeight The term weight
         * @param queryPos The position of the term inside the query
         */
        QueryExprTerm(const std::string &lexeme, unsigned short queryPos)
                : lexeme(lexeme),
                  queryPos(queryPos) {}

        /**
         * Create a new QETerm_c with the specified weight and representing the given query position
         * @param lexeme The term lexeme
         * @param fWeight The term weight
         * @param queryPos The position of the term inside the query
         */
        QueryExprTerm(std::string &&lexeme, unsigned short queryPos)
                : lexeme(lexeme),
                  queryPos(queryPos) {}

        /**
         * Assign the same content of the given term to this one
         * @param other The term to copy
         * @return The current term
         */
        QueryExprTerm &
        operator=(const QueryExprTerm &other) {
            this->lexeme = other.lexeme;
            this->queryPos = other.queryPos;
            return *this;
        }

        /**
         * Assign the content of the given term to this one
         * @param other The term to move inside this one
         * @return The current term
         */
        QueryExprTerm &
        operator=(QueryExprTerm &&other) {
            return *this = other;
        }
    };
}

/**
 * Write on the output stream os the term ref
 * @param os The std::ostream to use
 * @param ref The QueryExprTerm to write
 * @return The input std::ostream
 */
std::ostream &
operator<<(std::ostream &os, const query::QueryExprTerm &ref) {
    if (ref.lexeme.find(' ') != std::string::npos) {
        return os << '"' << ref.lexeme << '"';
    } else {
        return os << ref.lexeme;
    }
}


#endif //INDEX_PARTITIONING_QUERY_EXPR_TERM_HPP
