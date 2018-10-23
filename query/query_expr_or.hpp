#ifndef INDEX_PARTITIONING_QUERY_EXPR_OR_HPP
#define INDEX_PARTITIONING_QUERY_EXPR_OR_HPP

#include <vector>
#include <ostream>


namespace query {
    /**
     * Query OR Expression
     */
    template<typename T>
    class QueryExprOR {
    private:
        /**
         * The OR sub-expressions
         */
        std::vector<T> expressions;

    public:
        /**
         * Assigns to this expression the result of the OR between this and the given value
         * @param value The value to use as right OR operand
         * @return *this
         */
        QueryExprOR<T> &
        operator|=(const T &value) {
            this->expressions.push_back(value);
            return *this;
        }

        /**
         * Assigns to this expression the result of the OR between this and the given value
         * @param value The value to use as right OR operand
         * @return *this
         */
        QueryExprOR<T> &
        operator|=(T &&value) {
            this->expressions.push_back(std::move(value));
            return *this;
        }

        /**
         * Returns a reference to the value of the term at position idx
         * @param idx Position of the term in the expression
         * @return The value of the term at the given position
         */
        T &
        operator[](std::size_t idx) {
            return this->expressions[idx];
        }

        /**
         * Returns a reference to the value of the term at position idx
         * @param idx Position of the term in the expression
         * @return The value of the term at the given position
         */
        const T &
        operator[](std::size_t idx) const {
            return this->expressions[idx];
        }

        /**
         * Returns a direct pointer to the memory array used internally to store the sub expressions
         * @return A pointer to the first element in the array used internally
         */
        const T *
        getSubExpressions() const {
            return this->expressions.data();
        }

        /**
         * @return The number of elements inside this %QEORExpr_c expression
         */
        std::size_t
        getSubExpressionsNumber() const {
            return this->expressions.size();
        }

        /**
         * @return The number of terms in all the subexpressions
         */
        std::size_t
        getTermsNumber() const {
            std::size_t iTermsNumber = 0;
            for (std::size_t i = 0, max = this->expressions.size(); i < max; ++i) {
                iTermsNumber += this->expressions[i].getTermsNumber();
            }
            return iTermsNumber;
        }

        /**
         * Requests that the expression capacity be at least enough to contain @a size elements.
         * @param size The minimum capacity for the expression
         */
        void
        reserve(std::size_t size) {
            this->expressions.reserve(size);
        }

        /**
         * Removes all terms from the expression, leaving the expression empty
         */
        void
        clear() {
            this->expressions.clear();
        }
    };
}

/**
 * Write on the output stream os the OR expression ref
 * @param os The std::ostream to use
 * @param ref The QueryExprOR to write
 * @return The input std::ostream
 */
template<typename T>
std::ostream &
operator<<(std::ostream &os, const query::QueryExprOR <T> &ref) {
    const std::size_t terms_number = ref.getSubExpressionsNumber();
    const T *sub_expressions = ref.getSubExpressions();

    os << '(';
    for (std::size_t i = 0; i < terms_number; ++i) {
        if (i > 0) {
            //os << ") | (";
            os << " | ";
        }
        os << sub_expressions[i];
    }
    return os << ')';
}

#endif //INDEX_PARTITIONING_QUERY_EXPR_OR_HPP
