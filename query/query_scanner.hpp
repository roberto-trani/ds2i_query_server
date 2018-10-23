#ifndef INDEX_PARTITIONING_QUERY_SCANNER_HPP
#define INDEX_PARTITIONING_QUERY_SCANNER_HPP

#include <string>

#include "query_scanner_token.hpp"


namespace query {

    /**
     * QueryExpansion Argument Scanner
     */
    class QueryScanner {
    private:
        /**
         * Pointer to the string to scan
         */
        const char *pString;

        /**
         * Current position inside the string
         */
        long iCurPos;

        /**
         * Last position inside the string
         */
        long iLastPos;

        /**
         * Current %QueryScannerToken
         */
        QueryScannerToken eCurToken;

    public:
        /**
         * A boolean indicating if this scanner must ignore the spaces or not
         */
        bool bIgnoreSpaces;

        /**
         * The value associated to the token, if it has one
         */
        struct {
            const char *str_ptr;
            unsigned long str_size;
        } uLexVal;

    public:
        /**
         * Constructs an unitilized Scanner
         */
        QueryScanner()
                : QueryScanner("") {}

        /**
         * Constructs a Scanner on top of the given string
         * @param pString The string to scan
         */
        QueryScanner(const std::string &sString)
                : QueryScanner(sString.data()) {}

        /**
         * Constructs a Scanner on top of the given string
         * @param pString The string to scan
         */
        QueryScanner(const char *pString)
                : pString(pString),
                  iCurPos(-1),
                  iLastPos(-1),
                  eCurToken(TOK_UNDEFINED),
                  bIgnoreSpaces(true) {}

        /**
         * Advances the internal pointer to the next token and returns it
         * @return The next Token
         */
        QueryScannerToken
        getNextToken() {
            // if I have already encountered the end of the string I don't go on
            if (this->eCurToken == TOK_END)
                return TOK_END;
            // save the last starting point to implement the cancelLastGetNextToken operation
            this->iLastPos = this->iCurPos;

            // local variables
            long startPos = ++this->iCurPos;
            char currentChar = this->pString[startPos];

            // skip all the spaces
            if (bIgnoreSpaces) {
                while (currentChar != '\0' && currentChar == ' ') {
                    currentChar = this->pString[++this->iCurPos];
                }
            }

            // decide the next token on the base of the current character
            switch (currentChar) {
                case '\0':
                    return this->eCurToken = TOK_END;
                case '|':
                    return this->eCurToken = TOK_OR;
                case '(':
                    return this->eCurToken = TOK_L_BRACKET;
                case ')':
                    return this->eCurToken = TOK_R_BRACKET;
                case ' ':
                    return this->eCurToken = TOK_SPACE;
                case '"':
                    return this->eCurToken = TOK_DOUBLE_QUOTE;
                default:
                    break;
            }
            // TODO: it should be modified in such a way to return directly the string within double quotes when they are encountered.
            // TODO: after the previous todo the quote escaping should be implemented too.

            startPos = this->iCurPos;
            // try to recognize if this is a term: [0-9a-zA-Z]+
            while (
                    (currentChar >= 'a' && currentChar <= 'z')
                    || (currentChar >= '0' && currentChar <= '9')
                    || (currentChar >= 'A' && currentChar <= 'Z')
                    || (currentChar == '_')  // added as a special character to identify particular segments
                //|| (!bIgnoreSpaces && currentChar == ' ')
                    ) {
                currentChar = this->pString[++this->iCurPos];
            }
            if (this->iCurPos > startPos) {
                this->uLexVal.str_ptr = this->pString + startPos;
                this->uLexVal.str_size = static_cast<unsigned long>(this->iCurPos - startPos);
                --this->iCurPos;
                return this->eCurToken = TOK_TERM;
            }

            return this->eCurToken = TOK_UNDEFINED;
        }

        /**
         * Cancels the last getNextToken operation: takes back the internal pointer to the previous token and returns it.
         * The scanner can cancel only the last @c getNextToken call, if you try to cancel two times the same call an exception will be raised
         * @return The next Token
         * @throw runtime_error If the operation is called without a @c getNextToken before, or if you try to cancel two times the same call
         */
        QueryScannerToken
        cancelLastGetNextToken() {
            if (this->iLastPos == -1) {
                throw std::runtime_error("No getNextOperation has been called before");
            } else if (this->iLastPos == this->iCurPos) {
                throw std::runtime_error(
                        "It is not allowed to cancel more than one time the last getNextOperation call");
            }
            this->iCurPos = this->iLastPos;
            return this->getNextToken();
        }

        /**
         * @return The current Token
         */
        QueryScannerToken
        getCurrentToken() {
            return this->eCurToken;
        }

        /**
         * @return The current character. If it is a token composed of multiple characters, only the last one is returned
         */
        char
        getCurrentChar() {
            return this->pString[this->iCurPos];
        }

        /**
         * @return The current position inside the string
         */
        long
        getCurrentPosition() {
            return this->iCurPos;
        }
    };
}

#endif //INDEX_PARTITIONING_QUERY_SCANNER_HPP
