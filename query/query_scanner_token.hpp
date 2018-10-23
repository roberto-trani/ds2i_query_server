#ifndef INDEX_PARTITIONING_QUERY_SCANNER_TOKEN_HPP
#define INDEX_PARTITIONING_QUERY_SCANNER_TOKEN_HPP

namespace query {

    /**
    * Token types returned by a generic %QueryScanner
    */
    enum QueryScannerToken {
        TOK_TERM,
        TOK_L_BRACKET,
        TOK_R_BRACKET,
        TOK_SPACE,
        TOK_OR,
        TOK_DOUBLE_QUOTE,
        TOK_UNDEFINED,
        TOK_END
    };

    /**
    * Gets a string with the name of the given token
    * @param token The token for which we want to know the name
    * @return A string with the token name
    */
    const char *
    QueryScannerTokenToString(QueryScannerToken token) {
        switch (token) {
            case TOK_TERM:
                return "TERM";
            case TOK_L_BRACKET:
                return "LEFT_BRACKET";
            case TOK_R_BRACKET:
                return "RIGHT_BRACKET";
            case TOK_SPACE:
                return "SPACE";
            case TOK_OR:
                return "OR";
            case TOK_DOUBLE_QUOTE:
                return "DOUBLE_QUOTE";
            case TOK_UNDEFINED:
                return "UNDEFINED";
            case TOK_END:
                return "END";
            default:
                return "unidentified";
        }
    }

}
#endif //INDEX_PARTITIONING_QUERY_SCANNER_TOKEN_HPP
