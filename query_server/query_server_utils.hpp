#ifndef INDEX_PARTITIONING_QUERY_SERVER_UTILS_HPP
#define INDEX_PARTITIONING_QUERY_SERVER_UTILS_HPP

#include <boost/iostreams/device/mapped_file.hpp>
#include <boost/iostreams/filtering_streambuf.hpp>
#include <boost/iostreams/filter/gzip.hpp>
#include <fstream>
#include <iostream>
#include <unordered_map>
#include <unordered_set>
#include <vector>
#include "ds2i/queries.hpp"
#include "query/query_expr_and.hpp"
#include "query/query_expr_or.hpp"
#include "query/query_expr_term.hpp"

//#include "../queries.hpp"


namespace query_server {
    using term_id_type = ds2i::term_id_type;
    using term_id_vec = ds2i::term_id_vec;

    std::unordered_map<std::size_t, uint64_t>
    get_docid_to_new_docid_map(
            std::string map_file_path
    ) {
        std::unordered_map<std::size_t, uint64_t> result;

        // open the file
        boost::iostreams::mapped_file_source map_file(map_file_path);
        if ( !map_file.is_open() ) {
            throw std::runtime_error("Error opening file");
        }

        // check the file size
        std::size_t map_file_size = map_file.size();
        if (map_file_size % 12) {
            map_file.close();
            throw std::runtime_error("Incompatible size of the file");
        }
        const char * map = map_file.data();
        const std::size_t map_size = map_file_size;

        for (std::size_t i=0; i < map_size; i += 12) {
            result[*(std::size_t *)(map+i)] = *(unsigned int *)(map+i+8);
        }

        std::cerr << " get_docid_to_new_docid_map: stored " << result.size() << " docid." << std::endl;

        return result;
    };

    std::unordered_map<std::string, term_id_type>
    get_segment_to_termid_map(
            std::string file_path
    ) {
        std::unordered_map<std::string, term_id_type> result;
        term_id_type count = 0;

        // open the file
        std::ifstream file(file_path, std::ios_base::in | std::ios_base::binary);
        boost::iostreams::filtering_streambuf<boost::iostreams::input> inbuf;
        inbuf.push(boost::iostreams::gzip_decompressor());
        inbuf.push(file);
        std::istream instream(&inbuf); // convert streambuf to istream

        for (std::string segment; getline(instream, segment);) {
            if (segment.size() == 0) {
                continue;
            }

            // put the pair segment - termid into the result map
            auto result_it = result.find(segment);
            if (result_it != result.end()) {
                std::cerr << "The segment \"" << segment << "\" appeared two times into the map file";
            } else {
                result[segment] = count++;
            }
        }

        std::cerr << " get_segment_to_termid_map: stored " << count << " segments" << std::endl;

        return result;
    };

    std::unordered_set<std::string>
    get_segment_set(
            std::string file_path
    ) {
        std::unordered_set<std::string> result;
        std::size_t count = 0;

        // open the file
        std::ifstream file(file_path, std::ios_base::in | std::ios_base::binary);
        boost::iostreams::filtering_streambuf<boost::iostreams::input> inbuf;
        inbuf.push(boost::iostreams::gzip_decompressor());
        inbuf.push(file);
        std::istream instream(&inbuf); // convert streambuf to istream

        for (std::string segment; getline(instream, segment);) {
            if (segment.size() == 0) {
                continue;
            }

            // put the pair segment - termid into the result map
            auto result_it = result.find(segment);
            if (result_it != result.end()) {
                std::cerr << "The segment \"" << segment << "\" appeared two times into the map file";
            } else {
                result.insert(segment);
                ++count;
            }
        }

        std::cerr << " get_segment_set: stored " << count << " segments" << std::endl;

        return result;
    };

    std::vector<term_id_vec>
    translate_cnf_expression(
            const query::QueryExprAND<query::QueryExprOR<query::QueryExprTerm>> & cnf_expr,
            const std::unordered_map<std::string, term_id_type> & segment_to_termid_map
    ) {
        std::vector<term_id_vec> result(cnf_expr.getSubExpressionsNumber());

        // translate each segment into a term id
        for (std::size_t i=0, i_max = cnf_expr.getSubExpressionsNumber(); i < i_max; ++i) {
            result[i].reserve(cnf_expr[i].getSubExpressionsNumber());

            for (std::size_t j=0, j_max=cnf_expr[i].getSubExpressionsNumber(); j < j_max; ++j) {
                auto find_it = segment_to_termid_map.find(cnf_expr[i][j].lexeme);
                if (find_it != segment_to_termid_map.end()) {
                    result[i].push_back(find_it->second);
                }
            }
        }

        // remove empty OR clauses
        result.erase(
                std::remove_if(
                        result.begin(),
                        result.end(),
                        [](const term_id_vec & v) { return v.size() == 0; }
                ),
                result.end()
        );

        return result;
    }

    template <typename FLATQUERYEXPR>
    term_id_vec
    translate_flat_expression(
            const FLATQUERYEXPR & flat_expr,
            const std::unordered_map<std::string, term_id_type> & segment_to_termid_map
    ) {
        term_id_vec result;
        result.reserve(flat_expr.getSubExpressionsNumber());

        // translate each segment into a term id
        for (std::size_t i=0, i_max = flat_expr.getSubExpressionsNumber(); i < i_max; ++i) {
            auto find_it = segment_to_termid_map.find(flat_expr[i].lexeme);
            if (find_it != segment_to_termid_map.end()) {
                result.push_back(find_it->second);
            }
        }

        return result;
    }
}

#endif //INDEX_PARTITIONING_QUERY_SERVER_UTILS_HPP
