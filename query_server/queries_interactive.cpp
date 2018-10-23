#include <iostream>

#include <succinct/mapper.hpp>

#include "../query/query_evaluation.hpp"

using namespace query;

bool read_query_interactive(term_id_vec& ret, std::istringstream & iline)
{
    ret.clear();
    term_id_type term_id;
    while (iline >> term_id) {
        ret.push_back(term_id);
    }
    if (ret.size() == 0) {
        std::cerr << "The query is empty" << std::endl;
        return false;
    }

    return true;
}

bool read_cnf_query_interactive(std::vector<term_id_vec>& ret, std::istringstream & iline)
{
    ret.clear();

    // read the part containing the query shape
    unsigned int count;
    if (!(iline >> count)) {
        std::cerr << "Unable to read the main counter" << std::endl;
        return false;
    }
    ret.resize(count);
    for (std::size_t i = 0, i_end = ret.size(); i < i_end; ++i) {
        if (!(iline >> count)) {
            std::cerr << "Unable to read one the " << i + 1 << "-th counter" << std::endl;
            return false;
        }
        if (count == 0) {
            std::cerr << "The " << i + 1 << "-th counter is zero" << std::endl;
            return false;
        }
        ret[i].resize(count);
    }
    // read the part containing the query terms
    term_id_type term_id;
    for (std::size_t i = 0, i_end = ret.size(), k = 0; i < i_end; ++i) {
        for (std::size_t j = 0, j_end = ret[i].size(); j < j_end; ++j, ++k) {
            if (!(iline >> term_id)) {
                std::cerr << "Unable to read the " << k << "-th term" << std::endl;
                return false;
            }
            ret[i][j] = term_id;
        }
    }

    return true;
}




template <typename QueryOperator, typename IndexType, typename QueryType>
void
op_perf_evaluation(
        IndexType const& index,
        QueryOperator&& query_op, // XXX!!!
        QueryType const& query,
        uint64_t * num_results,
        double * elapsed_time
) {
    using namespace ds2i;
    query_op(index, query);

    auto tick = get_time_usecs();
    auto results = query_op(index, query);
    auto elapsed = double(get_time_usecs() - tick);

    *num_results = results;
    *elapsed_time = elapsed;
}

template <typename IndexType>
void
interactive_op(
        const char* index_filename,
        std::string const& type
) {
    using namespace ds2i;

    IndexType index;
    logger() << "Index type: " << type << std::endl;
    logger() << "Loading index from " << index_filename << std::endl;
    boost::iostreams::mapped_file_source m(index_filename);
    succinct::mapper::map(index, m, succinct::mapper::map_flags::warmup);

    // reading one query at a time
    logger() << "Ready to answer queries" << std::endl;
    ds2i::term_id_vec query;
    std::vector<ds2i::term_id_vec> cnf_query;
    while (true) {
        unsigned int op = 0;

        // read the query
        {
            std::string line;
            if (!std::getline(std::cin, line)) {
                break;
            }

            std::istringstream iline(line);
            // read the first part containing the operation
            std::string op_str;
            if (!(iline >> op_str)) {
                // empty line
                //std::cerr << "Unable to read the operation type" << std::endl;
                continue;
            }

            if (op_str == "cnf") {
                op = 0;
                read_cnf_query_interactive(cnf_query, iline);
            } else if (op_str == "and") {
                op = 1;
                read_query_interactive(query, iline);
            } else if (op_str == "or") {
                op = 2;
                read_query_interactive(query, iline);
            } else {
                std::cerr << "Unrecognized operation type " << op_str << std::endl;
                continue;
            }
        }

        // perform the query
        {
            uint64_t result;
            double elapsed;

            switch(op) {
                case (0):
                    op_perf_evaluation(index, and_or_query<true>(), cnf_query, &result, &elapsed);
                    break;
                case (1):
                    op_perf_evaluation(index, and_query<true>(), query, &result, &elapsed);
                    break;
                case (2):
                    op_perf_evaluation(index, or_query<true>(), query, &result, &elapsed);
                    break;
            }

            logger() << elapsed << " us" << "\t" << result<< " docs" << std::endl;
        }
    }
}

int main(int argc, const char** argv)
{
    using namespace ds2i;
    if (argc != 3) {
        std::cerr << "queries_interactive index_type index_filename";
    }

    std::string type = argv[1];
    const char * index_filename = argv[2];

    if (false) {
#define LOOP_BODY(R, DATA, T)                                   \
        } else if (type == BOOST_PP_STRINGIZE(T)) {             \
            interactive_op<BOOST_PP_CAT(T, _index)>                   \
                (index_filename, type); \
            /**/

    BOOST_PP_SEQ_FOR_EACH(LOOP_BODY, _, DS2I_INDEX_TYPES);
#undef LOOP_BODY
    } else {
        logger() << "ERROR: Unknown type " << type << std::endl;
    }

}
