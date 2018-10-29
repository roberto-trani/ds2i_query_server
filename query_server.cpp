#include <iostream>
#include <boost/iostreams/device/mapped_file.hpp>

#include <boost/property_tree/ptree.hpp>
#include <boost/property_tree/json_parser.hpp>
#include <boost/thread/thread.hpp>
#include <unordered_map>
#include <unordered_set>

#include "../ds2i/succinct/mapper.hpp"
#include "../ds2i/index_types.hpp"
#include "../ds2i/wand_data.hpp"
#include "../ds2i/bm25.hpp"
//#include "../queries.hpp"

#include "query_server/socket.hpp"
#include "query_server/query_server_utils.hpp"
#include "query/query_static_parser.hpp"
#include "query/query_evaluation.hpp"

//#include "../queries.hpp"


namespace pt = boost::property_tree;


template <typename QueryOperator, typename IndexType, typename ScorerType, typename QueryType>
void
op_perf_evaluation(
        IndexType const& index,
        ds2i::wand_data<ScorerType> * wdata,
        QueryOperator&& query_op, // XXX!!!
        QueryType & query,
        std::vector<uint64_t> &rel,
        uint64_t * num_ret,
        uint64_t * num_rel_ret,
        unsigned int ranked_at,
        double * exe_time
) {
    uint64_t results;
    if (ranked_at > 0 && wdata == nullptr) {
        throw std::runtime_error("wdata must be specified when ranked_at is required");
    }

    // warm up
    if (ranked_at > 0) {
        results = query_op(index, *wdata, query, rel, num_rel_ret, ranked_at);
    } else {
        results = query_op(index, query, rel, num_rel_ret);
    }

    // second run without rel
    auto tick = ds2i::get_time_usecs();
    if (ranked_at > 0) {
        query_op(index, *wdata, query, ranked_at);
    } else {
        query_op(index, query);
    }
    double elapsed = double(ds2i::get_time_usecs() - tick);

    *num_ret = results;
    *exe_time = elapsed/1000.0;
}


template <typename IndexType, typename ScorerType>
void handle_request(
        const pt::ptree &request,
        pt::ptree &reply,
        const std::unordered_map<std::string, unsigned int> * segment_to_termid_map,
        const std::unordered_map<std::size_t, uint64_t> * docid_to_new_docid,
        IndexType * index,
        ds2i::wand_data<ScorerType> * wdata // optional
) {
    uint64_t num_ret;
    uint64_t num_rel_ret;
    double exe_time;

    // identify the query inside the json
    boost::optional<std::string> query_opt = request.get_optional<std::string>("query");
    if (!query_opt) {
        throw std::runtime_error("Missing query field");
    }

    std::vector<uint64_t> rel;
    auto rel_opt = request.get_child_optional("rel");
    if (rel_opt) {
        for (const pt::ptree::value_type &docid_obj : rel_opt.get()) {
            std::size_t docid = docid_obj.second.get_value<std::size_t>();
            auto find_it = docid_to_new_docid->find(docid);
            if (find_it == docid_to_new_docid->end()) {
                throw std::runtime_error("Unable to find one of the docids");
            }
            rel.push_back(find_it->second);
        }
        if (rel.size() == 0) {
            throw std::runtime_error("Empty rel option");
        }
    }

    // query normalization
    bool query_normalization = true;
    boost::optional<std::string> query_normalization_opt = request.get_optional<std::string>("query_normalization");
    if (query_normalization_opt) {
        if (query_normalization_opt.get() == "false") {
            query_normalization = false;
        } else if (query_normalization_opt.get() != "true") {
            throw std::runtime_error("Unrecognized query_normalization");
        }
    }

    // ranked
    unsigned int ranked_at = 0;
    boost::optional<unsigned int> ranked_at_opt = request.get_optional<unsigned int>("ranked_at");
    if (ranked_at_opt) {
        ranked_at = ranked_at_opt.get();
        if (ranked_at == 0 || ranked_at > 1000*1000) {
            throw std::runtime_error("Ranked at must be greater than 0 and lower than 1M");
        }
    }

    // query type
    boost::optional<std::string> query_type_opt = request.get_optional<std::string>("query_type");
    if (query_type_opt && query_type_opt.get() == "and") {
        // parse it and transforms the terms into termids
        auto query_expression = query::QueryStaticParser::parse<query::QueryExprAND<query::QueryExprTerm>>(query_opt.get());
        auto query_vector = query_server::translate_flat_expression(query_expression, *segment_to_termid_map);

        // perform the query
        if (query_normalization) {
            op_perf_evaluation(*index, wdata, query::and_query<true, true>(), query_vector, rel, &num_ret, &num_rel_ret, ranked_at, &exe_time);
        } else {
            op_perf_evaluation(*index, wdata, query::and_query<false, true>(), query_vector, rel, &num_ret, &num_rel_ret, ranked_at, &exe_time);
        }
    } else if (query_type_opt && query_type_opt.get() == "or") {
        // parse it and transforms the terms into termids
        auto query_expression = query::QueryStaticParser::parse<query::QueryExprOR<query::QueryExprTerm>>(query_opt.get());
        auto query_vector = query_server::translate_flat_expression(query_expression, *segment_to_termid_map);

        // perform the query
        if (query_normalization) {
            op_perf_evaluation(*index, wdata, query::or_query<true, true>(), query_vector, rel, &num_ret, &num_rel_ret, ranked_at, &exe_time);
        } else {
            op_perf_evaluation(*index, wdata, query::or_query<false, true>(), query_vector, rel, &num_ret, &num_rel_ret, ranked_at, &exe_time);
        }
    } else if (!query_type_opt || query_type_opt.get() == "cnf") {
        // parse it and transforms the terms into termids
        auto query_expression = query::QueryStaticParser::parse<query::QueryExprAND<query::QueryExprOR<query::QueryExprTerm>>>(query_opt.get());
        auto query_vector = query_server::translate_cnf_expression(query_expression, *segment_to_termid_map);

        // perform the query
        if (query_normalization) {
            op_perf_evaluation(*index, wdata, query::and_or_query<true, true>(), query_vector, rel, &num_ret, &num_rel_ret, ranked_at, &exe_time);
        } else {
            op_perf_evaluation(*index, wdata, query::and_or_query<false, true>(), query_vector, rel, &num_ret, &num_rel_ret, ranked_at, &exe_time);
        }
    } else if (!query_type_opt || query_type_opt.get() == "cnf opt") {
        // parse it and transforms the terms into termids
        auto query_expression = query::QueryStaticParser::parse<query::QueryExprAND<query::QueryExprOR<query::QueryExprTerm>>>(query_opt.get());
        auto query_vector = query_server::translate_cnf_expression(query_expression, *segment_to_termid_map);

        // perform the query
        if (query_normalization) {
            op_perf_evaluation(*index, wdata, query::opt_and_or_query<true, true>(), query_vector, rel, &num_ret, &num_rel_ret, ranked_at, &exe_time);
        } else {
            op_perf_evaluation(*index, wdata, query::opt_and_or_query<false, true>(), query_vector, rel, &num_ret, &num_rel_ret, ranked_at, &exe_time);
        }
    } else if (query_type_opt && query_type_opt.get() == "maxscore") {
        // parse it and transforms the terms into termids
        auto query_expression = query::QueryStaticParser::parse<query::QueryExprOR<query::QueryExprTerm>>(query_opt.get());
        auto query_vector = query_server::translate_flat_expression(query_expression, *segment_to_termid_map);

        // perform the query
        if (!query_normalization) {
            throw std::runtime_error("normalization cannot be disabled for maxscore");
        }
        op_perf_evaluation(*index, wdata, query::maxscore_query(), query_vector, rel, &num_ret, &num_rel_ret, ranked_at, &exe_time);
    } else {
        throw std::runtime_error("Unrecognized query_type");
    }

    // compose the json
    reply.put<std::size_t>("num_ret", num_ret);
    reply.put<double>("exe_time", exe_time);
    if (rel_opt) {
        reply.put<std::size_t>("num_rel_ret", num_rel_ret);
        reply.put<std::size_t>("num_rel", rel.size());
    }
}


template <typename IndexType, typename ScorerType>
void session(
        query_server::Socket * sock,
        const std::unordered_map<std::string, unsigned int> * segment_to_termid_map,
        const std::unordered_map<std::size_t, uint64_t> * docid_to_new_docid,
        IndexType * index,
        ds2i::wand_data<ScorerType> * wdata // optional
) {
    bool close_socket = true;

    //std::cout << "CLIENT Connection" << std::endl;
    try {
        // string stream used as buffer
        std::stringstream ss;
        // create a json root for the request and for the reply
        pt::ptree request;
        pt::ptree reply;

        for (;;) {
            ss.str("");
            sock->receive_message(ss);
            //std::cout << "RECEIVED: " << ss.str() << std::endl;

            // handle the request
            try {
                // load the json file in this ptree
                request.clear();
                pt::read_json(ss, request);

                // handle the request
                reply.clear();
                handle_request<IndexType, ScorerType>(request, reply, segment_to_termid_map, docid_to_new_docid, index, wdata);
            } catch (std::exception &e) {
                reply.clear();
                reply.put<std::string>("error", e.what());
            }

            // reply the request
            {
                // transform the json tree into a string
                ss.str("");
                pt::write_json(ss, reply, false);
                //std::cout << "ANSWER: " << ss.str() << std::endl;

                // send the string
                sock->send_message(ss.str());
            }
        }
    } catch (const query_server::SocketConnectionClosedByPeerException &e) {
        //std::cout << "CLIENT Disconnection" << std::endl;
        close_socket = false;
    } catch (const std::exception &e) {
        std::cerr << "Exception in thread: " << e.what() << std::endl;
    } catch (...) {
        std::cerr << "Exception in thread: unrecognized" << std::endl;
    }
    //std::cout << "EXIT" << std::endl;

    if (close_socket) {
        sock->shutdown();
        sock->close();
    }

    delete (sock);
}


template <typename IndexType, typename ScorerType>
void server(
        const char *ip,
        unsigned short port,
        const std::string & index_type,
        const std::string & index_basename
) {
    // create the socket server to check the ip and port
    boost::asio::io_service io_service;
    query_server::SocketServer server(&io_service, ip, port);

    // loading the term map
    std::cerr << "Loading the term map from " << index_basename << ".terms and from " << index_basename << ".mterms" << std::endl;
    std::unordered_map<std::string, unsigned int> segment_to_termid = query_server::get_segment_to_termid_map(
            index_basename + ".terms"
    );
    std::unordered_set<std::string> missing_segments = query_server::get_segment_set(
            index_basename + ".mterms"
    );

    // loading the doc map
    std::cerr << "Loading the doc map from " << index_basename << ".docids.map" << std::endl;
    std::unordered_map<std::size_t, uint64_t> docid_to_new_docid = query_server::get_docid_to_new_docid_map(
            index_basename + ".docids.map"
    );

    // loading the index
    std::cerr << "Loading the index (type " << index_type << ") from " << index_basename << "." << index_type << std::endl;
    IndexType index;
    boost::iostreams::mapped_file_source index_file_source(index_basename + "." + index_type);
    succinct::mapper::map(index, index_file_source, succinct::mapper::map_flags::warmup);

    ds2i::wand_data<ScorerType> wdata;
    ds2i::wand_data<ScorerType> * wdata_ptr = nullptr;
    boost::iostreams::mapped_file_source md;
    std::string wand_data_filename = index_basename + ".wand";
    if ( access( wand_data_filename.c_str(), F_OK ) != -1 ) { // it can also not exist
        std::cerr << "Loading wand data from " << index_basename << ".wand" << std::endl;
        md.open(wand_data_filename);
        succinct::mapper::map(wdata, md, succinct::mapper::map_flags::warmup);
        wdata_ptr = &wdata;
    }

    // accepting connections
    std::cerr << "Accepting connections" << std::endl;
    while (true) {
        auto sock = server.acceptConnection();
        boost::thread t(boost::bind(session<IndexType, ScorerType>, sock, &segment_to_termid, &docid_to_new_docid, &index, wdata_ptr));
    }

    server.close();
}


int main(
        int argc,
        char *argv[]
) {
    using namespace ds2i;

    try {
        if (argc <= 4) {
            std::cerr << "Usage: " << argv[0] << " ip port index_type index_basename\n";
            return -1;
        }

        const char *ip = argv[1];
        unsigned short port = static_cast<unsigned short>(std::atoi(argv[2]));
        std::string index_type = argv[3];
        std::string index_basename = argv[4];

        if (false) {
#define LOOP_BODY(R, DATA, T)                                   \
        } else if (index_type == BOOST_PP_STRINGIZE(T)) {             \
            server<BOOST_PP_CAT(T, _index), ds2i::bm25>(ip, port, index_type, index_basename);
            /**/

        BOOST_PP_SEQ_FOR_EACH(LOOP_BODY, _, DS2I_INDEX_TYPES);
#undef LOOP_BODY
        } else {
            std::cerr << "ERROR: Unknown type " << index_type << std::endl;
        }
        //server(ip, port, index_type, index_basename);

    } catch (std::exception &e) {
        std::cerr << "Exception: " << e.what() << "\n";
    }

    return 0;
}
