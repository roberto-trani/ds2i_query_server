#ifndef INDEX_PARTITIONING_QUERY_EVALUATION_HPP
#define INDEX_PARTITIONING_QUERY_EVALUATION_HPP

#include "../ds2i/index_types.hpp"
#include <iostream>
#include <unordered_set>


namespace query {
    using namespace ds2i;

    typedef uint32_t term_id_type;
    typedef std::vector<term_id_type> term_id_vec;

    struct docid_score {
        uint64_t docid;
        float score;

        docid_score():
                docid(static_cast<uint64_t>(-1)),
                score(std::numeric_limits<float>::min()) {
        }
    };

    class TopK_Queue {
    private:
        std::vector<docid_score> heap;
        unsigned int K;

    public:
        TopK_Queue(unsigned int K) {
            this->heap.resize(K, docid_score());
            this->K = K;
        }

        inline bool
        insert(uint64_t docid, float score) {
            if (score <= this->heap[0].score) {
                return false;
            }

            this->heap[0].docid = docid;
            this->heap[0].score = score;

            percolate_down(0);

            return true;
        }

        inline bool
        would_enter(float score) const {
            return score > this->heap[0].score;
        }

        void finalize() {
            uint64_t null_docid = static_cast<uint64_t>(-1);

            unsigned int last = this->K;
            unsigned int i = last;
            // remove all fake elements, putting them into the last positions
            while (i>0) {
                --i;
                if (this->heap[i].docid == null_docid) {
                    this->heap[i] = this->heap[--last];
                }
            }
            // resize the heap, removing the last elements
            this->heap.resize(last);
        }

        std::vector<docid_score> const& get_list() const {
            return this->heap;
        }

    private:
        inline unsigned int
        left(unsigned int i) {
            return (2*i + 1);
        }

        inline void
        swap(unsigned int i, unsigned int j) {
            docid_score tmp = this->heap[i];
            this->heap[i] = this->heap[j];
            this->heap[j] = tmp;
        }

        inline void
        percolate_down(unsigned int pos) {
            unsigned int smallest;
            unsigned int l, r;

            while (true) {
                // find the minimum among pos and its children
                l = this->left(pos);
                if (l >= this->K) {
                    break;
                }
                r = l + 1;
                if (this->heap[l].score < this->heap[pos].score) {
                    smallest = l;
                } else {
                    smallest = pos;
                }
                if (r < this->K && this->heap[r].score < this->heap[smallest].score) {
                    smallest = r;
                }

                // pos is already the smallest element
                if (smallest == pos) {
                    break;
                }
                // swap pos with its smallest children
                this->swap(pos, smallest);
                pos = smallest;
            }
        }
    };


    template <typename T>
    void
    remove_vector_duplicates_and_sort(
            std::vector<T> & vec
    ) {
        std::sort(vec.begin(), vec.end());
        vec.erase(std::unique(vec.begin(), vec.end()), vec.end());
    }


    template <bool normalize=true, bool with_freqs=true>
    struct and_or_query {
    public:
        template<typename Index, typename ScorerType=ds2i::bm25>
        uint64_t operator()(Index const &index, std::vector<term_id_vec> & and_or_terms) const {
            return this->get<Index, ScorerType, false, false>(index, and_or_terms);
        }

        template<typename Index, typename ScorerType=ds2i::bm25>
        uint64_t operator()(Index const &index, std::vector<term_id_vec> & and_or_terms, std::vector<uint64_t> & rel, uint64_t * num_rel_ret) const {
            return this->get<Index, ScorerType, true, false>(index, and_or_terms, &rel, num_rel_ret);
        }

        template<typename Index, typename ScorerType=ds2i::bm25>
        uint64_t operator()(Index const &index, wand_data<ScorerType> const& wdata, std::vector<term_id_vec> & and_or_terms, unsigned int K) const {
            return this->get<Index, ScorerType, false, true>(index, and_or_terms, nullptr, nullptr, &wdata, K);
        }

        template<typename Index, typename ScorerType=ds2i::bm25>
        uint64_t operator()(Index const &index, wand_data<ScorerType> const& wdata, std::vector<term_id_vec> & and_or_terms, std::vector<uint64_t> & rel, uint64_t * num_rel_ret, unsigned int K) const {
            return this->get<Index, ScorerType, true, true>(index, and_or_terms, &rel, num_rel_ret, &wdata, K);
        }

    private:
        template <typename Index, typename ScorerType, bool check_rel, bool rank_docs>
        uint64_t get(Index const& index, std::vector<term_id_vec> & and_or_terms, std::vector<uint64_t> * rel=nullptr, uint64_t * num_rel_ret=nullptr, wand_data<ScorerType> const* wdata=nullptr, unsigned int K=0) const
        {
            // check parameters
            if (rel != nullptr) {
                if (!check_rel) {
                    throw std::runtime_error("The template parameter check_rel must be true when rel is specified");
                }
                if (num_rel_ret == nullptr) {
                    throw std::runtime_error("The parameter num_rel_ret must be specified");
                }
            }
            if (wdata != nullptr) {
                if (!rank_docs) {
                    throw std::runtime_error("The template parameter rank_docs must be true when wdata is specified");
                }
                if (!with_freqs) {
                    throw std::runtime_error("The template parameter with_freqs must be true when wdata is specified");
                }
                if (K == 0) {
                    throw std::runtime_error("The parameter K must be greater than zero");
                }
            }

            if (check_rel) {
                *num_rel_ret = 0;
            }

            // handle empty query
            if (and_or_terms.empty())
                return 0;
            for (auto or_term: and_or_terms) {
                if (or_term.size() == 0)
                    return 0;
            }

            // remove duplicates
            if (normalize) {
                {
                    // remove duplicates inside the OR groups
                    for (std::size_t g = 0, g_end = and_or_terms.size(); g < g_end; ++g) {
                        remove_vector_duplicates_and_sort(and_or_terms.operator[](g));
                    }
                    // remove duplicate OR groups
                    remove_vector_duplicates_and_sort(and_or_terms);
                }
            }
            // end remove duplicates

            // num terms and num groups
            const std::size_t num_terms = std::accumulate(
                    and_or_terms.begin(),
                    and_or_terms.end(),
                    static_cast<std::size_t>(0),
                    [](const std::size_t result, const term_id_vec &synset_terms) {
                        return result + synset_terms.size();
                    }
            );
            const std::size_t num_groups = and_or_terms.size();

            // declare cnf enumerators: a two level document_enumerator
            typedef typename Index::document_enumerator enum_type;
            std::vector<std::vector<enum_type>> and_or_enums(num_groups);

            for (std::size_t g = 0; g < num_groups; ++g) {
                and_or_enums.reserve(and_or_terms[g].size());
                for (auto term: and_or_terms[g])
                    and_or_enums[g].push_back(index[term]);
            }
            // end cnf enumerators

            // sort by decreasing frequency the OR groups (second level) and by increasing frequency the AND groups
            if (normalize) {
                for (auto or_enums = and_or_enums.begin(), or_enums_end = and_or_enums.end(); or_enums != or_enums_end; ++or_enums) {
                    std::sort(
                            or_enums->begin(),
                            or_enums->end(),
                            [](enum_type const &lhs, enum_type const &rhs) {
                                return lhs.size() > rhs.size();
                            }
                    );
                }
                std::sort(
                        and_or_enums.begin(),
                        and_or_enums.end(),
                        [](std::vector<enum_type> const &lhs, std::vector<enum_type> const &rhs) {
                            return lhs[0].size() < rhs[0].size();
                        }
                );
            }
            // end sort

            // terms and groups as one-dimension vectors
            std::vector<enum_type> enums;
            enums.reserve(num_terms);
            std::vector<std::size_t> pos_to_group(num_terms);
            std::vector<unsigned int> group_to_start_pos(num_groups+1);

            group_to_start_pos[0] = 0;
            for (std::size_t g = 0, k=0; g < num_groups; ++g) {
                const unsigned int group_size = and_or_enums[g].size();
                group_to_start_pos[g+1] = group_to_start_pos[g] + group_size;
                for (std::size_t j=0; j < group_size; ++j, ++k) {
                    enums.push_back(and_or_enums[g][j]);
                    pos_to_group[k] = g;
                }
            }
            and_or_enums.clear();
            // end

            // support variables
            uint64_t results = 0;
            std::vector<std::size_t> matches(num_terms);
            std::vector<std::size_t> groups_min_docid(num_groups);
            std::size_t num_matches = 0;
            std::size_t num_groups_matched = 0;
            const uint64_t num_docs = index.num_docs();
            uint64_t cur_docid = enums[0].docid();
            for (std::size_t k = 1; k < group_to_start_pos[1]; ++k) {
                if (enums[k].docid() < cur_docid) {
                    cur_docid = enums[k].docid();
                }
            }

            // term weights
            std::vector<float> enums_weights;
            if (rank_docs) {
                enums_weights.reserve(num_terms);
                for (std::size_t i=0; i < num_terms; ++i) {
                    enums_weights.push_back(
                            ScorerType::query_term_weight(1ul, enums[i].size(), num_docs)
                    );
                }
            }
            TopK_Queue top_k(K);
            float score = 0;
            float norm_len = 0;

            // check_rel INTEGRATION
            const uint64_t * rel_it = nullptr;
            const uint64_t * rel_it_end = nullptr;
            if (check_rel) {
                remove_vector_duplicates_and_sort(*rel);
                rel_it_end = (rel_it = rel->data()) + rel->size();
            }
            // end

            // loop over documents to move on the cursor
            while (cur_docid < num_docs) {
                groups_min_docid[0] = num_docs;
                for (std::size_t k = 0, last_group = 0; k < num_terms; ++k) {
                    // group setting
                    const std::size_t group = pos_to_group[k];
                    if (num_groups_matched < group) { // the previous group has not matched
                        break;
                    }
                    if (last_group != group) { // move from one group to the other
                        groups_min_docid[group] = num_docs;
                        last_group = group;
                    }

                    // move on the cursor
                    enums[k].next_geq(cur_docid);
                    const uint64_t doc_id = enums[k].docid();

                    // check if there is a match, otherwise update groups_min_docid
                    if (doc_id == cur_docid) {
                        matches[num_matches++] = k;
                        if (num_groups_matched == group) {
                            num_groups_matched += 1;
                        }
                    } else if (doc_id < groups_min_docid[group]) {
                        groups_min_docid[group] = doc_id;
                    }
                }

                // move matches cursors, and update cur_docid to go on with the computation
                if (num_groups_matched == num_groups) {
                    if (rank_docs) {
                        score = 0;
                        norm_len = wdata->norm_len(cur_docid);

                        for (std::size_t i = 0; i < num_matches; ++i) {
                            const std::size_t k = matches[i];
                            score += enums_weights[k] * ScorerType::doc_term_weight(enums[k].freq(), norm_len);
                        }

                        top_k.insert(cur_docid, score);
                    } else {
                        ++results;
                        // check_rel INTEGRATION
                        if (check_rel) {
                            while (rel_it != rel_it_end && *rel_it < cur_docid) {
                                ++rel_it;
                            }
                            if (rel_it != rel_it_end && *rel_it == cur_docid) {
                                ++(*num_rel_ret);
                            }
                        }
                        if (with_freqs) { // freqs INTEGRATION
                            for (std::size_t i = 0; i < num_matches; ++i) {
                                const std::size_t k = matches[i];
                                if (enums[k].docid() == cur_docid) {
                                    do_not_optimize_away(enums[k].freq());
                                }
                            }
                        }
                    }

                    // advance the cursors
                    for (std::size_t i = 0; i < num_matches; ++i) {
                        const std::size_t k = matches[i];
                        const std::size_t group = pos_to_group[k];

                        // move cursor
                        enums[k].next();

                        // update groups_min_docid in order to compute correctly the next docid
                        const uint64_t doc_id = enums[k].docid();
                        if (doc_id < groups_min_docid[group]) {
                            groups_min_docid[group] = doc_id;
                        }
                    }

                    // next_docid is based on the maximum value among the scanned groups
                    // using a heap here could decrease the performance, mainly considering the low number of terms involved
                    uint64_t next_docid = 0;
                    for (std::size_t g = 0; g < num_groups; ++g) {
                        if (groups_min_docid[g] > next_docid) {
                            next_docid = groups_min_docid[g];
                        }
                    }
                    cur_docid = next_docid;
                } else {
                    // the new candidate docid is the min docid in the last group with a mismatch
                    cur_docid = groups_min_docid[num_groups_matched];
                }

                // update the loop status
                num_matches = 0;
                num_groups_matched = 0;
            }

            if (rank_docs) {
                top_k.finalize();
                const std::vector<docid_score> & top_k_list = top_k.get_list();
                results = top_k_list.size();

                if (check_rel) {
                    *num_rel_ret = 0;
                    std::unordered_set<uint64_t> rel_set(rel->begin(), rel->end());
                    for (unsigned int i=0, i_end=static_cast<unsigned int>(top_k_list.size()); i < i_end; ++i) {
                        if (rel_set.find(top_k_list[i].docid) != rel_set.end()) {
                            ++(*num_rel_ret);
                        }
                    }
                }
            }

            return results;
        }
    };


    template <bool normalize=true, bool with_freqs=true>
    struct opt_and_or_query {
    public:
        template<typename Index, typename ScorerType=ds2i::bm25>
        uint64_t operator()(Index const &index, std::vector<term_id_vec> & and_or_terms) const {
            return this->get<Index, ScorerType, false, false>(index, and_or_terms);
        }

        template<typename Index, typename ScorerType=ds2i::bm25>
        uint64_t operator()(Index const &index, std::vector<term_id_vec> & and_or_terms, std::vector<uint64_t> & rel, uint64_t * num_rel_ret) const {
            return this->get<Index, ScorerType, true, false>(index, and_or_terms, &rel, num_rel_ret);
        }

        template<typename Index, typename ScorerType=ds2i::bm25>
        uint64_t operator()(Index const &index, wand_data<ScorerType> const& wdata, std::vector<term_id_vec> & and_or_terms, unsigned int K) const {
            return this->get<Index, ScorerType, false, true>(index, and_or_terms, nullptr, nullptr, &wdata, K);
        }

        template<typename Index, typename ScorerType=ds2i::bm25>
        uint64_t operator()(Index const &index, wand_data<ScorerType> const& wdata, std::vector<term_id_vec> & and_or_terms, std::vector<uint64_t> & rel, uint64_t * num_rel_ret, unsigned int K) const {
            return this->get<Index, ScorerType, true, true>(index, and_or_terms, &rel, num_rel_ret, &wdata, K);
        }

    private:
        template <typename Index, typename ScorerType, bool check_rel, bool rank_docs>
        uint64_t get(Index const& index, std::vector<term_id_vec> & and_or_terms, std::vector<uint64_t> * rel=nullptr, uint64_t * num_rel_ret=nullptr, wand_data<ScorerType> const* wdata=nullptr, unsigned int K=0) const
        {
            // check parameters
            if (rel != nullptr) {
                if (!check_rel) {
                    throw std::runtime_error("The template parameter check_rel must be true when rel is specified");
                }
                if (num_rel_ret == nullptr) {
                    throw std::runtime_error("The parameter num_rel_ret must be specified");
                }
            }
            if (wdata != nullptr) {
                if (!rank_docs) {
                    throw std::runtime_error("The template parameter rank_docs must be true when wdata is specified");
                }
                if (!with_freqs) {
                    throw std::runtime_error("The template parameter with_freqs must be true when wdata is specified");
                }
                if (K == 0) {
                    throw std::runtime_error("The parameter K must be greater than zero");
                }
            }

            if (check_rel) {
                *num_rel_ret = 0;
            }

            // handle empty query
            if (and_or_terms.empty())
                return 0;
            for (auto or_term=and_or_terms.cbegin(); or_term != and_or_terms.cend(); ++or_term) {
                if (or_term->size() == 0)
                    return 0;
            }

            // remove duplicates
            if (normalize) {
                {
                    // remove duplicates inside the OR groups
                    for (std::size_t g = 0, g_end = and_or_terms.size(); g < g_end; ++g) {
                        remove_vector_duplicates_and_sort(and_or_terms.operator[](g));
                    }
                    // remove duplicate OR groups
                    remove_vector_duplicates_and_sort(and_or_terms);
                }
            }
            // end remove duplicates

            // num terms and num groups
            const std::size_t num_terms = std::accumulate(
                    and_or_terms.begin(),
                    and_or_terms.end(),
                    static_cast<std::size_t>(0),
                    [](const std::size_t result, const term_id_vec &synset_terms) {
                        return result + synset_terms.size();
                    }
            );
            const std::size_t num_groups = and_or_terms.size();

            // declare cnf enumerators: a two level document_enumerator
            typedef typename Index::document_enumerator enum_type;
            std::vector<std::pair<uint64_t, std::vector<enum_type>>> and_or_enums(num_groups);

            for (std::size_t g = 0; g < num_groups; ++g) {
                and_or_enums[g].first = 0;
                and_or_enums[g].second.reserve(and_or_terms[g].size());
                for (auto term: and_or_terms[g]) {
                    and_or_enums[g].first += index[term].size();
                    and_or_enums[g].second.push_back(index[term]);
                }
            }
            // end cnf enumerators

            // sort by decreasing frequency the OR groups (second level) and by increasing frequency the AND groups
            if (normalize) {
                for (auto or_enums = and_or_enums.begin(), or_enums_end = and_or_enums.end(); or_enums != or_enums_end; ++or_enums) {
                    std::sort(
                            or_enums->second.begin(),
                            or_enums->second.end(),
                            [](enum_type const &lhs, enum_type const &rhs) {
                                return lhs.size() > rhs.size();
                            }
                    );
                }
                std::sort(
                        and_or_enums.begin(),
                        and_or_enums.end(),
                        [](std::pair<uint64_t, std::vector<enum_type>> const &lhs, std::pair<uint64_t, std::vector<enum_type>> const &rhs) {
                            return lhs.first < rhs.first;
                        }
                );
            }
            // end sort

            // terms and groups as one-dimension vectors
            const uint64_t num_docs = index.num_docs();
            std::vector<enum_type> enums;
            enums.reserve(num_terms);

            std::vector<unsigned int> group_to_start_pos(num_groups+2);
            group_to_start_pos[0] = 0;
            for (std::size_t g = 0, k=0; g < num_groups; ++g) {
                const unsigned int group_size = and_or_enums[g].second.size();
                group_to_start_pos[g+1] = group_to_start_pos[g] + group_size;
                for (std::size_t j=0; j < group_size; ++j, ++k) {
                    enums.push_back(and_or_enums[g].second[j]);
                }
            }
            group_to_start_pos[num_groups+1] = group_to_start_pos[num_groups];
            and_or_enums.clear();
            // end

            // term weights
            std::vector<float> enums_weights;
            if (rank_docs) {
                enums_weights.reserve(num_terms);
                for (std::size_t i=0; i < num_terms; ++i) {
                    enums_weights.push_back(
                            ScorerType::query_term_weight(1ul, enums[i].size(), num_docs)
                    );
                }
            }
            TopK_Queue top_k(K);
            float score = 0;
            float norm_len = 0;

            // support variables
            uint64_t results = 0;
            std::size_t num_groups_matched = 0;
            // cur_docid is the candidate id. It is initialized with the minimum docid of the first group
            uint64_t cur_docid = enums[0].docid();
            for (std::size_t k = 1; k < group_to_start_pos[1]; ++k) {
                if (enums[k].docid() < cur_docid) {
                    cur_docid = enums[k].docid();
                }
            }

            // check_rel INTEGRATION
            const uint64_t * rel_it = nullptr;
            const uint64_t * rel_it_end = nullptr;
            if (check_rel) {
                remove_vector_duplicates_and_sort(*rel);
                rel_it_end = (rel_it = rel->data()) + rel->size();
            }
            // end

            std::size_t k = 0; // term index
            // loop over documents to move on the cursor
            while (cur_docid < num_docs) {
                std::size_t k_end = group_to_start_pos[num_groups_matched+1];
                while (k < k_end) {
                    // move on the cursor
                    enums[k].next_geq(cur_docid);
                    const uint64_t doc_id = enums[k].docid();

                    // check if there is a match
                    if (doc_id == cur_docid) {
                        // I can skip the remaining part of this group
                        k = group_to_start_pos[++num_groups_matched];
                        k_end = group_to_start_pos[num_groups_matched+1];
                    } else {
                        // go on with the other terms of the group
                        ++k;
                    }
                }

                // move matches cursors, and update cur_docid to go on with the computation
                if (num_groups_matched == num_groups) {
                    // advance all cursors and update the "virtual score" (if it is needed)
                    for (std::size_t i = 0; i < num_terms; ++i) {
                        enums[i].next_geq(cur_docid);
                    }

                    // update the score
                    if (rank_docs) {
                        score = 0;
                        norm_len = wdata->norm_len(cur_docid);

                        for (std::size_t i = 0; i < enums.size(); ++i) {
                            score += enums_weights[i] * ScorerType::doc_term_weight(enums[i].freq(), norm_len);
                        }

                        top_k.insert(cur_docid, score);
                    } else {
                        ++results;
                        // check_rel INTEGRATION
                        if (check_rel) {
                            while (rel_it != rel_it_end && *rel_it < cur_docid) {
                                ++rel_it;
                            }
                            if (rel_it != rel_it_end && *rel_it == cur_docid) {
                                ++(*num_rel_ret);
                            }
                        }
                        if (with_freqs) { // freqs INTEGRATION
                            for (std::size_t i = 0; i < num_terms; ++i) {
                                if (enums[i].docid() == cur_docid) {
                                    do_not_optimize_away(enums[i].freq());
                                }
                            }
                        }
                    }

                    // next_docid is the minimum docid of the first group
                    uint64_t next_docid = num_docs;
                    for (std::size_t i = 0, i_end=group_to_start_pos[1]; i < i_end; ++i) {
                        uint64_t docid_i = enums[i].docid();
                        if (docid_i == cur_docid) {
                            enums[i].next();
                            docid_i = enums[i].docid();
                        }
                        if (docid_i < next_docid) {
                            next_docid = docid_i;
                        }
                    }

                    cur_docid = next_docid;
                    k = group_to_start_pos[1];
                    num_groups_matched = 1;

                } else {
                    // the new candidate docid is the min docid in the last group with a mismatch
                    uint64_t next_docid = num_docs;
                    for (std::size_t i = group_to_start_pos[num_groups_matched], i_end=group_to_start_pos[num_groups_matched+1]; i < i_end; ++i) {
                        uint64_t docid_i = enums[i].docid();
                        if (docid_i < next_docid) {
                            next_docid = docid_i;
                        }
                    }
                    cur_docid = next_docid;

                    // if the next_docid has been chosen from the first group I can start from the second one
                    if (num_groups_matched == 0) {
                        k = group_to_start_pos[1];
                        num_groups_matched = 1;
                    } else {
                        k = 0;
                        num_groups_matched = 0;
                    }
                }
            }

            if (rank_docs) {
                top_k.finalize();
                const std::vector<docid_score> & top_k_list = top_k.get_list();
                results = top_k_list.size();

                if (check_rel) {
                    *num_rel_ret = 0;
                    std::unordered_set<uint64_t> rel_set(rel->begin(), rel->end());
                    for (unsigned int i=0, i_end=static_cast<unsigned int>(top_k_list.size()); i < i_end; ++i) {
                        if (rel_set.find(top_k_list[i].docid) != rel_set.end()) {
                            ++(*num_rel_ret);
                        }
                    }
                }
            }

            return results;
        }
    };


    template <bool normalize=true, bool with_freqs=true>
    struct and_query {
    public:
        template<typename Index, typename ScorerType=ds2i::bm25>
        uint64_t operator()(Index const &index, term_id_vec & terms) const {
            return this->get<Index, ScorerType, false, false>(index, terms);
        }

        template<typename Index, typename ScorerType=ds2i::bm25>
        uint64_t operator()(Index const &index, term_id_vec & terms, std::vector<uint64_t> & rel, uint64_t * num_rel_ret) const {
            return this->get<Index, ScorerType, true, false>(index, terms, &rel, num_rel_ret);
        }

        template<typename Index, typename ScorerType=ds2i::bm25>
        uint64_t operator()(Index const &index, wand_data<ScorerType> const& wdata, term_id_vec & terms, unsigned int K) const {
            return this->get<Index, ScorerType, false, true>(index, terms, nullptr, nullptr, &wdata, K);
        }

        template<typename Index, typename ScorerType=ds2i::bm25>
        uint64_t operator()(Index const &index, wand_data<ScorerType> const& wdata, term_id_vec & terms, std::vector<uint64_t> & rel, uint64_t * num_rel_ret, unsigned int K) const {
            return this->get<Index, ScorerType, true, true>(index, terms, &rel, num_rel_ret, &wdata, K);
        }

    private:
        template <typename Index, typename ScorerType, bool check_rel, bool rank_docs>
        uint64_t get(Index const& index, term_id_vec & terms, std::vector<uint64_t> * rel=nullptr, uint64_t * num_rel_ret=nullptr, wand_data<ScorerType> const* wdata=nullptr, unsigned int K=0) const
        {
            // check parameters
            if (rel != nullptr) {
                if (!check_rel) {
                    throw std::runtime_error("The template parameter check_rel must be true when rel is specified");
                }
                if (num_rel_ret == nullptr) {
                    throw std::runtime_error("The parameter num_rel_ret must be specified");
                }
            }
            if (wdata != nullptr) {
                if (!rank_docs) {
                    throw std::runtime_error("The template parameter rank_docs must be true when wdata is specified");
                }
                if (!with_freqs) {
                    throw std::runtime_error("The template parameter with_freqs must be true when wdata is specified");
                }
                if (K == 0) {
                    throw std::runtime_error("The parameter K must be greater than zero");
                }
            }

            if (check_rel) {
                *num_rel_ret = 0;
            }

            // handle empty query
            if (terms.empty()) {
                return 0;
            }
            // remove duplicates
            if (normalize) {
                remove_vector_duplicates_and_sort(terms);
            }

            typedef typename Index::document_enumerator enum_type;
            const uint64_t num_docs = index.num_docs();
            std::vector<enum_type> enums;
            enums.reserve(terms.size());

            for (auto term: terms) {
                enums.push_back(index[term]);
            }


            // sort by increasing frequency
            if (normalize) {
                std::sort(enums.begin(), enums.end(),
                          [](enum_type const &lhs, enum_type const &rhs) {
                              return lhs.size() < rhs.size();
                          });
            }
            // term weights
            std::vector<float> enums_weights;
            if (rank_docs) {
                const std::size_t num_terms = enums.size();
                enums_weights.reserve(num_terms);
                for (std::size_t i=0; i < num_terms; ++i) {
                    enums_weights.push_back(
                            ScorerType::query_term_weight(1ul, enums[i].size(), num_docs)
                    );
                }
            }
            TopK_Queue top_k(K);
            float score = 0;
            float norm_len = 0;

            uint64_t results = 0;
            uint64_t candidate = enums[0].docid();

            // check_rel INTEGRATION
            const uint64_t * rel_it = nullptr;
            const uint64_t * rel_it_end = nullptr;
            if (check_rel) {
                remove_vector_duplicates_and_sort(*rel);
                rel_it_end = (rel_it = rel->data()) + rel->size();
            }
            // end

            size_t i = 1; // term index
            while (candidate < num_docs) {
                // compute next candidate docid and update score
                for (; i < enums.size(); ++i) {
                    enums[i].next_geq(candidate);
                    if (enums[i].docid() != candidate) {
                        candidate = enums[i].docid();
                        i = 0;
                        break;
                    }
                }

                if (i == enums.size()) {
                    // update the score
                    if (rank_docs) {
                        score = 0;
                        norm_len = wdata->norm_len(candidate);

                        for (std::size_t i = 0; i < enums.size(); ++i) {
                            score += enums_weights[i] * ScorerType::doc_term_weight(enums[i].freq(), norm_len);
                        }

                        top_k.insert(candidate, score);
                    } else {
                        ++results;
                        // check_rel INTEGRATION
                        if (check_rel) {
                            while (rel_it != rel_it_end && *rel_it < candidate) {
                                ++rel_it;
                            }
                            if (rel_it != rel_it_end && *rel_it == candidate) {
                                ++(*num_rel_ret);
                            }
                        }
                        if (with_freqs) { // freqs INTEGRATION
                            for (std::size_t i = 0; i < enums.size(); ++i) {
                                do_not_optimize_away(enums[i].freq());
                            }
                        }
                    }
                    enums[0].next();
                    candidate = enums[0].docid();
                    i = 1;
                }
            }

            if (rank_docs) {
                top_k.finalize();
                const std::vector<docid_score> & top_k_list = top_k.get_list();
                results = top_k_list.size();

                if (check_rel) {
                    *num_rel_ret = 0;
                    std::unordered_set<uint64_t> rel_set(rel->begin(), rel->end());
                    for (unsigned int i=0, i_end=static_cast<unsigned int>(top_k_list.size()); i < i_end; ++i) {
                        if (rel_set.find(top_k_list[i].docid) != rel_set.end()) {
                            ++(*num_rel_ret);
                        }
                    }
                }
            }

            return results;
        }
    };


    template <bool normalize=true, bool with_freqs=true>
    struct or_query {
    public:
        template<typename Index, typename ScorerType=ds2i::bm25>
        uint64_t operator()(Index const &index, term_id_vec & terms) const {
            return this->get<Index, ScorerType, false, false>(index, terms);
        }

        template<typename Index, typename ScorerType=ds2i::bm25>
        uint64_t operator()(Index const &index, term_id_vec & terms, std::vector<uint64_t> & rel, uint64_t * num_rel_ret) const {
            return this->get<Index, ScorerType, true, false>(index, terms, &rel, num_rel_ret);
        }

        template<typename Index, typename ScorerType=ds2i::bm25>
        uint64_t operator()(Index const &index, wand_data<ScorerType> const& wdata, term_id_vec & terms, unsigned int K) const {
            return this->get<Index, ScorerType, false, true>(index, terms, nullptr, nullptr, &wdata, K);
        }

        template<typename Index, typename ScorerType=ds2i::bm25>
        uint64_t operator()(Index const &index, wand_data<ScorerType> const& wdata, term_id_vec & terms, std::vector<uint64_t> & rel, uint64_t * num_rel_ret, unsigned int K) const {
            return this->get<Index, ScorerType, true, true>(index, terms, &rel, num_rel_ret, &wdata, K);
        }

    private:
        template <typename Index, typename ScorerType, bool check_rel, bool rank_docs>
        uint64_t get(Index const& index, term_id_vec & terms, std::vector<uint64_t> * rel=nullptr, uint64_t * num_rel_ret=nullptr, wand_data<ScorerType> const* wdata=nullptr, unsigned int K=0) const
        {
            // check parameters
            if (rel != nullptr) {
                if (!check_rel) {
                    throw std::runtime_error("The template parameter check_rel must be true when rel is specified");
                }
                if (num_rel_ret == nullptr) {
                    throw std::runtime_error("The parameter num_rel_ret must be specified");
                }
            }
            if (wdata != nullptr) {
                if (!rank_docs) {
                    throw std::runtime_error("The template parameter rank_docs must be true when wdata is specified");
                }
                if (!with_freqs) {
                    throw std::runtime_error("The template parameter with_freqs must be true when wdata is specified");
                }
                if (K == 0) {
                    throw std::runtime_error("The parameter K must be greater than zero");
                }
            }

            if (check_rel) {
                *num_rel_ret = 0;
            }

            // handle empty query
            if (terms.empty()) {
                return 0;
            }
            // remove duplicates
            if (normalize) {
                remove_vector_duplicates_and_sort(terms);
            }

            typedef typename Index::document_enumerator enum_type;
            const uint64_t num_docs = index.num_docs();
            std::vector<enum_type> enums;
            enums.reserve(terms.size());

            for (auto term: terms) {
                enums.push_back(index[term]);
            }

            // term weights
            std::vector<float> enums_weights;
            if (rank_docs) {
                const std::size_t num_terms = enums.size();
                enums_weights.reserve(num_terms);
                for (std::size_t i=0; i < num_terms; ++i) {
                    enums_weights.push_back(
                            ScorerType::query_term_weight(1ul, enums[i].size(), num_docs)
                    );
                }
            }
            TopK_Queue top_k(K);
            float score = 0;
            float norm_len = 0;

            uint64_t results = 0;
            uint64_t cur_doc = std::min_element(enums.begin(), enums.end(),
                                                [](enum_type const& lhs, enum_type const& rhs) {
                                                    return lhs.docid() < rhs.docid();
                                                })->docid();

            // check_rel INTEGRATION
            const uint64_t * rel_it = nullptr;
            const uint64_t * rel_it_end = nullptr;
            if (check_rel) {
                remove_vector_duplicates_and_sort(*rel);
                rel_it_end = (rel_it = rel->data()) + rel->size();
            }
            // end

            while (cur_doc < num_docs) {
                // init the variables used by the scorer
                if (rank_docs) {
                    score = 0;
                    norm_len = wdata->norm_len(cur_doc);
                }

                // compute next candidate docid and update score
                uint64_t next_doc = num_docs;
                for (size_t i = 0; i < enums.size(); ++i) {
                    if (enums[i].docid() == cur_doc) {
                        if (rank_docs) {
                            score += enums_weights[i] * ScorerType::doc_term_weight(enums[i].freq(), norm_len);
                        } else {
                            if (with_freqs) { // freqs INTEGRATION
                                do_not_optimize_away(enums[i].freq());
                            }
                        }
                        enums[i].next();
                    }
                    if (enums[i].docid() < next_doc) {
                        next_doc = enums[i].docid();
                    }
                }

                // update the score
                if (rank_docs) {
                    top_k.insert(cur_doc, score);
                } else {
                    ++results;
                    // check_rel INTEGRATION
                    if (check_rel) {
                        while (rel_it != rel_it_end && *rel_it < cur_doc) {
                            ++rel_it;
                        }
                        if (rel_it != rel_it_end && *rel_it == cur_doc) {
                            ++(*num_rel_ret);
                        }
                    }
                }

                cur_doc = next_doc;
            }

            if (rank_docs) {
                top_k.finalize();
                const std::vector<docid_score> & top_k_list = top_k.get_list();
                results = top_k_list.size();

                if (check_rel) {
                    *num_rel_ret = 0;
                    std::unordered_set<uint64_t> rel_set(rel->begin(), rel->end());
                    for (unsigned int i=0, i_end=static_cast<unsigned int>(top_k_list.size()); i < i_end; ++i) {
                        if (rel_set.find(top_k_list[i].docid) != rel_set.end()) {
                            ++(*num_rel_ret);
                        }
                    }
                }
            }

            return results;
        }
    };


    struct maxscore_query {
    public:
        template<typename Index, typename ScorerType=ds2i::bm25>
        uint64_t operator()(Index const &index, term_id_vec & terms) const {
            throw std::runtime_error("this constructor cannot be implemented");
            return this->get<Index, ScorerType, false>(index, terms, nullptr, nullptr);
        }

        template<typename Index, typename ScorerType=ds2i::bm25>
        uint64_t operator()(Index const &index, term_id_vec & terms, std::vector<uint64_t> & rel, uint64_t * num_rel_ret) const {
            throw std::runtime_error("this constructor cannot be implemented");
            return this->get<Index, ScorerType, true>(index, terms, &rel, num_rel_ret);
        }

        template<typename Index, typename ScorerType=ds2i::bm25>
        uint64_t operator()(Index const &index, wand_data<ScorerType> const& wdata, term_id_vec & terms, unsigned int K) const {
            return this->get<Index, ScorerType, false>(index, terms, nullptr, nullptr, &wdata, K);
        }

        template<typename Index, typename ScorerType=ds2i::bm25>
        uint64_t operator()(Index const &index, wand_data<ScorerType> const& wdata, term_id_vec & terms, std::vector<uint64_t> & rel, uint64_t * num_rel_ret, unsigned int K) const {
            return this->get<Index, ScorerType, true>(index, terms, &rel, num_rel_ret, &wdata, K);
        }

    private:
        template <typename Index, typename ScorerType, bool check_rel>
        uint64_t get(Index const& index, term_id_vec & terms, std::vector<uint64_t> * rel=nullptr, uint64_t * num_rel_ret=nullptr, wand_data<ScorerType> const* wdata=nullptr, unsigned int K=0) const
        {
            // check parameters
            if (K == 0) {
                throw std::runtime_error("The parameter K must be greater than zero");
            }

            // handle empty query
            if (terms.empty()) {
                return 0;
            }

            auto query_term_freqs = query_freqs(terms);

            const uint64_t num_docs = index.num_docs();
            typedef typename Index::document_enumerator enum_type;
            struct scored_enum {
                enum_type docs_enum;
                float q_weight;
                float max_weight;
            };

            std::vector<scored_enum> enums;
            enums.reserve(query_term_freqs.size());

            for (auto term: query_term_freqs) {
                auto list = index[term.first];
                auto q_weight = ScorerType::query_term_weight
                        (term.second, list.size(), num_docs);
                auto max_weight = q_weight * wdata->max_term_weight(term.first);
                enums.push_back(scored_enum {std::move(list), q_weight, max_weight});
            }

            std::vector<scored_enum*> ordered_enums;
            ordered_enums.reserve(enums.size());
            for (auto& en: enums) {
                ordered_enums.push_back(&en);
            }

            // sort enumerators by increasing maxscore
            std::sort(ordered_enums.begin(), ordered_enums.end(),
                      [](scored_enum* lhs, scored_enum* rhs) {
                          return lhs->max_weight < rhs->max_weight;
                      });

            std::vector<float> upper_bounds(ordered_enums.size());
            upper_bounds[0] = ordered_enums[0]->max_weight;
            for (size_t i = 1; i < ordered_enums.size(); ++i) {
                upper_bounds[i] = upper_bounds[i - 1] + ordered_enums[i]->max_weight;
            }

            uint64_t non_essential_lists = 0;
            uint64_t cur_doc =
                    std::min_element(enums.begin(), enums.end(),
                                     [](scored_enum const& lhs, scored_enum const& rhs) {
                                         return lhs.docs_enum.docid() < rhs.docs_enum.docid();
                                     })
                            ->docs_enum.docid();

            TopK_Queue top_k(K);
            while (non_essential_lists < ordered_enums.size() &&
                   cur_doc < num_docs) {
                float score = 0;
                float norm_len = wdata->norm_len(cur_doc);
                uint64_t next_doc = num_docs;
                for (size_t i = non_essential_lists; i < ordered_enums.size(); ++i) {
                    if (ordered_enums[i]->docs_enum.docid() == cur_doc) {
                        score += ordered_enums[i]->q_weight * ScorerType::doc_term_weight
                                (ordered_enums[i]->docs_enum.freq(), norm_len);
                        ordered_enums[i]->docs_enum.next();
                    }
                    if (ordered_enums[i]->docs_enum.docid() < next_doc) {
                        next_doc = ordered_enums[i]->docs_enum.docid();
                    }
                }

                // try to complete evaluation with non-essential lists
                for (size_t i = non_essential_lists - 1; i + 1 > 0; --i) {
                    if (!top_k.would_enter(score + upper_bounds[i])) {
                        break;
                    }
                    ordered_enums[i]->docs_enum.next_geq(cur_doc);
                    if (ordered_enums[i]->docs_enum.docid() == cur_doc) {
                        score += ordered_enums[i]->q_weight * ScorerType::doc_term_weight
                                (ordered_enums[i]->docs_enum.freq(), norm_len);
                    }
                }

                if (top_k.insert(cur_doc, score)) {
                    // update non-essential lists
                    while (non_essential_lists < ordered_enums.size() &&
                           !top_k.would_enter(upper_bounds[non_essential_lists])) {
                        non_essential_lists += 1;
                    }
                }

                cur_doc = next_doc;
            }

            top_k.finalize();

            const std::vector<docid_score> & top_k_list = top_k.get_list();
            if (check_rel) {
                *num_rel_ret = 0;
                std::unordered_set<uint64_t> rel_set(rel->begin(), rel->end());
                for (unsigned int i=0, i_end=static_cast<unsigned int>(top_k_list.size()); i < i_end; ++i) {
                    if (rel_set.find(top_k_list[i].docid) != rel_set.end()) {
                        ++(*num_rel_ret);
                    }
                }
            }

            return top_k_list.size();
        }
    };
}

#endif //INDEX_PARTITIONING_QUERY_EVALUATION_HPP
