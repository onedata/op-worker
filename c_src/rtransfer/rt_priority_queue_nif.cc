/**
 * @file rt_queue_nif.cc
 * @author Krzysztof Trzepla
 * @copyright (C): 2014 ACK CYFRONET AGH
 * This software is released under the MIT license
 * cited in 'LICENSE.txt'.
 */

#include "../nifpp.h"
#include "rt_local_term.h"
#include "rt_exception.h"
#include "rt_priority_queue.h"

#include <string>
#include <functional>
#include <set>

using namespace one::provider;

namespace {

int load(ErlNifEnv *env, void **priv, ERL_NIF_TERM load_info)
{
    nifpp::register_resource<rt_priority_queue>(env, nullptr,
                                                "rt_priority_queue");
    return 0;
}

ERL_NIF_TERM init_nif(ErlNifEnv *env, int argc, const ERL_NIF_TERM argv[])
{
    try {
        ErlNifUInt64 block_size;
        nifpp::get_throws(env, argv[0], block_size);
        auto queue = nifpp::construct_resource<rt_priority_queue>(block_size);

        return nifpp::make(env, std::make_tuple(nifpp::str_atom("ok"),
                                                nifpp::make(env, queue)));
    }
    catch (...) {
        return enif_make_badarg(env);
    }
}

ERL_NIF_TERM push_nif(ErlNifEnv *env, int argc, const ERL_NIF_TERM argv[])
{
    try {
        nifpp::resource_ptr<rt_priority_queue> queue;
        nifpp::str_atom record_name;
        std::string file_id;
        nifpp::TERM provider_ref;
        ErlNifUInt64 offset, size, priority;
        int retry;
        std::list<nifpp::TERM> terms;
        auto record = std::make_tuple(std::ref(record_name), std::ref(file_id),
                                      std::ref(provider_ref), std::ref(offset),
                                      std::ref(size), std::ref(priority),
                                      std::ref(retry), std::ref(terms));

        nifpp::get_throws(env, argv[0], queue);
        nifpp::get_throws(env, argv[1], record);

        std::set<rt_local_term> rt_local_terms;
        for (const auto &term : terms)
            rt_local_terms.insert(rt_local_term(term));

        rt_block block(file_id, rt_local_term(provider_ref), offset, size,
                       priority, retry, rt_local_terms);
        queue->push(block);
        ErlNifUInt64 queue_size = queue->size();

        return nifpp::make(env,
                           std::make_tuple(nifpp::str_atom("ok"), queue_size));
    }
    catch (const rt_exception &ex) {
        std::string message = ex.what();
        return nifpp::make(env, std::make_tuple(nifpp::str_atom("error"),
                                                nifpp::str_atom(message)));
    }
    catch (...) {
        return enif_make_badarg(env);
    }
}

ERL_NIF_TERM pop_nif(ErlNifEnv *env, int argc, const ERL_NIF_TERM argv[])
{
    try {
        nifpp::resource_ptr<rt_priority_queue> queue;
        nifpp::get_throws(env, argv[0], queue);

        rt_block block = queue->pop();
        ErlNifUInt64 queue_size = queue->size();

        std::list<nifpp::TERM> terms;
        for (const auto &term : block.terms())
            terms.push_back(term.get(env));

        nifpp::binary fileId{block.file_id().size()};
        std::copy(block.file_id().begin(), block.file_id().end(), fileId.data);

        auto record = std::make_tuple(
            nifpp::str_atom("rt_block"), nifpp::make(env, fileId),
            block.provider_ref().get(env), block.offset(), block.size(),
            block.priority(), block.retry(), terms);

        return nifpp::make(
            env, std::make_tuple(nifpp::str_atom("ok"), queue_size, record));
    }
    catch (const rt_exception &ex) {
        std::string message = ex.what();
        return nifpp::make(env, std::make_tuple(nifpp::str_atom("error"),
                                                nifpp::str_atom(message)));
    }
    catch (...) {
        return enif_make_badarg(env);
    }
}

ERL_NIF_TERM change_counter_nif(ErlNifEnv *env, int argc,
                                const ERL_NIF_TERM argv[])
{
    try {
        nifpp::resource_ptr<rt_priority_queue> queue;
        std::string file_id;
        ErlNifUInt64 offset, size;
        ErlNifSInt64 change;
        nifpp::get_throws(env, argv[0], queue);
        nifpp::get_throws(env, argv[1], file_id);
        nifpp::get_throws(env, argv[2], offset);
        nifpp::get_throws(env, argv[3], size);
        nifpp::get_throws(env, argv[4], change);

        queue->change_counter(file_id, offset, size, change);
        ErlNifUInt64 queue_size = queue->size();

        return nifpp::make(env,
                           std::make_tuple(nifpp::str_atom("ok"), queue_size));
    }
    catch (const rt_exception &ex) {
        std::string message = ex.what();
        return nifpp::make(env, std::make_tuple(nifpp::str_atom("error"),
                                                nifpp::str_atom(message)));
    }
    catch (...) {
        return enif_make_badarg(env);
    }
}

ErlNifFunc nif_funcs[] = {{"init_nif", 1, init_nif},
                          {"push_nif", 2, push_nif},
                          {"pop_nif", 1, pop_nif},
                          {"change_counter_nif", 5, change_counter_nif}};

ERL_NIF_INIT(rt_priority_queue, nif_funcs, load, NULL, NULL, NULL)
}
