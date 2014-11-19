/**
 * @file rt_queue_nif.cc
 * @author Krzysztof Trzepla
 * @copyright (C): 2014 ACK CYFRONET AGH
 * This software is released under the MIT license
 * cited in 'LICENSE.txt'.
 */

#include "nifpp.h"
#include "rt_map.h"
#include "rt_exception.h"
#include "rt_priority_queue.h"

#include <functional>
#include <string>
#include <vector>
#include <set>

using namespace one::provider;

static int load(ErlNifEnv *env, void **priv, ERL_NIF_TERM load_info)
{
    nifpp::register_resource<rt_priority_queue>(env, nullptr,
                                                "rt_priority_queue");
    return 0;
}

static ERL_NIF_TERM init_nif(ErlNifEnv *env, int argc,
                             const ERL_NIF_TERM argv[])
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

static ERL_NIF_TERM push_nif(ErlNifEnv *env, int argc,
                             const ERL_NIF_TERM argv[])
{
    try {
        nifpp::resource_ptr<rt_priority_queue> queue;
        nifpp::str_atom record_name;
        std::string file_id;
        nifpp::TERM provider_ref;
        ErlNifUInt64 offset, size, priority;
        std::list<nifpp::TERM> terms;
        auto record = std::make_tuple(std::ref(record_name), std::ref(file_id),
                                      std::ref(provider_ref), std::ref(offset),
                                      std::ref(size), std::ref(priority),
                                      std::ref(terms));

        nifpp::get_throws(env, argv[0], queue);
        nifpp::get_throws(env, argv[1], record);

        rt_block block(file_id, provider_ref, offset, size, priority, terms);
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

static ERL_NIF_TERM fetch_nif(ErlNifEnv *env, int argc,
                              const ERL_NIF_TERM argv[])
{
    try {
        nifpp::resource_ptr<rt_priority_queue> queue;
        nifpp::get_throws(env, argv[0], queue);

        rt_block block = queue->fetch();
        ErlNifUInt64 queue_size = queue->size();
        auto record = std::make_tuple(
            nifpp::str_atom("rt_block"), block.file_id(), block.provider_ref(),
            block.offset(), block.size(), block.priority(), block.terms());

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

static ErlNifFunc nif_funcs[] = {{"init_nif", 1, init_nif},
                                 {"push_nif", 2, push_nif},
                                 {"fetch_nif", 1, fetch_nif}};

ERL_NIF_INIT(rt_priority_queue, nif_funcs, load, NULL, NULL, NULL)
