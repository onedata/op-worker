/**
 * @file rt_heap_nif.cc
 * @author Krzysztof Trzepla
 * @copyright (C): 2014 ACK CYFRONET AGH
 * This software is released under the MIT license
 * cited in 'LICENSE.txt'.
 */

#include "nifpp.h"
#include "rt_heap.h"
#include "rt_container.h"

#include <functional>
#include <string>
#include <vector>
#include <set>
#include <memory>

using namespace one::provider;

static int load(ErlNifEnv *env, void **priv, ERL_NIF_TERM load_info)
{
    nifpp::register_resource<std::shared_ptr<rt_container>>(env, nullptr,
                                                            "rt_container");
    return 0;
}

static ERL_NIF_TERM init_nif(ErlNifEnv *env, int argc,
                             const ERL_NIF_TERM argv[])
{
    try {
        ErlNifUInt64 block_size;
        nifpp::get_throws(env, argv[0], block_size);
        auto heap = nifpp::construct_resource<std::shared_ptr<rt_container>>(
            new rt_heap(block_size));
        return nifpp::make(env, std::make_tuple(nifpp::str_atom("ok"),
                                                nifpp::make(env, heap)));
    }
    catch (...) {
        return enif_make_badarg(env);
    }
}

static ERL_NIF_TERM push_nif(ErlNifEnv *env, int argc,
                             const ERL_NIF_TERM argv[])
{
    try {
        nifpp::resource_ptr<std::shared_ptr<rt_container>> heap;
        nifpp::str_atom record_name;
        std::string file_id;
        std::string provider_id;
        ErlNifUInt64 offset, size;
        int priority;
        std::list<ErlNifPid> pids;
        auto record = std::make_tuple(std::ref(record_name), std::ref(file_id),
                                      std::ref(provider_id), std::ref(offset),
                                      std::ref(size), std::ref(priority),
                                      std::ref(pids));

        nifpp::get_throws(env, argv[0], heap);
        nifpp::get_throws(env, argv[1], record);

        rt_block block(file_id, provider_id, offset, size, priority, pids);
        (*heap)->push(block);

        return nifpp::make(env, nifpp::str_atom("ok"));
    }
    catch (const std::runtime_error &error) {
        std::string message = error.what();
        return nifpp::make(env,
                           std::make_tuple(nifpp::str_atom("error"), message));
    }
    catch (...) {
        return enif_make_badarg(env);
    }
}

static ERL_NIF_TERM pop_nif(ErlNifEnv *env, int argc, const ERL_NIF_TERM argv[])
{
    try {
        nifpp::resource_ptr<std::shared_ptr<rt_container>> heap;
        nifpp::get_throws(env, argv[0], heap);

        rt_block block = (*heap)->pop();
        auto record = std::make_tuple(
            nifpp::str_atom("rt_block"), block.file_id(), block.provider_id(),
            block.offset(), block.size(), block.priority(), block.pids());

        return nifpp::make(env, std::make_tuple(nifpp::str_atom("ok"), record));
    }
    catch (const std::runtime_error &error) {
        std::string message = error.what();
        return nifpp::make(env,
                           std::make_tuple(nifpp::str_atom("error"), message));
    }
    catch (...) {
        return enif_make_badarg(env);
    }
}

static ErlNifFunc nif_funcs[] = {{"init_nif", 1, init_nif},
                                 {"push_nif", 2, push_nif},
                                 {"pop_nif", 1, pop_nif}};

ERL_NIF_INIT(rt_container, nif_funcs, load, NULL, NULL, NULL)
