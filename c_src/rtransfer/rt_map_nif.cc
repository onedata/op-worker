/**
 * @file rt_map_nif.cc
 * @author Krzysztof Trzepla
 * @copyright (C): 2014 ACK CYFRONET AGH
 * This software is released under the MIT license
 * cited in 'LICENSE.txt'.
 */

#include "../nifpp.h"
#include "rt_map.h"
#include "rt_local_term.h"
#include "rt_exception.h"

#include <string>
#include <functional>
#include <set>

using namespace one::provider;

namespace {

int load(ErlNifEnv *env, void **priv, ERL_NIF_TERM load_info)
{
    std::string module_name;
    nifpp::get(env, load_info, module_name);
    nifpp::register_resource<rt_map>(env, nullptr, module_name.c_str());
    return 0;
}

ERL_NIF_TERM init_nif(ErlNifEnv *env, int argc, const ERL_NIF_TERM argv[])
{
    try {
        auto map = nifpp::construct_resource<rt_map>();

        return nifpp::make(
            env, std::make_tuple(nifpp::str_atom("ok"), nifpp::make(env, map)));
    }
    catch (...) {
        return enif_make_badarg(env);
    }
}

ERL_NIF_TERM put_nif(ErlNifEnv *env, int argc, const ERL_NIF_TERM argv[])
{
    try {
        nifpp::resource_ptr<rt_map> map;
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

        nifpp::get_throws(env, argv[0], map);
        nifpp::get_throws(env, argv[1], record);

        std::set<rt_local_term> rt_local_terms;
        for (const auto &term : terms)
            rt_local_terms.insert(rt_local_term(term));

        rt_block block(file_id, rt_local_term(provider_ref), offset, size,
                       priority, retry, rt_local_terms);
        map->put(block);

        return nifpp::make(env, nifpp::str_atom("ok"));
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

ERL_NIF_TERM get_nif(ErlNifEnv *env, int argc, const ERL_NIF_TERM argv[])
{
    try {
        nifpp::resource_ptr<rt_map> map;
        std::string file_id;
        ErlNifUInt64 offset, size;
        nifpp::get_throws(env, argv[0], map);
        nifpp::get_throws(env, argv[1], file_id);
        nifpp::get_throws(env, argv[2], offset);
        nifpp::get_throws(env, argv[3], size);

        std::list<std::tuple<nifpp::str_atom, nifpp::TERM, nifpp::TERM,
                             ErlNifUInt64, ErlNifUInt64, ErlNifUInt64, int,
                             std::list<nifpp::TERM>>> records;

        for (const auto &block : map->get(file_id, offset, size)) {
            std::list<nifpp::TERM> terms;
            for (const auto &term : block.terms())
                terms.push_back(term.get(env));

            nifpp::binary fileId{block.file_id().size()};
            std::copy(block.file_id().begin(), block.file_id().end(),
                      fileId.data);

            records.push_back(std::make_tuple(
                nifpp::str_atom("rt_block"), nifpp::make(env, fileId),
                block.provider_ref().get(env), block.offset(), block.size(),
                block.priority(), block.retry(), terms));
        }

        return nifpp::make(env,
                           std::make_tuple(nifpp::str_atom("ok"), records));
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

ERL_NIF_TERM remove_nif(ErlNifEnv *env, int argc, const ERL_NIF_TERM argv[])
{
    try {
        nifpp::resource_ptr<rt_map> map;
        std::string file_id;
        ErlNifUInt64 offset, size;
        nifpp::get_throws(env, argv[0], map);
        nifpp::get_throws(env, argv[1], file_id);
        nifpp::get_throws(env, argv[2], offset);
        nifpp::get_throws(env, argv[3], size);

        map->remove(file_id, offset, size);

        return nifpp::make(env, nifpp::str_atom("ok"));
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

ErlNifFunc nif_funcs[] = {{"init_nif", 0, init_nif},
                          {"put_nif", 2, put_nif},
                          {"get_nif", 4, get_nif},
                          {"remove_nif", 4, remove_nif}};

ERL_NIF_INIT(rt_map, nif_funcs, load, NULL, NULL, NULL)
}
