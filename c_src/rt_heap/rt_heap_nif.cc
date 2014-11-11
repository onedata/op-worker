/*********************************************************************
 * @author Krzysztof Trzepla
 * @copyright (C): 2014 ACK CYFRONET AGH
 * This software is released under the MIT license
 * cited in 'LICENSE.txt'.
*********************************************************************/

#include <functional>
#include <string>
#include <vector>

#include "nifpp.h"
#include "rt_heap.h"

using namespace one::provider;

extern "C" {

static int load(ErlNifEnv* env, void** priv, ERL_NIF_TERM load_info)
{
    nifpp::register_resource< rt_heap >(env, nullptr, "rt_heap");
    return 0;
}

static ERL_NIF_TERM init_nif(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[])
{
    try
    {
        long int block_size;
        nifpp::get_throws(env, argv[0], block_size);
        auto heap = nifpp::construct_resource< rt_heap >(block_size);
        return nifpp::make(env, std::make_tuple(nifpp::str_atom("ok"),
                                                nifpp::make(env, std::move(heap))));
    }
    catch(...) {}
    return enif_make_badarg(env);
}

static ERL_NIF_TERM push_nif(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[])
{
    try
    {
        nifpp::resource_ptr< rt_heap > heap;
        nifpp::str_atom record_name;
        std::string file_id;
        long int offset, size, priority;
        auto record = std::make_tuple(std::ref(record_name),
                                      std::ref(file_id),
                                      std::ref(offset),
                                      std::ref(size),
                                      std::ref(priority));

        nifpp::get_throws(env, argv[0], heap);
        nifpp::get_throws(env, argv[1], record);

        rt_block block(file_id, offset, size, priority);
        heap->push(block);

        return nifpp::make(env, nifpp::str_atom("ok"));
    }
    catch (const std::runtime_error& error)
    {
        std::string message = error.what();
        return nifpp::make(env, std::make_tuple(nifpp::str_atom("error"),
                                                message));
    }
    catch(...) {}
    return enif_make_badarg(env);
}

static ERL_NIF_TERM fetch_nif(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[])
{
    try
    {
        nifpp::resource_ptr< rt_heap > heap;
        nifpp::get_throws(env, argv[0], heap);

        rt_block block = heap->fetch();
        auto record = std::make_tuple(nifpp::str_atom("rt_block"),
                                      block.file_id(),
                                      block.offset(),
                                      block.size(),
                                      block.priority());

        return nifpp::make(env, std::make_tuple(nifpp::str_atom("ok"),
                                                record));
    }
    catch (const std::runtime_error& error)
    {
        std::string message = error.what();
        return nifpp::make(env, std::make_tuple(nifpp::str_atom("error"),
                                                message));
    }
    catch(...) {}
    return enif_make_badarg(env);
}

static ErlNifFunc nif_funcs[] = {
    {"init_nif", 1, init_nif},
    {"push_nif", 2, push_nif},
    {"fetch_nif", 1, fetch_nif}
};

ERL_NIF_INIT(rt_heap, nif_funcs, load, NULL, NULL, NULL)

} //extern C