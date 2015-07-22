#define BOOST_THREAD_PROVIDES_FUTURE_CONTINUATION
#define BOOST_THREAD_PROVIDES_EXECUTORS

#include "helpers/storageHelperFactory.h"

#include <boost/thread/executors/basic_thread_pool.hpp>

#include <string>
#include <memory>
#include <vector>
#include <random>
#include <tuple>
#include <map>
#include "nifpp.h"

#include <pwd.h>
#include <grp.h>



#ifndef __APPLE__
#include <sys/fsuid.h>
#endif

namespace {
/**
 * @defgroup StaticAtoms Statically created atoms for ease of usage.
 * @{
 */
nifpp::str_atom ok{"ok"};
nifpp::str_atom error{"error"};
/** @} */

using helper_ptr = std::shared_ptr<one::helpers::IStorageHelper>;
using helper_ctx_ptr = std::shared_ptr<one::helpers::StorageHelperCTX>;
using reqid_t = std::tuple<int, int, int>;



std::shared_ptr<one::communication::Communicator> nullCommunicator = nullptr;
one::helpers::BufferLimits limits = one::helpers::BufferLimits();
asio::io_service dio_service;
asio::io_service cproxy_service;
asio::io_service callback_service;
asio::io_service::work dio_work(dio_service);
asio::io_service::work cproxy_work(cproxy_service);
asio::io_service::work callback_work(callback_service);

std::vector<std::thread> workers;

one::helpers::StorageHelperFactory SHFactory = one::helpers::StorageHelperFactory(nullCommunicator, limits, dio_service, cproxy_service);

std::map<nifpp::str_atom, int> atom_to_flag = {
    {"O_RDONLY", O_RDONLY},
    {"O_WRONLY", O_WRONLY},
    {"O_RDWR", O_RDWR}
};
std::map<int, nifpp::str_atom> flag_to_atom;

}

using std::string;
//using namespace nifpp;
//
one::helpers::IStorageHelper::ArgsMap get_args(ErlNifEnv* env, ERL_NIF_TERM term)
{
    one::helpers::IStorageHelper::ArgsMap args;

    if(enif_is_list(env, term) && !enif_is_empty_list(env, term))
    {
        int i = 0;
        ERL_NIF_TERM list, head, tail;
        for(list = term; enif_get_list_cell(env, list, &head, &tail); list = tail)
            args.emplace(one::helpers::srvArg(i), nifpp::get<string>(env, head));
    }

    return std::move(args);
}

/**
 * A shared pointer wrapper to help with Erlang NIF environment management.
 */
class Env {
public:
    /**
     * Creates a new environment by creating a @c shared_ptr with a custom
     * deleter.
     */
    Env()
        : env{enif_alloc_env(), enif_free_env}
    {
    }

    /**
     * Implicit conversion operator to @cErlNifEnv* .
     */
    operator ErlNifEnv *() { return env.get(); }

    ErlNifEnv *get() { return env.get(); }

private:
    std::shared_ptr<ErlNifEnv> env;
};


struct NifCTX {
    NifCTX(ErlNifEnv *env, Env localEnv, ErlNifPid pid, reqid_t reqId, helper_ptr helper_obj, helper_ctx_ptr helper_ctx)
        : env(env)
        , localEnv(localEnv)
        , reqPid(pid)
        , reqId(reqId)
        , helper_obj(helper_obj)
        , helper_ctx(helper_ctx)
    {
    }

    ErlNifEnv *env;
    Env localEnv;
    ErlNifPid reqPid;
    reqid_t reqId;
    helper_ptr helper_obj;
    helper_ctx_ptr helper_ctx;

};

template <class T>
ERL_NIF_TERM handle_errors(ErlNifEnv *env, T fun)
{
    try {
        return fun();
    }
    catch (const nifpp::badarg &) {
        return enif_make_badarg(env);
    }
    catch (const std::system_error &e) {
        return nifpp::make(
            env, std::make_tuple(error, nifpp::str_atom{e.code().message()}));
    }
    catch (const std::exception &e) {
        return nifpp::make(env, std::make_tuple(error, std::string{e.what()}));
    }
}

template <typename... Args, std::size_t... I>
ERL_NIF_TERM wrap_helper(
    ERL_NIF_TERM (*fun)(NifCTX ctx, Args...), ErlNifEnv *env,
    const ERL_NIF_TERM args[], std::index_sequence<I...>)
{
    return handle_errors(env, [&]() {
        ErlNifPid pid;
        enif_self(env, &pid);

        std::random_device rd;
        std::mt19937 gen(rd());


        auto reqId = std::make_tuple(std::rand(), std::rand(), std::rand());
        return fun(NifCTX(env, Env(), pid, reqId, nifpp::get<helper_ptr>(env, args[0]), nifpp::get<helper_ctx_ptr>(env, args[1])), nifpp::get<Args>(env, args[2 + I])...);
    });
}

template <typename... Args>
ERL_NIF_TERM wrap(ERL_NIF_TERM (*fun)(NifCTX, Args...),
    ErlNifEnv *env, const ERL_NIF_TERM args[])
{
    return wrap_helper(fun, env, args, std::index_sequence_for<Args...>{});
}


uid_t uNameToUID(std::string uname)
{
    struct passwd *ownerInfo = getpwnam(uname.c_str()); // Static buffer, do NOT free !
    return (ownerInfo ? ownerInfo->pw_uid : -1);
}

gid_t gNameToGID(std::string gname, std::string uname = "")
{
    struct passwd *ownerInfo = getpwnam(uname.c_str()); // Static buffer, do NOT free !
    struct group  *groupInfo = getgrnam(gname.c_str()); // Static buffer, do NOT free !

    gid_t primary_gid = (ownerInfo ? ownerInfo->pw_gid : -1);
    return (groupInfo ? groupInfo->gr_gid : primary_gid);
}


void handle_future(const NifCTX &ctx, std::shared_ptr<one::helpers::future_t<void>> f)
{
    f->get();
    enif_send(ctx.env, &ctx.reqPid, ctx.env, nifpp::make(ctx.env, std::make_tuple(ctx.reqId, ok)));
}

void handle_future(const NifCTX &ctx, std::shared_ptr<one::helpers::future_t<asio::mutable_buffer>> f)
{
    auto buffer = f->get();
    nifpp::binary bin(asio::buffer_size(buffer));
    memcpy(bin.data, asio::buffer_cast<const char*>(buffer), asio::buffer_size(buffer));
    enif_send(ctx.env, &ctx.reqPid, ctx.env, nifpp::make(ctx.env, std::make_tuple(ctx.reqId, std::make_tuple(ok, nifpp::make(ctx.env, bin)))));
}

void handle_future(const NifCTX &ctx, std::shared_ptr<one::helpers::future_t<struct stat>> f)
{
    auto s = f->get();
    auto record = std::make_tuple(nifpp::str_atom("statbuf"),
                      s.st_dev,
                      s.st_ino,
                      s.st_mode,
                      s.st_nlink,
                      s.st_uid,
                      s.st_gid,
                      s.st_rdev,
                      s.st_size,
                      s.st_atime,
                      s.st_mtime,
                      s.st_ctime,
                      s.st_blksize,
                      s.st_blocks);
    enif_send(ctx.env, &ctx.reqPid, ctx.env, nifpp::make(ctx.env, std::make_tuple(ctx.reqId, std::make_tuple(ok, record))));
}


template<class T>
void handle_future(const NifCTX &ctx, std::shared_ptr<one::helpers::future_t<T>> f)
{
    auto response = f->get();
    enif_send(ctx.env, &ctx.reqPid, ctx.env, nifpp::make(ctx.env, std::make_tuple(ctx.reqId, std::make_tuple(ok, response))));
}

template<class T>
void handle_result(const NifCTX ctx, std::shared_ptr<one::helpers::future_t<T>> f)
{
    try {
        handle_future(ctx, f);
    } catch(std::system_error &e) {
        enif_send(ctx.env, &ctx.reqPid, ctx.env, nifpp::make(ctx.env, std::make_tuple(ctx.reqId, std::make_tuple(error, nifpp::str_atom{e.code().message()}))));
    }
}

template<class T, class H>
void async_handle_result(const NifCTX &ctx, std::shared_ptr<one::helpers::future_t<T>> future, H &res_holder)
{
    callback_service.post([future, ctx, res_holder]() { handle_result(ctx, future); });
}

template<class T, class H = int>
void async_handle_result(const NifCTX &ctx, std::shared_ptr<one::helpers::future_t<T>> future)
{
    callback_service.post([future, ctx]() { handle_result(ctx, future); });
}

static ERL_NIF_TERM new_helper_obj(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[])
{
    auto helperName = nifpp::get<string>(env, argv[0]);
    auto helperArgs = get_args(env, argv[1]);
    auto helperObj = SHFactory.getStorageHelper(helperName, helperArgs);
    if(!helperObj) {
        return make(env, std::make_tuple(error, nifpp::str_atom("invalid_helper")));
    } else {
        auto resource =
                    nifpp::construct_resource<helper_ptr>(helperObj);

        return make(env, std::make_tuple(ok, resource));
    }

}

static ERL_NIF_TERM new_helper_ctx(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[])
{
    auto ctx = std::make_shared<one::helpers::StorageHelperCTX>();
    auto ctx_resource = nifpp::construct_resource<helper_ctx_ptr>(ctx);
    return nifpp::make(env, std::make_tuple(ok, ctx_resource));
}

static ERL_NIF_TERM username_to_uid(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[])
{
     auto uidTerm = argv[0];
     return nifpp::make(env, std::make_tuple(ok, uNameToUID(nifpp::get<string>(env, uidTerm))));
}

static ERL_NIF_TERM groupname_to_gid(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[])
{
     auto gidTerm = argv[0];
     return nifpp::make(env, std::make_tuple(ok, gNameToGID(nifpp::get<string>(env, gidTerm))));
}

static ERL_NIF_TERM set_user_ctx(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[])
{
    return handle_errors(env, [&]() {
        auto ctx = nifpp::get<helper_ctx_ptr>(env, argv[0]);


        auto uidTerm = argv[1];
        auto gidTerm = argv[2];

        if(!nifpp::get(env, uidTerm, ctx->uid)) {
            ctx->uid = uNameToUID(nifpp::get<string>(env, uidTerm));
        }

        if(!nifpp::get(env, gidTerm, ctx->gid)) {
            ctx->gid = gNameToGID(nifpp::get<string>(env, gidTerm));
        }

        return nifpp::make(env, ok);
    });
}


static ERL_NIF_TERM get_user_ctx(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[])
{
    return handle_errors(env, [&]() {
        auto ctx = nifpp::get<helper_ctx_ptr>(env, argv[0]);
        return make(env, std::make_tuple(ok, std::make_tuple(ctx->uid, ctx->gid)));
    });
}

static ERL_NIF_TERM get_flags(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[])
{
    return handle_errors(env, [&]() {
        auto ctx = nifpp::get<helper_ctx_ptr>(env, argv[0]);
        std::vector<nifpp::str_atom> flags;
        for(auto &flag : atom_to_flag) {
            if(ctx->m_ffi.flags & flag.second) {
                flags.push_back(flag.first);
            }
        }

        return nifpp::make(env, std::make_tuple(ok, flags));
    });
}

static ERL_NIF_TERM set_flags(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[])
{
    return handle_errors(env, [&]() {
        auto ctx = nifpp::get<helper_ctx_ptr>(env, argv[0]);
        ctx->m_ffi.flags = 0;
        nifpp::list_for_each<nifpp::str_atom>(env, argv[1], [&ctx](nifpp::str_atom atom) {
            auto it = atom_to_flag.find(atom);
            if(it == atom_to_flag.end()) {
                throw nifpp::badarg();
            } else {
                ctx->m_ffi.flags |= it->second;
            }
        });

        return nifpp::make(env, ok);
    });
}

static ERL_NIF_TERM getattr(NifCTX ctx, const std::string file)
{
    auto future = std::make_shared<one::helpers::future_t<struct stat>>();
    *future = ctx.helper_obj->ash_getattr(file);
    async_handle_result(ctx, future);
    return nifpp::make(ctx.env, std::make_tuple(ok, ctx.reqId));
}

static ERL_NIF_TERM access(NifCTX ctx, const std::string file, const int mask)
{
    auto future = std::make_shared<one::helpers::future_t<void>>();
    *future = ctx.helper_obj->ash_access(file, mask);
    async_handle_result(ctx, future);
    return nifpp::make(ctx.env, std::make_tuple(ok, ctx.reqId));
}

static ERL_NIF_TERM mknod(NifCTX ctx, const std::string file, const mode_t mode, const dev_t dev)
{
    auto future = std::make_shared<one::helpers::future_t<void>>();
    *future = ctx.helper_obj->ash_mknod(file, mode, dev);
    async_handle_result(ctx, future);
    return nifpp::make(ctx.env, std::make_tuple(ok, ctx.reqId));
}

static ERL_NIF_TERM mkdir(NifCTX ctx, const std::string file, const mode_t mode)
{
    auto future = std::make_shared<one::helpers::future_t<void>>();
    *future = ctx.helper_obj->ash_mkdir(file, mode);
    async_handle_result(ctx, future);
    return nifpp::make(ctx.env, std::make_tuple(ok, ctx.reqId));
}

static ERL_NIF_TERM unlink(NifCTX ctx, const std::string file)
{
    auto future = std::make_shared<one::helpers::future_t<void>>();
    *future = ctx.helper_obj->ash_unlink(file);
    async_handle_result(ctx, future);
    return nifpp::make(ctx.env, std::make_tuple(ok, ctx.reqId));
}

static ERL_NIF_TERM rmdir(NifCTX ctx, const std::string file, const mode_t mode)
{
    auto future = std::make_shared<one::helpers::future_t<void>>();
    *future = ctx.helper_obj->ash_rmdir(file);
    async_handle_result(ctx, future);
    return nifpp::make(ctx.env, std::make_tuple(ok, ctx.reqId));
}

static ERL_NIF_TERM symlink(NifCTX ctx, const std::string from, const std::string to)
{
    auto future = std::make_shared<one::helpers::future_t<void>>();
    *future = ctx.helper_obj->ash_symlink(from, to);
    async_handle_result(ctx, future);
    return nifpp::make(ctx.env, std::make_tuple(ok, ctx.reqId));
}

static ERL_NIF_TERM rename(NifCTX ctx, const std::string from, const std::string to)
{
    auto future = std::make_shared<one::helpers::future_t<void>>();
    *future = ctx.helper_obj->ash_rename(from, to);
    async_handle_result(ctx, future);
    return nifpp::make(ctx.env, std::make_tuple(ok, ctx.reqId));
}

static ERL_NIF_TERM link(NifCTX ctx, const std::string from, const std::string to)
{
    auto future = std::make_shared<one::helpers::future_t<void>>();
    *future = ctx.helper_obj->ash_link(from, to);
    async_handle_result(ctx, future);
    return nifpp::make(ctx.env, std::make_tuple(ok, ctx.reqId));
}

static ERL_NIF_TERM chmod(NifCTX ctx, const std::string file, const mode_t mode)
{
    auto future = std::make_shared<one::helpers::future_t<void>>();
    *future = ctx.helper_obj->ash_chmod(file, mode);
    async_handle_result(ctx, future);
    return nifpp::make(ctx.env, std::make_tuple(ok, ctx.reqId));
}

static ERL_NIF_TERM chown(NifCTX ctx, const std::string file, const uid_t uid, const gid_t gid)
{
    auto future = std::make_shared<one::helpers::future_t<void>>();
    *future = ctx.helper_obj->ash_chown(file, uid, gid);
    async_handle_result(ctx, future);
    return nifpp::make(ctx.env, std::make_tuple(ok, ctx.reqId));
}

static ERL_NIF_TERM truncate(NifCTX ctx, const std::string file, const off_t size)
{
    auto future = std::make_shared<one::helpers::future_t<void>>();
    *future = ctx.helper_obj->ash_truncate(file, size);
    async_handle_result(ctx, future);
    return nifpp::make(ctx.env, std::make_tuple(ok, ctx.reqId));
}

static ERL_NIF_TERM open(NifCTX ctx, const std::string file)
{
    auto future = std::make_shared<one::helpers::future_t<int>>();
    *future = ctx.helper_obj->ash_open(file, *ctx.helper_ctx);
    async_handle_result(ctx, future);
    return nifpp::make(ctx.env, std::make_tuple(ok, ctx.reqId));
}

static ERL_NIF_TERM read(NifCTX ctx, const std::string file, off_t offset, size_t size)
{
    auto buf = std::make_shared<std::vector<char>>(size);
    auto future = std::make_shared<one::helpers::future_t<asio::mutable_buffer>>();
    *future = ctx.helper_obj->ash_read(file, asio::mutable_buffer(buf->data(), size), offset, *ctx.helper_ctx);
    async_handle_result(ctx, future, buf);
    return nifpp::make(ctx.env, std::make_tuple(ok, ctx.reqId));
}

static ERL_NIF_TERM write(NifCTX ctx, const std::string file, const off_t offset, std::string data)
{
    auto future = std::make_shared<one::helpers::future_t<int>>();
    *future = ctx.helper_obj->ash_write(file, asio::const_buffer(data.data(), data.size()), offset, *ctx.helper_ctx);
    async_handle_result(ctx, future, data);
    return nifpp::make(ctx.env, std::make_tuple(ok, ctx.reqId));
}

static ERL_NIF_TERM release(NifCTX ctx, const std::string file)
{
    auto future = std::make_shared<one::helpers::future_t<void>>();
    *future = ctx.helper_obj->ash_release(file, *ctx.helper_ctx);
    async_handle_result(ctx, future);
    return nifpp::make(ctx.env, std::make_tuple(ok, ctx.reqId));
}

static ERL_NIF_TERM flush(NifCTX ctx, const std::string file)
{
    auto future = std::make_shared<one::helpers::future_t<void>>();
    *future = ctx.helper_obj->ash_flush(file, *ctx.helper_ctx);
    async_handle_result(ctx, future);
    return nifpp::make(ctx.env, std::make_tuple(ok, ctx.reqId));
}

static ERL_NIF_TERM fsync(NifCTX ctx, const std::string file, const int isdatasync)
{
    auto future = std::make_shared<one::helpers::future_t<void>>();
    *future = ctx.helper_obj->ash_fsync(file, isdatasync, *ctx.helper_ctx);
    async_handle_result(ctx, future);
    return nifpp::make(ctx.env, std::make_tuple(ok, ctx.reqId));
}

/*********************************************************************
*
*                          WRAPPERS (NIF based)
*       All functions below are described in helpers_nif.erl
*
*********************************************************************/
extern "C" {

static int load(ErlNifEnv* env, void** priv_data, ERL_NIF_TERM load_info)
{
    for(auto i = 0; i < 5; ++i) {
        workers.push_back(std::thread([]() { dio_service.run(); }));
        workers.push_back(std::thread([]() { callback_service.run(); }));
    }

    return !(nifpp::register_resource<helper_ptr>(env, nullptr, "helper_ptr") &&
             nifpp::register_resource<helper_ctx_ptr>(env, nullptr, "helper_ctx") &&
             nifpp::register_resource<fuse_file_info>(env, nullptr, "fuse_file_info"));
}

static void unload(ErlNifEnv* env, void* priv_data)
{
    dio_service.stop();
    callback_service.stop();

    for(auto &th : workers) {
        th.join();
    }
}

static ERL_NIF_TERM sh_getattr(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[])
{
    return wrap(getattr, env, argv);
}

static ERL_NIF_TERM sh_access(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[])
{
    return wrap(access, env, argv);
}

static ERL_NIF_TERM sh_mknod(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[])
{
    return wrap(mknod, env, argv);
}

static ERL_NIF_TERM sh_mkdir(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[])
{
    return wrap(mkdir, env, argv);
}

static ERL_NIF_TERM sh_unlink(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[])
{
    return wrap(unlink, env, argv);
}

static ERL_NIF_TERM sh_rmdir(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[])
{
    return wrap(rmdir, env, argv);
}

static ERL_NIF_TERM sh_symlink(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[])
{
    return wrap(symlink, env, argv);
}

static ERL_NIF_TERM sh_rename(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[])
{
    return wrap(rename, env, argv);
}

static ERL_NIF_TERM sh_link(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[])
{
    return wrap(link, env, argv);
}

static ERL_NIF_TERM sh_chmod(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[])
{
    return wrap(chmod, env, argv);
}

static ERL_NIF_TERM sh_chown(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[])
{
    return wrap(chown, env, argv);
}

static ERL_NIF_TERM sh_truncate(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[])
{
    return wrap(truncate, env, argv);
}

static ERL_NIF_TERM sh_open(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[])
{
    return wrap(open, env, argv);
}

static ERL_NIF_TERM sh_read(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[])
{
    return wrap(read, env, argv);
}

static ERL_NIF_TERM sh_write(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[])
{
    return wrap(write, env, argv);
}

static ERL_NIF_TERM sh_release(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[])
{
    return wrap(release, env, argv);
}

static ERL_NIF_TERM sh_flush(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[])
{
    return wrap(flush, env, argv);
}

static ERL_NIF_TERM sh_fsync(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[])
{
    return wrap(fsync, env, argv);
}


static ErlNifFunc nif_funcs[] =
{
    {"getattr",         3, sh_getattr},
    {"access",          4, sh_access},
    {"mknod",           5, sh_mknod},
    {"mkdir",           4, sh_mkdir},
    {"unlink",          3, sh_unlink},
    {"rmdir",           3, sh_rmdir},
    {"symlink",         4, sh_symlink},
    {"rename",          4, sh_rename},
    {"link",            4, sh_link},
    {"chmod",           4, sh_chmod},
    {"chown",           5, sh_chown},
    {"truncate",        4, sh_truncate},
    {"open",            3, sh_open},
    {"read",            5, sh_read},
    {"write",           5, sh_write},
    {"release",         3, sh_release},
    {"flush",           3, sh_flush},
    {"fsync",           4, sh_fsync},

    {"set_flags",       2, set_flags},
    {"get_flags",       1, get_flags},
    {"username_to_uid", 1, username_to_uid},
    {"groupname_to_gid",1, groupname_to_gid},
    {"new_helper_obj",  2, new_helper_obj},
    {"new_helper_ctx",  0, new_helper_ctx},
    {"set_user_ctx",    3, set_user_ctx},
    {"get_user_ctx",    1, get_user_ctx}
};


ERL_NIF_INIT(helpers_nif, nif_funcs, load, NULL, NULL, unload);

} // extern C
