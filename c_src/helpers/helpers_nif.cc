#define BOOST_THREAD_PROVIDES_FUTURE_CONTINUATION
#define BOOST_THREAD_PROVIDES_EXECUTORS

#include "helpers/storageHelperFactory.h"

#include <boost/thread/executors/basic_thread_pool.hpp>

#include <string>
#include <memory>
#include <vector>
#include <random>
#include <tuple>
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


class io_service_executor
  {

  public:
    BOOST_THREAD_NO_COPYABLE(io_service_executor)

    /**
     * Effects: creates a thread pool that runs closures on @c thread_count threads.
     */
    io_service_executor(asio::io_service &io_service)
        : _io_service(io_service)
    {};
    /**
     * Effects: Destroys the thread pool.
     * Synchronization: The completion of all the closures happen before the completion of the thread pool destructor.
     */
    ~io_service_executor() {};

    /**
     * Effects: close the thread_pool for submissions. The worker threads will work until
     */
    void close() { };

    /**
     * Returns: whether the pool is closed for submissions.
     */
    bool is_close() { return false; }

    /**
     * Effects: The specified function will be scheduled for execution at some point in the future.
     * If invoking closure throws an exception the thread pool will call std::terminate, as is the case with threads.
     * Synchronization: completion of closure on a particular thread happens before destruction of thread's thread local variables.
     * Throws: sync_queue_is_closed if the thread pool is closed.
     *
     */
    template <typename T>
    void submit(T closure) {
        _io_service.post(closure);
    }

    /**
     * This must be called from an scheduled task.
     * Effects: reschedule functions until pred()
     */
    template <typename Pred>
    void reschedule_until(Pred const& pred) {

    }

    private:
        asio::io_service &_io_service;
  };


using helper_ptr = std::shared_ptr<one::helpers::IStorageHelper>;
using handle_t = std::tuple<one::helpers::StorageHelperCTX*, fuse_file_info*>;
using reqid_t = std::tuple<int, int, int>;



std::shared_ptr<one::communication::Communicator> nullCommunicator = nullptr;
one::helpers::BufferLimits limits = one::helpers::BufferLimits();
asio::io_service dio_service;
asio::io_service cproxy_service;
asio::io_service::work dio_work(dio_service);
asio::io_service::work cproxy_work(cproxy_service);

std::vector<std::thread> workers;

boost::basic_thread_pool executor(10);
io_service_executor io_executor(dio_service);

one::helpers::StorageHelperFactory SHFactory = one::helpers::StorageHelperFactory(nullCommunicator, limits, dio_service, cproxy_service);

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




template <typename... Args, std::size_t... I>
ERL_NIF_TERM wrap_helper(
    ERL_NIF_TERM (*fun)(ErlNifEnv *, Env, ErlNifPid, reqid_t, Args...), ErlNifEnv *env,
    const ERL_NIF_TERM args[], std::index_sequence<I...>)
{
    try {
        ErlNifPid pid;
        enif_self(env, &pid);

        std::random_device rd;
        std::mt19937 gen(rd());

        auto reqId = std::make_tuple(std::rand(), std::rand(), std::rand());
        return fun(env, Env{}, pid, reqId, nifpp::get<Args>(env, args[I])...);
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

template <typename... Args>
ERL_NIF_TERM wrap(ERL_NIF_TERM (*fun)(ErlNifEnv *, Env, ErlNifPid, reqid_t, Args...),
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


void handle_result(Env env, ErlNifPid pid, reqid_t reqId, one::helpers::future_t<void> &f)
{
    try {
        f.get();
        enif_send(env, &pid, env, nifpp::make(env, std::make_tuple(reqId, ok)));
    } catch(std::system_error &e) {
        enif_send(env, &pid, env, nifpp::make(env, std::make_tuple(reqId, std::make_tuple(error, nifpp::str_atom{e.code().message()}))));
    }
}

void async_handle_result(Env localEnv, ErlNifPid pid, reqid_t reqId, one::helpers::future_t<void> &future)
{
    io_executor.submit([&future, localEnv, pid, reqId]() { handle_result(localEnv, pid, reqId, future); });
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
    auto ffi_resource = nifpp::construct_resource<fuse_file_info>();
    auto ctx_resource = nifpp::construct_resource<one::helpers::StorageHelperCTX>(*ffi_resource);
    return nifpp::make(env, std::make_tuple(ok, std::make_tuple(ctx_resource, ffi_resource)));
}




static ERL_NIF_TERM set_user_ctx(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[])
{
    one::helpers::StorageHelperCTX* ctx;
    std::tie(ctx, std::ignore) = nifpp::get<handle_t>(env, argv[0]);

    auto uidTerm = argv[1];
    auto gidTerm = argv[2];

    if(!nifpp::get(env, uidTerm, ctx->uid)) {
        ctx->uid = uNameToUID(nifpp::get<string>(env, uidTerm));
    }

    if(!nifpp::get(env, gidTerm, ctx->gid)) {
        ctx->gid = gNameToGID(nifpp::get<string>(env, gidTerm));
    }

    return nifpp::make(env, ok);
}


static ERL_NIF_TERM get_user_ctx(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[])
{
    one::helpers::StorageHelperCTX* ctx;
    std::tie(ctx, std::ignore) = nifpp::get<handle_t>(env, argv[0]);
    return make(env, std::make_tuple(ok, std::make_tuple(ctx->uid, ctx->gid)));
}

ERL_NIF_TERM mkdir(ErlNifEnv *env, Env localEnv, ErlNifPid pid, reqid_t reqId, helper_ptr helper, handle_t ctx, std::string file, uint32_t mode)
{
    auto future = helper->ash_mkdir(file, mode);
    async_handle_result(localEnv, pid, reqId, future);
    return make(env, std::make_tuple(ok, reqId));
}

/*********************************************************************
*
*                          WRAPPERS (NIF based)
*       All functions below are described in helpers_nif.erl
*
*********************************************************************/
extern "C" {

//static ERL_NIF_TERM sh_link(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[])
//{
//    UserCTX(env, argv[1], argv[2]);
//    auto sh = get_helper(env, argv[0]);
//    auto pid = get<ErlNifPid>(env, argv[3]);

//    auto future = sh->ash_link(get<string>(env, argv[4]).c_str(), get<string>(env, argv[5]).c_str());
//    future.then([=](one::helpers::future_t<void> f) {
//        handle_result(env, pid, f);
//    });

//    return make(env, str_atom("ok"));
//}

//static ERL_NIF_TERM sh_getattr(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[])
//{
//    auto sh = get_helper(env, argv[0]);
//
//    struct stat st;
//    int ret = sh->sh_getattr(auto sh = (env, argv[4]).c_str(), &st);
//
//    return enif_make_tuple2(env, enif_make_int(env, ret), make_stat(env, st));
//}
//
//static ERL_NIF_TERM sh_access(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[])
//{
//    auto sh = get_helper(env, argv[0]);
//
//    return make(env, sh->sh_access(get<string>(env, argv[4]).c_str(), get<int>(env, argv[5])));
//}
//
//static ERL_NIF_TERM sh_mknod(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[])
//{
//    auto sh = get_helper(env, argv[0]);
//
//    return make(env, sh->sh_mknod(get<string>(env, argv[4]).c_str(), get<int>(env, argv[5]), get<int>(env, argv[6])));
//}
//
//static ERL_NIF_TERM sh_unlink(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[])
//{
//    auto sh = get_helper(env, argv[0]);
//
//    return make(env, sh->sh_unlink(auto sh = (env, argv[4]).c_str()));
//}
//
//static ERL_NIF_TERM sh_rename(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[])
//{
//    auto sh = get_helper(env, argv[0]);
//
//    return make(env, sh->sh_rename(get<string>(env, argv[4]).c_str(), auto sh = (env, argv[5]).c_str()));
//}
//
//static ERL_NIF_TERM sh_chmod(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[])
//{
//    auto sh = get_helper(env, argv[0]);
//
//    return make(env, sh->sh_chmod(get<string>(env, argv[4]).c_str(), get<int>(env, argv[5])));
//}
//
//static ERL_NIF_TERM sh_chown(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[])
//{
//    auto sh = get_helper(env, argv[0]);
//
//    return make(env, sh->sh_chown(get<string>(env, argv[4]).c_str(), get<int>(env, argv[5]), get<int>(env, argv[6])));
//}
//
//static ERL_NIF_TERM sh_chown_name(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[])
//{
//    auto sh = get_helper(env, argv[0]);
//
//    uid_t uid = -1;
//    gid_t gid = -1;
////
////    struct passwd *ownerInfo = getpwnam(aget<string>(env, argv[5]).c_str()); // Static buffer, do NOT free !
////    struct group  *groupInfo = getgrnam(auto sh = (env, argv[6]).c_str()); // Static buffer, do NOT free !
////
////    if(!ownerInfo && auto sh = (env, argv[5]).size() > 0) // User not found
////        return enif_make_int(env, -EINVAL);
////    if(!groupInfo && auto sh = (env, argv[6]).size() > 0) // Group not found
////        return enif_make_int(env, -EINVAL);
////
////    uid   = (ownerInfo ? ownerInfo->pw_uid : -1);
////    gid   = (groupInfo ? groupInfo->gr_gid : -1);
//
//    return make(env, sh->sh_chown(get<string>(env, argv[4]).c_str(), uid, gid));
//}
//
//static ERL_NIF_TERM sh_truncate(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[])
//{
//    auto sh = get_helper(env, argv[0]);
//
//    return make(env, sh->sh_truncate(get<string>(env, argv[4]).c_str(), get<int>(env, argv[5])));
//}
//
//static ERL_NIF_TERM sh_open(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[])
//{
//    auto sh = get_helper(env, argv[0]);
//
//    struct fuse_file_info ffi = get<ffi>(env, argv[5]);
//    ffi.flags |= O_NOFOLLOW;
//    int ret = sh->sh_open(auto sh = (env, argv[4]).c_str(), &ffi);
//
//    return enif_make_tuple2(env, enif_make_int(env, ret), make_ffi(env, ffi));
//}
//
//static ERL_NIF_TERM sh_read(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[])
//{
//    auto sh = get_helper(env, argv[0]);
//
//    struct fuse_file_info ffi = get<ffi>(env, argv[7]);
//    unsigned int size = get<int>(env, argv[5]);
//    ERL_NIF_TERM bin;
//    char *buff = new char[size];
//
//    int ret = sh->sh_read(auto sh = (env, argv[4]).c_str(), buff, size, get<int>(env, argv[6]), &ffi);
//    char *tmp = (char *) enif_make_new_binary(env, (ret > 0 ? ret : 0), &bin);
//    memcpy(tmp, buff, (ret > 0 ? ret : 0));
//
//    delete[] buff;
//
//    return enif_make_tuple2(env, enif_make_int(env, ret), bin);
//}
//
//static ERL_NIF_TERM sh_write(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[])
//{
//    auto sh = get_helper(env, argv[0]);
//
//    struct fuse_file_info ffi = get<ffi>(env, argv[7]);
//    ErlNifBinary bin;
//    if(!enif_inspect_binary(env, argv[5], &bin))
//        return BADARG;
//
//    return make(env, sh->sh_write(get<string>(env, argv[4]).c_str(), (const char*)bin.data, bin.size, get<int>(env, argv[6]), &ffi));
//}
//
//static ERL_NIF_TERM sh_statfs(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[])
//{
//    auto sh = get_helper(env, argv[0]);
//
//    struct statvfs stat;
//    int ret = sh->sh_statfs(auto sh = (env, argv[4]).c_str(), &stat);
//
//    return enif_make_tuple2(env, enif_make_int(env, ret), make_statvfs(env, stat));
//}
//
//static ERL_NIF_TERM sh_release(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[])
//{
//    auto sh = get_helper(env, argv[0]);
//
//    struct fuse_file_info ffi = get<ffi>(env, argv[5]);
//    return make(env, sh->sh_release(get<string>(env, argv[4]).c_str(), &ffi));
//}
//
//static ERL_NIF_TERM sh_fsync(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[])
//{
//    auto sh = get_helper(env, argv[0]);
//
//    struct fuse_file_info ffi = get<ffi>(env, argv[6]);
//
//    return make(env, sh->sh_fsync(get<string>(env, argv[4]).c_str(), get<int>(env, argv[5]), &ffi));
//}

static ERL_NIF_TERM sh_mkdir(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[])
{
    return wrap(mkdir, env, argv);
}

static int load(ErlNifEnv* env, void** priv_data, ERL_NIF_TERM load_info)
{
    for(auto i = 0; i < 20; ++i) {
        workers.push_back(std::thread([]() { dio_service.run(); }));
    }

    return !(nifpp::register_resource<helper_ptr>(env, nullptr, "helper_ptr") &&
             nifpp::register_resource<one::helpers::StorageHelperCTX>(env, nullptr, "StorageHelperCTX") &&
             nifpp::register_resource<fuse_file_info>(env, nullptr, "fuse_file_info"));
}


static ErlNifFunc nif_funcs[] =
{
//    {"link",        6, sh_link},
//    {"getattr",     5, sh_getattr},
//    {"access",      6, sh_access},
//    {"mknod",       7, sh_mknod},
//    {"unlink",      5, sh_unlink},
//    {"rename",      6, sh_rename},
//    {"chmod",       6, sh_chmod},
//    {"chown",       7, sh_chown},
//    {"chown_name",  7, sh_chown_name},
//    {"truncate",    6, sh_truncate},
//    {"open",        6, sh_open},
//    {"read",        8, sh_read},
//    {"write",       8, sh_write},
//    {"statfs",      5, sh_statfs},
//    {"release",     6, sh_release},
//    {"fsync",       7, sh_fsync},
//    {"rmdir",       5, sh_rmdir},
    {"mkdir",           4, sh_mkdir},
    {"new_helper_obj",  2, new_helper_obj},
    {"new_helper_ctx",  0, new_helper_ctx},
    {"set_user_ctx",    3, set_user_ctx},
    {"get_user_ctx",    1, get_user_ctx}
};


ERL_NIF_INIT(helpers_nif, nif_funcs, load, NULL, NULL, NULL);

} // extern C
