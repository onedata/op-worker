#define BOOST_THREAD_PROVIDES_FUTURE_CONTINUATION
#define BOOST_THREAD_PROVIDES_EXECUTORS

#include "helpers/storageHelperFactory.h"

#include <boost/thread/executors/basic_thread_pool.hpp>
#include <asio/executor_work.hpp>
#include <asio.hpp>

#include <string>
#include <memory>
#include <vector>
#include <random>
#include <tuple>
#include <map>
#include <system_error>
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

class HelpersNIF {
public:
    std::shared_ptr<one::communication::Communicator> nullCommunicator = nullptr;
    one::helpers::BufferLimits limits = one::helpers::BufferLimits();
    asio::io_service dio_service;
    asio::io_service cproxy_service;
    asio::io_service callback_service;
    asio::executor_work<asio::io_service::executor_type> dio_work = asio::make_work(dio_service);
    asio::executor_work<asio::io_service::executor_type> cproxy_work = asio::make_work(cproxy_service);
    asio::executor_work<asio::io_service::executor_type> callback_work = asio::make_work(callback_service);

    std::vector<std::thread> workers;

    one::helpers::StorageHelperFactory SHFactory = one::helpers::StorageHelperFactory(nullCommunicator, limits, dio_service, cproxy_service);

    HelpersNIF()
    {
    }

    ~HelpersNIF()
    {
        dio_service.stop();
        callback_service.stop();

        for(auto &th : workers) {
            th.join();
        }
    }

} application;

std::map<nifpp::str_atom, int> atom_to_flag = {
    {"O_NONBLOCK", O_NONBLOCK},
    {"O_APPEND",   O_APPEND},
    {"O_ASYNC",    O_ASYNC},
    {"O_FSYNC",    O_FSYNC},
    {"O_NOFOLLOW", O_NOFOLLOW},
    {"O_CREAT",    O_CREAT},
    {"O_TRUNC",    O_TRUNC},
    {"O_EXCL",     O_EXCL}
};

std::map<nifpp::str_atom, int> atom_to_open_mode = {
    {"O_RDONLY",    O_RDONLY},
    {"O_WRONLY",    O_WRONLY},
    {"O_RDWR",      O_RDWR}
};


std::map<std::error_code, nifpp::str_atom> error_to_atom = {
    {std::error_code(static_cast<int>(std::errc::address_family_not_supported), std::system_category()),        nifpp::str_atom("eafnosupport")},
    {std::error_code(static_cast<int>(std::errc::address_in_use), std::system_category()),                      nifpp::str_atom("eaddrinuse")},
    {std::error_code(static_cast<int>(std::errc::address_not_available), std::system_category()),               nifpp::str_atom("eaddrnotavail")},
    {std::error_code(static_cast<int>(std::errc::already_connected), std::system_category()),                   nifpp::str_atom("eisconn")},
    {std::error_code(static_cast<int>(std::errc::argument_list_too_long), std::system_category()),              nifpp::str_atom("e2big")},
    {std::error_code(static_cast<int>(std::errc::argument_out_of_domain), std::system_category()),              nifpp::str_atom("edom")},
    {std::error_code(static_cast<int>(std::errc::bad_address), std::system_category()),                         nifpp::str_atom("efault")},
    {std::error_code(static_cast<int>(std::errc::bad_file_descriptor), std::system_category()),                 nifpp::str_atom("ebadf")},
    {std::error_code(static_cast<int>(std::errc::bad_message), std::system_category()),                         nifpp::str_atom("ebadmsg")},
    {std::error_code(static_cast<int>(std::errc::broken_pipe), std::system_category()),                         nifpp::str_atom("epipe")},
    {std::error_code(static_cast<int>(std::errc::connection_aborted), std::system_category()),                  nifpp::str_atom("econnaborted")},
    {std::error_code(static_cast<int>(std::errc::connection_already_in_progress), std::system_category()),      nifpp::str_atom("ealready")},
    {std::error_code(static_cast<int>(std::errc::connection_refused), std::system_category()),                  nifpp::str_atom("econnrefused")},
    {std::error_code(static_cast<int>(std::errc::connection_reset), std::system_category()),                    nifpp::str_atom("econnreset")},
    {std::error_code(static_cast<int>(std::errc::cross_device_link), std::system_category()),                   nifpp::str_atom("exdev")},
    {std::error_code(static_cast<int>(std::errc::destination_address_required), std::system_category()),        nifpp::str_atom("edestaddrreq")},
    {std::error_code(static_cast<int>(std::errc::device_or_resource_busy), std::system_category()),             nifpp::str_atom("ebusy")},
    {std::error_code(static_cast<int>(std::errc::directory_not_empty), std::system_category()),                 nifpp::str_atom("enotempty")},
    {std::error_code(static_cast<int>(std::errc::executable_format_error), std::system_category()),             nifpp::str_atom("enoexec")},
    {std::error_code(static_cast<int>(std::errc::file_exists), std::system_category()),                         nifpp::str_atom("eexist")},
    {std::error_code(static_cast<int>(std::errc::file_too_large), std::system_category()),                      nifpp::str_atom("efbig")},
    {std::error_code(static_cast<int>(std::errc::filename_too_long), std::system_category()),                   nifpp::str_atom("enametoolong")},
    {std::error_code(static_cast<int>(std::errc::function_not_supported), std::system_category()),              nifpp::str_atom("enosys")},
    {std::error_code(static_cast<int>(std::errc::host_unreachable), std::system_category()),                    nifpp::str_atom("ehostunreach")},
    {std::error_code(static_cast<int>(std::errc::identifier_removed), std::system_category()),                  nifpp::str_atom("eidrm")},
    {std::error_code(static_cast<int>(std::errc::illegal_byte_sequence), std::system_category()),               nifpp::str_atom("eilseq")},
    {std::error_code(static_cast<int>(std::errc::inappropriate_io_control_operation), std::system_category()),  nifpp::str_atom("enotty")},
    {std::error_code(static_cast<int>(std::errc::interrupted), std::system_category()),                         nifpp::str_atom("eintr")},
    {std::error_code(static_cast<int>(std::errc::invalid_argument), std::system_category()),                    nifpp::str_atom("einval")},
    {std::error_code(static_cast<int>(std::errc::invalid_seek), std::system_category()),                        nifpp::str_atom("espipe")},
    {std::error_code(static_cast<int>(std::errc::io_error), std::system_category()),                            nifpp::str_atom("eio")},
    {std::error_code(static_cast<int>(std::errc::is_a_directory), std::system_category()),                      nifpp::str_atom("eisdir")},
    {std::error_code(static_cast<int>(std::errc::message_size), std::system_category()),                        nifpp::str_atom("emsgsize")},
    {std::error_code(static_cast<int>(std::errc::network_down), std::system_category()),                        nifpp::str_atom("enetdown")},
    {std::error_code(static_cast<int>(std::errc::network_reset), std::system_category()),                       nifpp::str_atom("enetreset")},
    {std::error_code(static_cast<int>(std::errc::network_unreachable), std::system_category()),                 nifpp::str_atom("enetunreach")},
    {std::error_code(static_cast<int>(std::errc::no_buffer_space), std::system_category()),                     nifpp::str_atom("enobufs")},
    {std::error_code(static_cast<int>(std::errc::no_child_process), std::system_category()),                    nifpp::str_atom("echild")},
    {std::error_code(static_cast<int>(std::errc::no_link), std::system_category()),                             nifpp::str_atom("enolink")},
    {std::error_code(static_cast<int>(std::errc::no_lock_available), std::system_category()),                   nifpp::str_atom("enolck")},
    {std::error_code(static_cast<int>(std::errc::no_message_available), std::system_category()),                nifpp::str_atom("enodata")},
    {std::error_code(static_cast<int>(std::errc::no_message), std::system_category()),                          nifpp::str_atom("enomsg")},
    {std::error_code(static_cast<int>(std::errc::no_protocol_option), std::system_category()),                  nifpp::str_atom("enoprotoopt")},
    {std::error_code(static_cast<int>(std::errc::no_space_on_device), std::system_category()),                  nifpp::str_atom("enospc")},
    {std::error_code(static_cast<int>(std::errc::no_stream_resources), std::system_category()),                 nifpp::str_atom("enosr")},
    {std::error_code(static_cast<int>(std::errc::no_such_device_or_address), std::system_category()),           nifpp::str_atom("enxio")},
    {std::error_code(static_cast<int>(std::errc::no_such_device), std::system_category()),                      nifpp::str_atom("enodev")},
    {std::error_code(static_cast<int>(std::errc::no_such_file_or_directory), std::system_category()),           nifpp::str_atom("enoent")},
    {std::error_code(static_cast<int>(std::errc::no_such_process), std::system_category()),                     nifpp::str_atom("esrch")},
    {std::error_code(static_cast<int>(std::errc::not_a_directory), std::system_category()),                     nifpp::str_atom("enotdir")},
    {std::error_code(static_cast<int>(std::errc::not_a_socket), std::system_category()),                        nifpp::str_atom("enotsock")},
    {std::error_code(static_cast<int>(std::errc::not_a_stream), std::system_category()),                        nifpp::str_atom("enostr")},
    {std::error_code(static_cast<int>(std::errc::not_connected), std::system_category()),                       nifpp::str_atom("enotconn")},
    {std::error_code(static_cast<int>(std::errc::not_enough_memory), std::system_category()),                   nifpp::str_atom("enomem")},
    {std::error_code(static_cast<int>(std::errc::not_supported), std::system_category()),                       nifpp::str_atom("enotsup")},
    {std::error_code(static_cast<int>(std::errc::operation_canceled), std::system_category()),                  nifpp::str_atom("ecanceled")},
    {std::error_code(static_cast<int>(std::errc::operation_in_progress), std::system_category()),               nifpp::str_atom("einprogress")},
    {std::error_code(static_cast<int>(std::errc::operation_not_permitted), std::system_category()),             nifpp::str_atom("eperm")},
    {std::error_code(static_cast<int>(std::errc::operation_not_supported), std::system_category()),             nifpp::str_atom("eopnotsupp")},
    {std::error_code(static_cast<int>(std::errc::operation_would_block), std::system_category()),               nifpp::str_atom("ewouldblock")},
    {std::error_code(static_cast<int>(std::errc::owner_dead), std::system_category()),                          nifpp::str_atom("eownerdead")},
    {std::error_code(static_cast<int>(std::errc::permission_denied), std::system_category()),                   nifpp::str_atom("eacces")},
    {std::error_code(static_cast<int>(std::errc::protocol_error), std::system_category()),                      nifpp::str_atom("eproto")},
    {std::error_code(static_cast<int>(std::errc::protocol_not_supported), std::system_category()),              nifpp::str_atom("eprotonosupport")},
    {std::error_code(static_cast<int>(std::errc::read_only_file_system), std::system_category()),               nifpp::str_atom("erofs")},
    {std::error_code(static_cast<int>(std::errc::resource_deadlock_would_occur), std::system_category()),       nifpp::str_atom("edeadlk")},
    {std::error_code(static_cast<int>(std::errc::resource_unavailable_try_again), std::system_category()),      nifpp::str_atom("eagain")},
    {std::error_code(static_cast<int>(std::errc::result_out_of_range), std::system_category()),                 nifpp::str_atom("erange")},
    {std::error_code(static_cast<int>(std::errc::state_not_recoverable), std::system_category()),               nifpp::str_atom("enotrecoverable")},
    {std::error_code(static_cast<int>(std::errc::stream_timeout), std::system_category()),                      nifpp::str_atom("etime")},
    {std::error_code(static_cast<int>(std::errc::text_file_busy), std::system_category()),                      nifpp::str_atom("etxtbsy")},
    {std::error_code(static_cast<int>(std::errc::timed_out), std::system_category()),                           nifpp::str_atom("etimedout")},
    {std::error_code(static_cast<int>(std::errc::too_many_files_open_in_system), std::system_category()),       nifpp::str_atom("enfile")},
    {std::error_code(static_cast<int>(std::errc::too_many_files_open), std::system_category()),                 nifpp::str_atom("emfile")},
    {std::error_code(static_cast<int>(std::errc::too_many_links), std::system_category()),                      nifpp::str_atom("emlink")},
    {std::error_code(static_cast<int>(std::errc::too_many_symbolic_link_levels), std::system_category()),       nifpp::str_atom("eloop")},
    {std::error_code(static_cast<int>(std::errc::value_too_large), std::system_category()),                     nifpp::str_atom("eoverflow")},
    {std::error_code(static_cast<int>(std::errc::wrong_protocol_type), std::system_category()),                 nifpp::str_atom("eprototype")}
};

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
        auto it = error_to_atom.find(e.code());
        nifpp::str_atom reason{e.code().message()};
        if(it != error_to_atom.end())
            reason = it->second;

        enif_send(ctx.env, &ctx.reqPid, ctx.env, nifpp::make(ctx.env, std::make_tuple(ctx.reqId, std::make_tuple(error, reason))));
    }
}

template<class T, class H>
void async_handle_result(const NifCTX &ctx, std::shared_ptr<one::helpers::future_t<T>> future, H &res_holder)
{
    application.callback_service.post([future, ctx, res_holder]() { handle_result(ctx, future); });
}

template<class T, class H = int>
void async_handle_result(const NifCTX &ctx, std::shared_ptr<one::helpers::future_t<T>> future)
{
    application.callback_service.post([future, ctx]() { handle_result(ctx, future); });
}

static ERL_NIF_TERM new_helper_obj(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[])
{
    auto helperName = nifpp::get<string>(env, argv[0]);
    auto helperArgs = get_args(env, argv[1]);
    auto helperObj = application.SHFactory.getStorageHelper(helperName, helperArgs);
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
     auto uid = uNameToUID(nifpp::get<string>(env, uidTerm));
     if(uid != static_cast<uid_t>(-1)) {
        return nifpp::make(env, std::make_tuple(ok, uid));
     } else {
        auto einval = std::error_code(static_cast<int>(std::errc::invalid_argument), std::system_category());
        return nifpp::make(env, std::make_tuple(error, error_to_atom[einval]));
     }
}

static ERL_NIF_TERM groupname_to_gid(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[])
{
     auto gidTerm = argv[0];
     auto gid = gNameToGID(nifpp::get<string>(env, gidTerm));
     if(gid != static_cast<gid_t>(-1)) {
        return nifpp::make(env, std::make_tuple(ok, gid));
     } else {
        auto einval = std::error_code(static_cast<int>(std::errc::invalid_argument), std::system_category());
        return nifpp::make(env, std::make_tuple(error, error_to_atom[einval]));
     }
}

static ERL_NIF_TERM set_user_ctx(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[])
{
    return handle_errors(env, [&]() {
        auto ctx = nifpp::get<helper_ctx_ptr>(env, argv[0]);


        auto uidTerm = argv[1];
        auto gidTerm = argv[2];
        long uid = -1;
        long gid = -1;

        if(!nifpp::get(env, uidTerm, uid)) {
            ctx->uid = uNameToUID(nifpp::get<string>(env, uidTerm));
        } else {
            ctx->uid = uid;
        }

        if(!nifpp::get(env, gidTerm, gid)) {
            ctx->gid = gNameToGID(nifpp::get<string>(env, gidTerm));
        } else {
            ctx->gid = gid;
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
            if((ctx->m_ffi.flags & flag.second) == flag.second) {
                flags.push_back(flag.first);
            }
        }

        for(auto &flag : atom_to_open_mode) {
            if((ctx->m_ffi.flags & O_ACCMODE) == flag.second) {
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
        auto foreach_result = nifpp::list_for_each<nifpp::str_atom>(env, argv[1], [&ctx](nifpp::str_atom atom) {
            auto it = atom_to_flag.find(atom);
            if(it == atom_to_flag.end()) {
                auto it_o = atom_to_open_mode.find(atom);
                if(it_o == atom_to_open_mode.end()) {
                    throw nifpp::badarg();
                } else {
                    ctx->m_ffi.flags |= it_o->second;
                }
            } else {
                ctx->m_ffi.flags |= it->second;
            }
        });

        if(!foreach_result)
            throw nifpp::badarg();

        return nifpp::make(env, ok);
    });
}

static ERL_NIF_TERM get_fd(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[])
{
    return handle_errors(env, [&]() {
        auto ctx = nifpp::get<helper_ctx_ptr>(env, argv[0]);
        return nifpp::make(env, std::make_tuple(ok, ctx->m_ffi.fh));
    });
}

static ERL_NIF_TERM set_fd(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[])
{
    return handle_errors(env, [&]() {
        auto ctx = nifpp::get<helper_ctx_ptr>(env, argv[0]);
        ctx->m_ffi.fh = nifpp::get<int>(env, argv[1]);
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
        application.workers.push_back(std::thread([]() { application.dio_service.run(); }));
        application.workers.push_back(std::thread([]() { application.callback_service.run(); }));
    }

    return !(nifpp::register_resource<helper_ptr>(env, nullptr, "helper_ptr") &&
             nifpp::register_resource<helper_ctx_ptr>(env, nullptr, "helper_ctx") &&
             nifpp::register_resource<fuse_file_info>(env, nullptr, "fuse_file_info"));
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
    {"set_fd",          2, set_fd},
    {"get_fd",          1, get_fd},
    {"username_to_uid", 1, username_to_uid},
    {"groupname_to_gid",1, groupname_to_gid},
    {"new_helper_obj",  2, new_helper_obj},
    {"new_helper_ctx",  0, new_helper_ctx},
    {"set_user_ctx",    3, set_user_ctx},
    {"get_user_ctx",    1, get_user_ctx}
};


ERL_NIF_INIT(helpers_nif, nif_funcs, load, NULL, NULL, NULL);

} // extern C
