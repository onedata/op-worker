#include "helpers/storageHelperFactory.h"

#include <asio/executor_work.hpp>
#include <asio.hpp>

#include <string>
#include <memory>
#include <vector>
#include <random>
#include <tuple>
#include <map>
#include <sstream>
#include <system_error>
#include "nifpp.h"

#include <pwd.h>
#include <grp.h>

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
using one::helpers::error_t;

/**
 * Static resource holder.
 */
struct HelpersNIF {
    std::shared_ptr<one::communication::Communicator> nullCommunicator =
        nullptr;
    one::helpers::BufferLimits limits = one::helpers::BufferLimits();
    asio::io_service dioService;
    asio::io_service cproxyService;
    asio::executor_work<asio::io_service::executor_type> dio_work =
        asio::make_work(dioService);

    std::vector<std::thread> workers;

    one::helpers::StorageHelperFactory SHFactory =
        one::helpers::StorageHelperFactory(
            nullCommunicator, limits, dioService, cproxyService);

    ~HelpersNIF()
    {
        dioService.stop();

        for (auto &th : workers) {
            th.join();
        }
    }

} application;

/**
 * @defgroup ModeTranslators Maps translating nifpp::str_atom into corresponding
 *           POSIX open mode / flag.
 * @{
 */
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


std::map<nifpp::str_atom, int> atom_to_file_type = {
    {"S_IFREG",    S_IFREG},
    {"S_IFCHR",    S_IFCHR},
    {"S_IFBLK",    S_IFBLK},
    {"S_IFIFO",    S_IFIFO},
    {"S_IFSOCK",   S_IFSOCK}
};

/** @} */

template <class T> error_t make_sys_error_code(T code)
{
    return error_t(static_cast<int>(code), std::system_category());
}

/**
 * @defgroup Errors Maps translating std::error_code to corresponding
 *           POSIX-like code description as atom.
 * @{
 */
std::map<error_t, nifpp::str_atom> error_to_atom = {
    {make_sys_error_code(std::errc::address_family_not_supported),        nifpp::str_atom("eafnosupport")},
    {make_sys_error_code(std::errc::address_in_use),                      nifpp::str_atom("eaddrinuse")},
    {make_sys_error_code(std::errc::address_not_available),               nifpp::str_atom("eaddrnotavail")},
    {make_sys_error_code(std::errc::already_connected),                   nifpp::str_atom("eisconn")},
    {make_sys_error_code(std::errc::argument_list_too_long),              nifpp::str_atom("e2big")},
    {make_sys_error_code(std::errc::argument_out_of_domain),              nifpp::str_atom("edom")},
    {make_sys_error_code(std::errc::bad_address),                         nifpp::str_atom("efault")},
    {make_sys_error_code(std::errc::bad_file_descriptor),                 nifpp::str_atom("ebadf")},
    {make_sys_error_code(std::errc::bad_message),                         nifpp::str_atom("ebadmsg")},
    {make_sys_error_code(std::errc::broken_pipe),                         nifpp::str_atom("epipe")},
    {make_sys_error_code(std::errc::connection_aborted),                  nifpp::str_atom("econnaborted")},
    {make_sys_error_code(std::errc::connection_already_in_progress),      nifpp::str_atom("ealready")},
    {make_sys_error_code(std::errc::connection_refused),                  nifpp::str_atom("econnrefused")},
    {make_sys_error_code(std::errc::connection_reset),                    nifpp::str_atom("econnreset")},
    {make_sys_error_code(std::errc::cross_device_link),                   nifpp::str_atom("exdev")},
    {make_sys_error_code(std::errc::destination_address_required),        nifpp::str_atom("edestaddrreq")},
    {make_sys_error_code(std::errc::device_or_resource_busy),             nifpp::str_atom("ebusy")},
    {make_sys_error_code(std::errc::directory_not_empty),                 nifpp::str_atom("enotempty")},
    {make_sys_error_code(std::errc::executable_format_error),             nifpp::str_atom("enoexec")},
    {make_sys_error_code(std::errc::file_exists),                         nifpp::str_atom("eexist")},
    {make_sys_error_code(std::errc::file_too_large),                      nifpp::str_atom("efbig")},
    {make_sys_error_code(std::errc::filename_too_long),                   nifpp::str_atom("enametoolong")},
    {make_sys_error_code(std::errc::function_not_supported),              nifpp::str_atom("enosys")},
    {make_sys_error_code(std::errc::host_unreachable),                    nifpp::str_atom("ehostunreach")},
    {make_sys_error_code(std::errc::identifier_removed),                  nifpp::str_atom("eidrm")},
    {make_sys_error_code(std::errc::illegal_byte_sequence),               nifpp::str_atom("eilseq")},
    {make_sys_error_code(std::errc::inappropriate_io_control_operation),  nifpp::str_atom("enotty")},
    {make_sys_error_code(std::errc::interrupted),                         nifpp::str_atom("eintr")},
    {make_sys_error_code(std::errc::invalid_argument),                    nifpp::str_atom("einval")},
    {make_sys_error_code(std::errc::invalid_seek),                        nifpp::str_atom("espipe")},
    {make_sys_error_code(std::errc::io_error),                            nifpp::str_atom("eio")},
    {make_sys_error_code(std::errc::is_a_directory),                      nifpp::str_atom("eisdir")},
    {make_sys_error_code(std::errc::message_size),                        nifpp::str_atom("emsgsize")},
    {make_sys_error_code(std::errc::network_down),                        nifpp::str_atom("enetdown")},
    {make_sys_error_code(std::errc::network_reset),                       nifpp::str_atom("enetreset")},
    {make_sys_error_code(std::errc::network_unreachable),                 nifpp::str_atom("enetunreach")},
    {make_sys_error_code(std::errc::no_buffer_space),                     nifpp::str_atom("enobufs")},
    {make_sys_error_code(std::errc::no_child_process),                    nifpp::str_atom("echild")},
    {make_sys_error_code(std::errc::no_link),                             nifpp::str_atom("enolink")},
    {make_sys_error_code(std::errc::no_lock_available),                   nifpp::str_atom("enolck")},
    {make_sys_error_code(std::errc::no_message_available),                nifpp::str_atom("enodata")},
    {make_sys_error_code(std::errc::no_message),                          nifpp::str_atom("enomsg")},
    {make_sys_error_code(std::errc::no_protocol_option),                  nifpp::str_atom("enoprotoopt")},
    {make_sys_error_code(std::errc::no_space_on_device),                  nifpp::str_atom("enospc")},
    {make_sys_error_code(std::errc::no_stream_resources),                 nifpp::str_atom("enosr")},
    {make_sys_error_code(std::errc::no_such_device_or_address),           nifpp::str_atom("enxio")},
    {make_sys_error_code(std::errc::no_such_device),                      nifpp::str_atom("enodev")},
    {make_sys_error_code(std::errc::no_such_file_or_directory),           nifpp::str_atom("enoent")},
    {make_sys_error_code(std::errc::no_such_process),                     nifpp::str_atom("esrch")},
    {make_sys_error_code(std::errc::not_a_directory),                     nifpp::str_atom("enotdir")},
    {make_sys_error_code(std::errc::not_a_socket),                        nifpp::str_atom("enotsock")},
    {make_sys_error_code(std::errc::not_a_stream),                        nifpp::str_atom("enostr")},
    {make_sys_error_code(std::errc::not_connected),                       nifpp::str_atom("enotconn")},
    {make_sys_error_code(std::errc::not_enough_memory),                   nifpp::str_atom("enomem")},
    {make_sys_error_code(std::errc::not_supported),                       nifpp::str_atom("enotsup")},
    {make_sys_error_code(std::errc::operation_canceled),                  nifpp::str_atom("ecanceled")},
    {make_sys_error_code(std::errc::operation_in_progress),               nifpp::str_atom("einprogress")},
    {make_sys_error_code(std::errc::operation_not_permitted),             nifpp::str_atom("eperm")},
    {make_sys_error_code(std::errc::operation_not_supported),             nifpp::str_atom("eopnotsupp")},
    {make_sys_error_code(std::errc::operation_would_block),               nifpp::str_atom("ewouldblock")},
    {make_sys_error_code(std::errc::owner_dead),                          nifpp::str_atom("eownerdead")},
    {make_sys_error_code(std::errc::permission_denied),                   nifpp::str_atom("eacces")},
    {make_sys_error_code(std::errc::protocol_error),                      nifpp::str_atom("eproto")},
    {make_sys_error_code(std::errc::protocol_not_supported),              nifpp::str_atom("eprotonosupport")},
    {make_sys_error_code(std::errc::read_only_file_system),               nifpp::str_atom("erofs")},
    {make_sys_error_code(std::errc::resource_deadlock_would_occur),       nifpp::str_atom("edeadlk")},
    {make_sys_error_code(std::errc::resource_unavailable_try_again),      nifpp::str_atom("eagain")},
    {make_sys_error_code(std::errc::result_out_of_range),                 nifpp::str_atom("erange")},
    {make_sys_error_code(std::errc::state_not_recoverable),               nifpp::str_atom("enotrecoverable")},
    {make_sys_error_code(std::errc::stream_timeout),                      nifpp::str_atom("etime")},
    {make_sys_error_code(std::errc::text_file_busy),                      nifpp::str_atom("etxtbsy")},
    {make_sys_error_code(std::errc::timed_out),                           nifpp::str_atom("etimedout")},
    {make_sys_error_code(std::errc::too_many_files_open_in_system),       nifpp::str_atom("enfile")},
    {make_sys_error_code(std::errc::too_many_files_open),                 nifpp::str_atom("emfile")},
    {make_sys_error_code(std::errc::too_many_links),                      nifpp::str_atom("emlink")},
    {make_sys_error_code(std::errc::too_many_symbolic_link_levels),       nifpp::str_atom("eloop")},
    {make_sys_error_code(std::errc::value_too_large),                     nifpp::str_atom("eoverflow")},
    {make_sys_error_code(std::errc::wrong_protocol_type),                 nifpp::str_atom("eprototype")}
};
/** @} */

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

/**
 * NIF context holder for all common operations.
 */
struct NifCTX {
    NifCTX(ErlNifEnv *env, Env localEnv, ErlNifPid pid, reqid_t reqId,
        helper_ptr helperObj, helper_ctx_ptr helperCTX)
        : env(env)
        , localEnv(localEnv)
        , reqPid(pid)
        , reqId(reqId)
        , helperObj(helperObj)
        , helperCTX(helperCTX)
    {
    }

    ~NifCTX()
    {
        // @todo: get valid file_id instead of empty one (its still works though since we are closing descriptor from CTX)
        helperObj->ash_release(*helperCTX, "", [=](error_t e) {  });
    }

    ErlNifEnv *env;
    Env localEnv;
    ErlNifPid reqPid;
    reqid_t reqId;
    helper_ptr helperObj;
    helper_ctx_ptr helperCTX;
};

/**
 * Converts NIF term to one::helpers::IStorageHelper::ArgsMap structure.
 */
one::helpers::IStorageHelper::ArgsMap get_helper_args(
    ErlNifEnv *env, ERL_NIF_TERM term)
{
    one::helpers::IStorageHelper::ArgsMap args;

    if (enif_is_list(env, term) && !enif_is_empty_list(env, term)) {
        int i = 0;
        ERL_NIF_TERM list, head, tail;
        for (list = term; enif_get_list_cell(env, list, &head, &tail);
             list = tail, ++i)
            args.emplace(
                one::helpers::srvArg(i), nifpp::get<std::string>(env, head));
    }

    return args;
}

/**
 * Runs given function and returns result or error term.
 */
template <class T> ERL_NIF_TERM handle_errors(ErlNifEnv *env, const T &fun)
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
ERL_NIF_TERM wrap_helper(ERL_NIF_TERM (*fun)(NifCTX ctx, Args...),
    ErlNifEnv *env, const ERL_NIF_TERM args[], std::index_sequence<I...>)
{
    return handle_errors(env, [&]() {
        ErlNifPid pid;
        enif_self(env, &pid);

        std::random_device rd;
        std::mt19937 gen(rd());

        auto reqId = std::make_tuple(std::rand(), std::rand(), std::rand());
        return fun(
            NifCTX(env, Env(), pid, reqId, nifpp::get<helper_ptr>(env, args[0]),
                nifpp::get<helper_ctx_ptr>(env, args[1])),
            nifpp::get<Args>(env, args[2 + I])...);
    });
}

template <typename... Args>
ERL_NIF_TERM wrap(ERL_NIF_TERM (*fun)(NifCTX, Args...), ErlNifEnv *env,
    const ERL_NIF_TERM args[])
{
    return wrap_helper(fun, env, args, std::index_sequence_for<Args...>{});
}

template <typename... Args, std::size_t... I>
ERL_NIF_TERM noctx_wrap_helper(ERL_NIF_TERM (*fun)(ErlNifEnv *env, Args...),
    ErlNifEnv *env, const ERL_NIF_TERM args[], std::index_sequence<I...>)
{
    return handle_errors(
        env, [&]() { return fun(env, nifpp::get<Args>(env, args[I])...); });
}

template <typename... Args>
ERL_NIF_TERM noctx_wrap(ERL_NIF_TERM (*fun)(ErlNifEnv *env, Args...),
    ErlNifEnv *env, const ERL_NIF_TERM args[])
{
    return noctx_wrap_helper(
        fun, env, args, std::index_sequence_for<Args...>{});
}

/**
 * Translates user name to uid.
 */
uid_t uNameToUID(const std::string &uname)
{
    struct passwd *ownerInfo =
        getpwnam(uname.c_str()); // Static buffer, do NOT free !
    return (ownerInfo ? ownerInfo->pw_uid : -1);
}

/**
 * Translates group name to gid.
 */
gid_t gNameToGID(const std::string &gname, const std::string &uname = "")
{
    struct passwd *ownerInfo =
        getpwnam(uname.c_str()); // Static buffer, do NOT free !
    struct group *groupInfo =
        getgrnam(gname.c_str()); // Static buffer, do NOT free !

    gid_t primary_gid = (ownerInfo ? ownerInfo->pw_gid : -1);
    return (groupInfo ? groupInfo->gr_gid : primary_gid);
}

/**
 * Handle asio::mutable_buffer value from helpers and send it to requesting
 * process.
 */
void handle_value(NifCTX &ctx, asio::mutable_buffer buffer)
{
    nifpp::binary bin(asio::buffer_size(buffer));
    asio::buffer_copy(asio::mutable_buffer{bin.data, bin.size}, buffer);
    enif_send(nullptr, &ctx.reqPid, ctx.localEnv,
        nifpp::make(ctx.localEnv,
                  std::make_tuple(ctx.reqId,
                        std::make_tuple(ok, nifpp::make(ctx.localEnv, bin)))));
}

/**
 * Handle struct stat value from helpers and send it to requesting process.
 */
void handle_value(NifCTX &ctx, struct stat &s)
{
    auto record =
        std::make_tuple(nifpp::str_atom("statbuf"), s.st_dev, s.st_ino,
            s.st_mode, s.st_nlink, s.st_uid, s.st_gid, s.st_rdev, s.st_size,
            s.st_atime, s.st_mtime, s.st_ctime, s.st_blksize, s.st_blocks);
    enif_send(nullptr, &ctx.reqPid, ctx.localEnv,
        nifpp::make(ctx.localEnv,
                  std::make_tuple(ctx.reqId, std::make_tuple(ok, record))));
}

/**
 * Handle generic result from helpers and send it to requesting process.
 */
template <class T> void handle_value(NifCTX &ctx, T &response)
{
    enif_send(nullptr, &ctx.reqPid, ctx.localEnv,
        nifpp::make(ctx.localEnv,
                  std::make_tuple(ctx.reqId, std::make_tuple(ok, response))));
}

/**
 * Handle void value from helpers and send it to requesting process.
 */
void handle_value(NifCTX &ctx)
{
    enif_send(nullptr, &ctx.reqPid, ctx.localEnv,
        nifpp::make(ctx.localEnv, std::make_tuple(ctx.reqId, ok)));
}

/**
 * Handles result from helpers callback either process return value or error.
 */
template <class... T>
void handle_result(NifCTX ctx, error_t e, T... value)
{
    if (!e) {
        handle_value(ctx, value...);
    }
    else {
        auto it = error_to_atom.find(e);
        nifpp::str_atom reason{e.message()};
        if (it != error_to_atom.end())
            reason = it->second;

        enif_send(nullptr, &ctx.reqPid, ctx.localEnv,
            nifpp::make(ctx.localEnv, std::make_tuple(ctx.reqId,
                                     std::make_tuple(error, reason))));
    }
}

/*********************************************************************
*
*                          WRAPPERS (NIF based)
*       All functions below are described in helpers_nif.erl
*
*********************************************************************/

ERL_NIF_TERM new_helper_obj(ErlNifEnv *env, int argc, const ERL_NIF_TERM argv[])
{
    auto helperName = nifpp::get<std::string>(env, argv[0]);
    auto helperArgs = get_helper_args(env, argv[1]);
    auto helperObj =
        application.SHFactory.getStorageHelper(helperName, helperArgs);
    if (!helperObj)
        return nifpp::make(
            env, std::make_tuple(error, nifpp::str_atom("invalid_helper")));

    auto resource = nifpp::construct_resource<helper_ptr>(helperObj);

    return nifpp::make(env, std::make_tuple(ok, resource));
}

ERL_NIF_TERM username_to_uid(
    ErlNifEnv *env, int argc, const ERL_NIF_TERM argv[])
{
    auto uidTerm = argv[0];
    auto uid = uNameToUID(nifpp::get<std::string>(env, uidTerm));
    if (uid != static_cast<uid_t>(-1))
        return nifpp::make(env, std::make_tuple(ok, uid));

    auto einval = make_sys_error_code(std::errc::invalid_argument);
    return nifpp::make(env, std::make_tuple(error, error_to_atom[einval]));
}

ERL_NIF_TERM groupname_to_gid(
    ErlNifEnv *env, int argc, const ERL_NIF_TERM argv[])
{
    auto gidTerm = argv[0];
    auto gid = gNameToGID(nifpp::get<std::string>(env, gidTerm));
    if (gid != static_cast<gid_t>(-1))
        return nifpp::make(env, std::make_tuple(ok, gid));

    auto einval = make_sys_error_code(std::errc::invalid_argument);
    return nifpp::make(env, std::make_tuple(error, error_to_atom[einval]));
}

ERL_NIF_TERM set_user_ctx(ErlNifEnv *env, int argc, const ERL_NIF_TERM argv[])
{
    return handle_errors(env, [&]() {
        auto ctx = nifpp::get<helper_ctx_ptr>(env, argv[0]);

        auto uidTerm = argv[1];
        auto gidTerm = argv[2];
        long uid = -1;
        long gid = -1;

        if (!nifpp::get(env, uidTerm, uid)) {
            ctx->uid = uNameToUID(nifpp::get<std::string>(env, uidTerm));
        }
        else {
            ctx->uid = uid;
        }

        if (!nifpp::get(env, gidTerm, gid)) {
            ctx->gid = gNameToGID(nifpp::get<std::string>(env, gidTerm));
        }
        else {
            ctx->gid = gid;
        }

        return nifpp::make(env, ok);
    });
}

ERL_NIF_TERM get_flag_value(ErlNifEnv *env, nifpp::str_atom flag)
{
    auto it = atom_to_flag.find(flag);
    if (it != atom_to_flag.end())
        return nifpp::make(env, it->second);

    it = atom_to_open_mode.find(flag);
    if (it != atom_to_open_mode.end())
        return nifpp::make(env, it->second);

    it = atom_to_file_type.find(flag);
    if (it != atom_to_file_type.end())
        return nifpp::make(env, it->second);

    throw nifpp::badarg();
}

ERL_NIF_TERM new_helper_ctx(ErlNifEnv *env)
{
    auto ctx = std::make_shared<one::helpers::StorageHelperCTX>();
    auto ctx_resource = nifpp::construct_resource<helper_ctx_ptr>(ctx);
    return nifpp::make(env, std::make_tuple(ok, ctx_resource));
}

ERL_NIF_TERM get_user_ctx(ErlNifEnv *env, helper_ctx_ptr ctx)
{
    return nifpp::make(
        env, std::make_tuple(ok, std::make_tuple(ctx->uid, ctx->gid)));
}

ERL_NIF_TERM get_flags(ErlNifEnv *env, helper_ctx_ptr ctx)
{
    std::vector<nifpp::str_atom> flags;
    for (auto &flag : atom_to_flag) {
        if (ctx->m_ffi.flags & flag.second) {
            flags.push_back(flag.first);
        }
    }

    for (auto &flag : atom_to_open_mode) {
        // Mask only open mode (ACCMODE) and compare by value
        if ((ctx->m_ffi.flags & O_ACCMODE) == flag.second) {
            flags.push_back(flag.first);
        }
    }

    return nifpp::make(env, std::make_tuple(ok, flags));
}

ERL_NIF_TERM set_flags(
    ErlNifEnv *env, helper_ctx_ptr ctx, std::vector<nifpp::str_atom> flagAtoms)
{
    ctx->m_ffi.flags = 0;
    for (auto &atom : flagAtoms) {
        auto flagTerm = get_flag_value(env, atom);
        auto flag = nifpp::get<int>(env, flagTerm);
        ctx->m_ffi.flags |= flag;
    }

    return nifpp::make(env, ok);
}

ERL_NIF_TERM get_fd(ErlNifEnv *env, helper_ctx_ptr ctx)
{
    return nifpp::make(env, std::make_tuple(ok, ctx->m_ffi.fh));
}

ERL_NIF_TERM set_fd(ErlNifEnv *env, helper_ctx_ptr ctx, int fh)
{
    ctx->m_ffi.fh = fh;
    return nifpp::make(env, ok);
}

ERL_NIF_TERM getattr(NifCTX ctx, const std::string file)
{
    ctx.helperObj->ash_getattr(
        *ctx.helperCTX, file, [=](struct stat statbuf, error_t e) {
            handle_result(ctx, e, statbuf);
        });

    return nifpp::make(ctx.env, std::make_tuple(ok, ctx.reqId));
}

ERL_NIF_TERM access(NifCTX ctx, const std::string file, const int mask)
{
    ctx.helperObj->ash_access(
        *ctx.helperCTX, file, mask, [=](error_t e) { handle_result(ctx, e); });

    return nifpp::make(ctx.env, std::make_tuple(ok, ctx.reqId));
}

ERL_NIF_TERM mknod(
    NifCTX ctx, const std::string file, const mode_t mode, const dev_t dev)
{
    ctx.helperObj->ash_mknod(*ctx.helperCTX, file, mode, dev,
        [=](error_t e) { handle_result(ctx, e); });

    return nifpp::make(ctx.env, std::make_tuple(ok, ctx.reqId));
}

ERL_NIF_TERM mkdir(NifCTX ctx, const std::string file, const mode_t mode)
{
    ctx.helperObj->ash_mkdir(
        *ctx.helperCTX, file, mode, [=](error_t e) { handle_result(ctx, e); });

    return nifpp::make(ctx.env, std::make_tuple(ok, ctx.reqId));
}

ERL_NIF_TERM unlink(NifCTX ctx, const std::string file)
{
    ctx.helperObj->ash_unlink(
        *ctx.helperCTX, file, [=](error_t e) { handle_result(ctx, e); });

    return nifpp::make(ctx.env, std::make_tuple(ok, ctx.reqId));
}

ERL_NIF_TERM rmdir(NifCTX ctx, const std::string file)
{
    ctx.helperObj->ash_rmdir(
        *ctx.helperCTX, file, [=](error_t e) { handle_result(ctx, e); });

    return nifpp::make(ctx.env, std::make_tuple(ok, ctx.reqId));
}

ERL_NIF_TERM symlink(NifCTX ctx, const std::string from, const std::string to)
{
    ctx.helperObj->ash_symlink(
        *ctx.helperCTX, from, to, [=](error_t e) { handle_result(ctx, e); });

    return nifpp::make(ctx.env, std::make_tuple(ok, ctx.reqId));
}

ERL_NIF_TERM rename(NifCTX ctx, const std::string from, const std::string to)
{
    ctx.helperObj->ash_rename(
        *ctx.helperCTX, from, to, [=](error_t e) { handle_result(ctx, e); });

    return nifpp::make(ctx.env, std::make_tuple(ok, ctx.reqId));
}

ERL_NIF_TERM link(NifCTX ctx, const std::string from, const std::string to)
{
    ctx.helperObj->ash_link(
        *ctx.helperCTX, from, to, [=](error_t e) { handle_result(ctx, e); });

    return nifpp::make(ctx.env, std::make_tuple(ok, ctx.reqId));
}

ERL_NIF_TERM chmod(NifCTX ctx, const std::string file, const mode_t mode)
{
    ctx.helperObj->ash_chmod(
        *ctx.helperCTX, file, mode, [=](error_t e) { handle_result(ctx, e); });

    return nifpp::make(ctx.env, std::make_tuple(ok, ctx.reqId));
}

ERL_NIF_TERM chown(
    NifCTX ctx, const std::string file, const int uid, const int gid)
{
    ctx.helperObj->ash_chown(*ctx.helperCTX, file, uid, gid,
        [=](error_t e) { handle_result(ctx, e); });

    return nifpp::make(ctx.env, std::make_tuple(ok, ctx.reqId));
}

ERL_NIF_TERM truncate(NifCTX ctx, const std::string file, const off_t size)
{
    ctx.helperObj->ash_truncate(
        *ctx.helperCTX, file, size, [=](error_t e) { handle_result(ctx, e); });

    return nifpp::make(ctx.env, std::make_tuple(ok, ctx.reqId));
}

ERL_NIF_TERM open(NifCTX ctx, const std::string file)
{
    ctx.helperObj->ash_open(*ctx.helperCTX, file,
        [=](int fh, error_t e) { handle_result(ctx, e, fh); });

    return nifpp::make(ctx.env, std::make_tuple(ok, ctx.reqId));
}

ERL_NIF_TERM read(NifCTX ctx, const std::string file, off_t offset, size_t size)
{
    auto buf = std::make_shared<std::vector<char>>(size);
    ctx.helperObj->ash_read(*ctx.helperCTX, file,
        asio::mutable_buffer(buf->data(), size), offset,
        [ctx, buf](asio::mutable_buffer mbuf, error_t e) {
            handle_result(ctx, e, mbuf);
        });

    return nifpp::make(ctx.env, std::make_tuple(ok, ctx.reqId));
}

ERL_NIF_TERM write(
    NifCTX ctx, const std::string file, const off_t offset, std::string data)
{
    auto sData = std::make_shared<std::string>(std::move(data));
    ctx.helperObj->ash_write(*ctx.helperCTX, file,
        asio::const_buffer(sData->data(), sData->size()), offset,
        [ctx, file, offset, sData](int size, error_t e) { handle_result(ctx, e, size); });

    return nifpp::make(ctx.env, std::make_tuple(ok, ctx.reqId));
}

ERL_NIF_TERM release(NifCTX ctx, const std::string file)
{
    ctx.helperObj->ash_release(
        *ctx.helperCTX, file, [=](error_t e) { handle_result(ctx, e); });

    return nifpp::make(ctx.env, std::make_tuple(ok, ctx.reqId));
}

ERL_NIF_TERM flush(NifCTX ctx, const std::string file)
{
    ctx.helperObj->ash_flush(
        *ctx.helperCTX, file, [=](error_t e) { handle_result(ctx, e); });

    return nifpp::make(ctx.env, std::make_tuple(ok, ctx.reqId));
}

ERL_NIF_TERM fsync(NifCTX ctx, const std::string file, const int isdatasync)
{
    ctx.helperObj->ash_fsync(*ctx.helperCTX, file, isdatasync,
        [=](error_t e) { handle_result(ctx, e); });

    return nifpp::make(ctx.env, std::make_tuple(ok, ctx.reqId));
}

} // namespace

extern "C" {

static int load(ErlNifEnv *env, void **priv_data, ERL_NIF_TERM load_info)
{
    for (auto i = 0; i < 100; ++i) {
        application.workers.push_back(
            std::thread([]() { application.dioService.run(); }));
    }

    return !(nifpp::register_resource<helper_ptr>(env, nullptr, "helper_ptr") &&
               nifpp::register_resource<helper_ctx_ptr>(
                   env, nullptr, "helper_ctx") &&
               nifpp::register_resource<fuse_file_info>(
                   env, nullptr, "fuse_file_info"));
}

static ERL_NIF_TERM sh_set_flags(
    ErlNifEnv *env, int argc, const ERL_NIF_TERM argv[])
{
    return noctx_wrap(set_flags, env, argv);
}

static ERL_NIF_TERM sh_get_flags(
    ErlNifEnv *env, int argc, const ERL_NIF_TERM argv[])
{
    return noctx_wrap(get_flags, env, argv);
}

static ERL_NIF_TERM sh_set_fd(
    ErlNifEnv *env, int argc, const ERL_NIF_TERM argv[])
{
    return noctx_wrap(set_fd, env, argv);
}

static ERL_NIF_TERM sh_get_fd(
    ErlNifEnv *env, int argc, const ERL_NIF_TERM argv[])
{
    return noctx_wrap(get_fd, env, argv);
}

static ERL_NIF_TERM sh_new_helper_ctx(
    ErlNifEnv *env, int argc, const ERL_NIF_TERM argv[])
{
    return noctx_wrap(new_helper_ctx, env, argv);
}

static ERL_NIF_TERM sh_get_user_ctx(
    ErlNifEnv *env, int argc, const ERL_NIF_TERM argv[])
{
    return noctx_wrap(get_user_ctx, env, argv);
}

static ERL_NIF_TERM sh_get_flag_value(
    ErlNifEnv *env, int argc, const ERL_NIF_TERM argv[])
{
    return noctx_wrap(get_flag_value, env, argv);
}

static ERL_NIF_TERM sh_getattr(
    ErlNifEnv *env, int argc, const ERL_NIF_TERM argv[])
{
    return wrap(getattr, env, argv);
}

static ERL_NIF_TERM sh_access(
    ErlNifEnv *env, int argc, const ERL_NIF_TERM argv[])
{
    return wrap(access, env, argv);
}

static ERL_NIF_TERM sh_mknod(
    ErlNifEnv *env, int argc, const ERL_NIF_TERM argv[])
{
    return wrap(mknod, env, argv);
}

static ERL_NIF_TERM sh_mkdir(
    ErlNifEnv *env, int argc, const ERL_NIF_TERM argv[])
{
    return wrap(mkdir, env, argv);
}

static ERL_NIF_TERM sh_unlink(
    ErlNifEnv *env, int argc, const ERL_NIF_TERM argv[])
{
    return wrap(unlink, env, argv);
}

static ERL_NIF_TERM sh_rmdir(
    ErlNifEnv *env, int argc, const ERL_NIF_TERM argv[])
{
    return wrap(rmdir, env, argv);
}

static ERL_NIF_TERM sh_symlink(
    ErlNifEnv *env, int argc, const ERL_NIF_TERM argv[])
{
    return wrap(symlink, env, argv);
}

static ERL_NIF_TERM sh_rename(
    ErlNifEnv *env, int argc, const ERL_NIF_TERM argv[])
{
    return wrap(rename, env, argv);
}

static ERL_NIF_TERM sh_link(ErlNifEnv *env, int argc, const ERL_NIF_TERM argv[])
{
    return wrap(link, env, argv);
}

static ERL_NIF_TERM sh_chmod(
    ErlNifEnv *env, int argc, const ERL_NIF_TERM argv[])
{
    return wrap(chmod, env, argv);
}

static ERL_NIF_TERM sh_chown(
    ErlNifEnv *env, int argc, const ERL_NIF_TERM argv[])
{
    return wrap(chown, env, argv);
}

static ERL_NIF_TERM sh_truncate(
    ErlNifEnv *env, int argc, const ERL_NIF_TERM argv[])
{
    return wrap(truncate, env, argv);
}

static ERL_NIF_TERM sh_open(ErlNifEnv *env, int argc, const ERL_NIF_TERM argv[])
{
    return wrap(open, env, argv);
}

static ERL_NIF_TERM sh_read(ErlNifEnv *env, int argc, const ERL_NIF_TERM argv[])
{
    return wrap(read, env, argv);
}

static ERL_NIF_TERM sh_write(
    ErlNifEnv *env, int argc, const ERL_NIF_TERM argv[])
{
    return wrap(write, env, argv);
}

static ERL_NIF_TERM sh_release(
    ErlNifEnv *env, int argc, const ERL_NIF_TERM argv[])
{
    return wrap(release, env, argv);
}

static ERL_NIF_TERM sh_flush(
    ErlNifEnv *env, int argc, const ERL_NIF_TERM argv[])
{
    return wrap(flush, env, argv);
}

static ERL_NIF_TERM sh_fsync(
    ErlNifEnv *env, int argc, const ERL_NIF_TERM argv[])
{
    return wrap(fsync, env, argv);
}

static ErlNifFunc nif_funcs[] = {
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

    {"set_flags",       2, sh_set_flags},
    {"get_flags",       1, sh_get_flags},
    {"set_fd",          2, sh_set_fd},
    {"get_fd",          1, sh_get_fd},
    {"username_to_uid", 1, username_to_uid},
    {"groupname_to_gid",1, groupname_to_gid},
    {"new_helper_obj",  2, new_helper_obj},
    {"new_helper_ctx",  0, sh_new_helper_ctx},
    {"set_user_ctx",    3, set_user_ctx},
    {"get_user_ctx",    1, sh_get_user_ctx},
    {"get_flag_value",  1, sh_get_flag_value}
};

ERL_NIF_INIT(helpers_nif, nif_funcs, load, NULL, NULL, NULL);

} // extern C
