#include "helpers/storageHelperFactory.h"

#include <asio/executor_work.hpp>
#include <asio.hpp>
#include <boost/bimap.hpp>

#include <string>
#include <memory>
#include <vector>
#include <random>
#include <tuple>
#include <map>
#include <sstream>
#include <system_error>
#include "nifpp.h"

#include <sys/types.h>
#include <sys/stat.h>
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
using helper_ctx_ptr = std::shared_ptr<one::helpers::IStorageHelperCTX>;
using reqid_t = std::tuple<int, int, int>;
using one::helpers::error_t;
using helper_args_t = std::unordered_map<std::string, std::string>;

/**
 * Static resource holder.
 */
struct HelpersNIF {
    asio::io_service ioService;
    asio::executor_work<asio::io_service::executor_type> ioWork =
        asio::make_work(ioService);

    std::vector<std::thread> workers;

    one::helpers::StorageHelperFactory SHFactory =
        one::helpers::StorageHelperFactory(ioService, ioService, ioService);

    HelpersNIF() { umask(0); }

    ~HelpersNIF()
    {
        ioService.stop();

        for (auto &th : workers) {
            th.join();
        }
    }

} application;

namespace {

using Translation =
    boost::bimap<nifpp::str_atom, one::helpers::IStorageHelperCTX::Flag>;

/**
 * @defgroup ModeTranslators Maps translating nifpp::str_atom into corresponding
 *           POSIX open mode / flag.
 * @{
 */
Translation createAtomToFlagTranslation()
{
    using namespace one::helpers;

    const std::vector<Translation::value_type> pairs{
        {"O_NONBLOCK", IStorageHelperCTX::Flag::NONBLOCK},
        {"O_APPEND", IStorageHelperCTX::Flag::APPEND},
        {"O_ASYNC", IStorageHelperCTX::Flag::ASYNC},
        {"O_FSYNC", IStorageHelperCTX::Flag::FSYNC},
        {"O_NOFOLLOW", IStorageHelperCTX::Flag::NOFOLLOW},
        {"O_CREAT", IStorageHelperCTX::Flag::CREAT},
        {"O_TRUNC", IStorageHelperCTX::Flag::TRUNC},
        {"O_EXCL", IStorageHelperCTX::Flag::EXCL},
        {"O_RDONLY", IStorageHelperCTX::Flag::RDONLY},
        {"O_WRONLY", IStorageHelperCTX::Flag::WRONLY},
        {"O_RDWR", IStorageHelperCTX::Flag::RDWR},
        {"S_IFREG", IStorageHelperCTX::Flag::IFREG},
        {"S_IFCHR", IStorageHelperCTX::Flag::IFCHR},
        {"S_IFBLK", IStorageHelperCTX::Flag::IFBLK},
        {"S_IFIFO", IStorageHelperCTX::Flag::IFIFO},
        {"S_IFSOCK", IStorageHelperCTX::Flag::IFSOCK}};

    return {pairs.begin(), pairs.end()};
}

const Translation atom_to_flag = createAtomToFlagTranslation();
}

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
    {make_sys_error_code(std::errc::address_family_not_supported),
        "eafnosupport"},
    {make_sys_error_code(std::errc::address_in_use), "eaddrinuse"},
    {make_sys_error_code(std::errc::address_not_available), "eaddrnotavail"},
    {make_sys_error_code(std::errc::already_connected), "eisconn"},
    {make_sys_error_code(std::errc::argument_list_too_long), "e2big"},
    {make_sys_error_code(std::errc::argument_out_of_domain), "edom"},
    {make_sys_error_code(std::errc::bad_address), "efault"},
    {make_sys_error_code(std::errc::bad_file_descriptor), "ebadf"},
    {make_sys_error_code(std::errc::bad_message), "ebadmsg"},
    {make_sys_error_code(std::errc::broken_pipe), "epipe"},
    {make_sys_error_code(std::errc::connection_aborted), "econnaborted"},
    {make_sys_error_code(std::errc::connection_already_in_progress),
        "ealready"},
    {make_sys_error_code(std::errc::connection_refused), "econnrefused"},
    {make_sys_error_code(std::errc::connection_reset), "econnreset"},
    {make_sys_error_code(std::errc::cross_device_link), "exdev"},
    {make_sys_error_code(std::errc::destination_address_required),
        "edestaddrreq"},
    {make_sys_error_code(std::errc::device_or_resource_busy), "ebusy"},
    {make_sys_error_code(std::errc::directory_not_empty), "enotempty"},
    {make_sys_error_code(std::errc::executable_format_error), "enoexec"},
    {make_sys_error_code(std::errc::file_exists), "eexist"},
    {make_sys_error_code(std::errc::file_too_large), "efbig"},
    {make_sys_error_code(std::errc::filename_too_long), "enametoolong"},
    {make_sys_error_code(std::errc::function_not_supported), "enosys"},
    {make_sys_error_code(std::errc::host_unreachable), "ehostunreach"},
    {make_sys_error_code(std::errc::identifier_removed), "eidrm"},
    {make_sys_error_code(std::errc::illegal_byte_sequence), "eilseq"},
    {make_sys_error_code(std::errc::inappropriate_io_control_operation),
        "enotty"},
    {make_sys_error_code(std::errc::interrupted), "eintr"},
    {make_sys_error_code(std::errc::invalid_argument), "einval"},
    {make_sys_error_code(std::errc::invalid_seek), "espipe"},
    {make_sys_error_code(std::errc::io_error), "eio"},
    {make_sys_error_code(std::errc::is_a_directory), "eisdir"},
    {make_sys_error_code(std::errc::message_size), "emsgsize"},
    {make_sys_error_code(std::errc::network_down), "enetdown"},
    {make_sys_error_code(std::errc::network_reset), "enetreset"},
    {make_sys_error_code(std::errc::network_unreachable), "enetunreach"},
    {make_sys_error_code(std::errc::no_buffer_space), "enobufs"},
    {make_sys_error_code(std::errc::no_child_process), "echild"},
    {make_sys_error_code(std::errc::no_link), "enolink"},
    {make_sys_error_code(std::errc::no_lock_available), "enolck"},
    {make_sys_error_code(std::errc::no_message_available), "enodata"},
    {make_sys_error_code(std::errc::no_message), "enomsg"},
    {make_sys_error_code(std::errc::no_protocol_option), "enoprotoopt"},
    {make_sys_error_code(std::errc::no_space_on_device), "enospc"},
    {make_sys_error_code(std::errc::no_stream_resources), "enosr"},
    {make_sys_error_code(std::errc::no_such_device_or_address), "enxio"},
    {make_sys_error_code(std::errc::no_such_device), "enodev"},
    {make_sys_error_code(std::errc::no_such_file_or_directory), "enoent"},
    {make_sys_error_code(std::errc::no_such_process), "esrch"},
    {make_sys_error_code(std::errc::not_a_directory), "enotdir"},
    {make_sys_error_code(std::errc::not_a_socket), "enotsock"},
    {make_sys_error_code(std::errc::not_a_stream), "enostr"},
    {make_sys_error_code(std::errc::not_connected), "enotconn"},
    {make_sys_error_code(std::errc::not_enough_memory), "enomem"},
    {make_sys_error_code(std::errc::not_supported), "enotsup"},
    {make_sys_error_code(std::errc::operation_canceled), "ecanceled"},
    {make_sys_error_code(std::errc::operation_in_progress), "einprogress"},
    {make_sys_error_code(std::errc::operation_not_permitted), "eperm"},
    {make_sys_error_code(std::errc::operation_not_supported), "eopnotsupp"},
    {make_sys_error_code(std::errc::operation_would_block), "ewouldblock"},
    {make_sys_error_code(std::errc::owner_dead), "eownerdead"},
    {make_sys_error_code(std::errc::permission_denied), "eacces"},
    {make_sys_error_code(std::errc::protocol_error), "eproto"},
    {make_sys_error_code(std::errc::protocol_not_supported), "eprotonosupport"},
    {make_sys_error_code(std::errc::read_only_file_system), "erofs"},
    {make_sys_error_code(std::errc::resource_deadlock_would_occur), "edeadlk"},
    {make_sys_error_code(std::errc::resource_unavailable_try_again), "eagain"},
    {make_sys_error_code(std::errc::result_out_of_range), "erange"},
    {make_sys_error_code(std::errc::state_not_recoverable), "enotrecoverable"},
    {make_sys_error_code(std::errc::stream_timeout), "etime"},
    {make_sys_error_code(std::errc::text_file_busy), "etxtbsy"},
    {make_sys_error_code(std::errc::timed_out), "etimedout"},
    {make_sys_error_code(std::errc::too_many_files_open_in_system), "enfile"},
    {make_sys_error_code(std::errc::too_many_files_open), "emfile"},
    {make_sys_error_code(std::errc::too_many_links), "emlink"},
    {make_sys_error_code(std::errc::too_many_symbolic_link_levels), "eloop"},
    {make_sys_error_code(std::errc::value_too_large), "eoverflow"},
    {make_sys_error_code(std::errc::wrong_protocol_type), "eprototype"}};
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

    ErlNifEnv *env;
    Env localEnv;
    ErlNifPid reqPid;
    reqid_t reqId;
    helper_ptr helperObj;
    helper_ctx_ptr helperCTX;
};

/**
 * Runs given function and returns result or error term.
 */
template <class T> ERL_NIF_TERM handle_errors(ErlNifEnv *env, const T &fun)
{
    try {
        return fun();
    }
    catch (nifpp::badarg &) {
        return enif_make_badarg(env);
    }
    catch (std::system_error &e) {
        return nifpp::make(
            env, std::make_tuple(error, nifpp::str_atom{e.code().message()}));
    }
    catch (std::exception &e) {
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
template <class... T> void handle_result(NifCTX ctx, error_t e, T... value)
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
    auto helperArgs = nifpp::get<helper_args_t>(env, argv[1]);
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

ERL_NIF_TERM set_user_ctx(ErlNifEnv *env, helper_ctx_ptr ctx,
    std::unordered_map<std::string, std::string> args)
{
    return handle_errors(env, [&]() {
        ctx->setUserCTX(args);

        return nifpp::make(env, ok);
    });
}

ERL_NIF_TERM get_flag_value(
    ErlNifEnv *env, helper_ctx_ptr ctx, nifpp::str_atom flag)
{
    auto searchResult = atom_to_flag.left.find(flag);
    if (searchResult != atom_to_flag.left.end())
        return nifpp::make(env, ctx->getFlagValue(searchResult->second));
    throw nifpp::badarg();
}

ERL_NIF_TERM new_helper_ctx(ErlNifEnv *env, helper_ptr helperObj)
{
    auto ctx = helperObj->createCTX();
    auto ctx_resource = nifpp::construct_resource<helper_ctx_ptr>(ctx);
    return nifpp::make(env, std::make_tuple(ok, ctx_resource));
}

ERL_NIF_TERM get_user_ctx(ErlNifEnv *env, helper_ctx_ptr ctx)
{
    return nifpp::make(env, std::make_tuple(ok, ctx->getUserCTX()));
}

ERL_NIF_TERM get_flags(ErlNifEnv *env, helper_ctx_ptr ctx)
{
    std::vector<nifpp::str_atom> atomFlags;
    auto flags = ctx->getFlags();
    for (const auto &flag : flags) {
        auto searchResult = atom_to_flag.right.find(flag);
        if (searchResult != atom_to_flag.right.end()) {
            atomFlags.push_back(searchResult->second);
        }
        else {
            throw nifpp::badarg();
        }
    }

    return nifpp::make(env, std::make_tuple(ok, atomFlags));
}

ERL_NIF_TERM set_flags(
    ErlNifEnv *env, helper_ctx_ptr ctx, std::vector<nifpp::str_atom> flagAtoms)
{
    std::vector<one::helpers::IStorageHelperCTX::Flag> flags;
    for (const auto &flagAtom : flagAtoms) {
        auto searchResult = atom_to_flag.left.find(flagAtom);
        if (searchResult != atom_to_flag.left.end()) {
            flags.push_back(searchResult->second);
        }
        else {
            throw nifpp::badarg();
        }
    }

    ctx->setFlags(std::move(flags));

    return nifpp::make(env, ok);
}

ERL_NIF_TERM getattr(NifCTX ctx, const std::string file)
{
    ctx.helperObj->ash_getattr(
        ctx.helperCTX, file, [=](struct stat statbuf, error_t e) {
            handle_result(ctx, e, statbuf);
        });

    return nifpp::make(ctx.env, std::make_tuple(ok, ctx.reqId));
}

ERL_NIF_TERM access(NifCTX ctx, const std::string file, const int mask)
{
    ctx.helperObj->ash_access(
        ctx.helperCTX, file, mask, [=](error_t e) { handle_result(ctx, e); });

    return nifpp::make(ctx.env, std::make_tuple(ok, ctx.reqId));
}

ERL_NIF_TERM mknod(
    NifCTX ctx, const std::string file, const mode_t mode, const dev_t dev)
{
    ctx.helperObj->ash_mknod(ctx.helperCTX, file, mode, dev,
        [=](error_t e) { handle_result(ctx, e); });

    return nifpp::make(ctx.env, std::make_tuple(ok, ctx.reqId));
}

ERL_NIF_TERM mkdir(NifCTX ctx, const std::string file, const mode_t mode)
{
    ctx.helperObj->ash_mkdir(
        ctx.helperCTX, file, mode, [=](error_t e) { handle_result(ctx, e); });

    return nifpp::make(ctx.env, std::make_tuple(ok, ctx.reqId));
}

ERL_NIF_TERM unlink(NifCTX ctx, const std::string file)
{
    ctx.helperObj->ash_unlink(
        ctx.helperCTX, file, [=](error_t e) { handle_result(ctx, e); });

    return nifpp::make(ctx.env, std::make_tuple(ok, ctx.reqId));
}

ERL_NIF_TERM rmdir(NifCTX ctx, const std::string file)
{
    ctx.helperObj->ash_rmdir(
        ctx.helperCTX, file, [=](error_t e) { handle_result(ctx, e); });

    return nifpp::make(ctx.env, std::make_tuple(ok, ctx.reqId));
}

ERL_NIF_TERM symlink(NifCTX ctx, const std::string from, const std::string to)
{
    ctx.helperObj->ash_symlink(
        ctx.helperCTX, from, to, [=](error_t e) { handle_result(ctx, e); });

    return nifpp::make(ctx.env, std::make_tuple(ok, ctx.reqId));
}

ERL_NIF_TERM rename(NifCTX ctx, const std::string from, const std::string to)
{
    ctx.helperObj->ash_rename(
        ctx.helperCTX, from, to, [=](error_t e) { handle_result(ctx, e); });

    return nifpp::make(ctx.env, std::make_tuple(ok, ctx.reqId));
}

ERL_NIF_TERM link(NifCTX ctx, const std::string from, const std::string to)
{
    ctx.helperObj->ash_link(
        ctx.helperCTX, from, to, [=](error_t e) { handle_result(ctx, e); });

    return nifpp::make(ctx.env, std::make_tuple(ok, ctx.reqId));
}

ERL_NIF_TERM chmod(NifCTX ctx, const std::string file, const mode_t mode)
{
    ctx.helperObj->ash_chmod(
        ctx.helperCTX, file, mode, [=](error_t e) { handle_result(ctx, e); });

    return nifpp::make(ctx.env, std::make_tuple(ok, ctx.reqId));
}

ERL_NIF_TERM chown(
    NifCTX ctx, const std::string file, const int uid, const int gid)
{
    ctx.helperObj->ash_chown(ctx.helperCTX, file, uid, gid,
        [=](error_t e) { handle_result(ctx, e); });

    return nifpp::make(ctx.env, std::make_tuple(ok, ctx.reqId));
}

ERL_NIF_TERM truncate(NifCTX ctx, const std::string file, const off_t size)
{
    ctx.helperObj->ash_truncate(
        ctx.helperCTX, file, size, [=](error_t e) { handle_result(ctx, e); });

    return nifpp::make(ctx.env, std::make_tuple(ok, ctx.reqId));
}

ERL_NIF_TERM open(NifCTX ctx, const std::string file)
{
    ctx.helperObj->ash_open(ctx.helperCTX, file,
        [=](int fh, error_t e) { handle_result(ctx, e, fh); });

    return nifpp::make(ctx.env, std::make_tuple(ok, ctx.reqId));
}

ERL_NIF_TERM read(NifCTX ctx, const std::string file, off_t offset, size_t size)
{
    auto buf = std::make_shared<std::vector<char>>(size);
    ctx.helperObj->ash_read(ctx.helperCTX, file,
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
    ctx.helperObj->ash_write(ctx.helperCTX, file,
        asio::const_buffer(sData->data(), sData->size()), offset,
        [ctx, file, offset, sData](int size, error_t e) {
            handle_result(ctx, e, size);
        });

    return nifpp::make(ctx.env, std::make_tuple(ok, ctx.reqId));
}

ERL_NIF_TERM release(NifCTX ctx, const std::string file)
{
    ctx.helperObj->ash_release(
        ctx.helperCTX, file, [=](error_t e) { handle_result(ctx, e); });

    return nifpp::make(ctx.env, std::make_tuple(ok, ctx.reqId));
}

ERL_NIF_TERM flush(NifCTX ctx, const std::string file)
{
    ctx.helperObj->ash_flush(
        ctx.helperCTX, file, [=](error_t e) { handle_result(ctx, e); });

    return nifpp::make(ctx.env, std::make_tuple(ok, ctx.reqId));
}

ERL_NIF_TERM fsync(NifCTX ctx, const std::string file, const int isdatasync)
{
    ctx.helperObj->ash_fsync(ctx.helperCTX, file, isdatasync,
        [=](error_t e) { handle_result(ctx, e); });

    return nifpp::make(ctx.env, std::make_tuple(ok, ctx.reqId));
}

} // namespace

extern "C" {

static int load(ErlNifEnv *env, void **priv_data, ERL_NIF_TERM load_info)
{
    for (auto i = 0; i < 100; ++i) {
        application.workers.push_back(
            std::thread([]() { application.ioService.run(); }));
    }

    return !(nifpp::register_resource<helper_ptr>(env, nullptr, "helper_ptr") &&
        nifpp::register_resource<helper_ctx_ptr>(
                 env, nullptr, "helper_ctx_ptr") &&
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

static ERL_NIF_TERM sh_set_user_ctx(
    ErlNifEnv *env, int argc, const ERL_NIF_TERM argv[])
{
    return noctx_wrap(set_user_ctx, env, argv);
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

static ErlNifFunc nif_funcs[] = {{"getattr", 3, sh_getattr},
    {"access", 4, sh_access}, {"mknod", 5, sh_mknod}, {"mkdir", 4, sh_mkdir},
    {"unlink", 3, sh_unlink}, {"rmdir", 3, sh_rmdir},
    {"symlink", 4, sh_symlink}, {"rename", 4, sh_rename}, {"link", 4, sh_link},
    {"chmod", 4, sh_chmod}, {"chown", 5, sh_chown},
    {"truncate", 4, sh_truncate}, {"open", 3, sh_open}, {"read", 5, sh_read},
    {"write", 5, sh_write}, {"release", 3, sh_release}, {"flush", 3, sh_flush},
    {"fsync", 4, sh_fsync}, {"set_flags", 2, sh_set_flags},
    {"get_flags", 1, sh_get_flags}, {"username_to_uid", 1, username_to_uid},
    {"groupname_to_gid", 1, groupname_to_gid},
    {"new_helper_obj", 2, new_helper_obj},
    {"new_helper_ctx", 1, sh_new_helper_ctx},
    {"set_user_ctx", 2, sh_set_user_ctx}, {"get_user_ctx", 1, sh_get_user_ctx},
    {"get_flag_value", 2, sh_get_flag_value}};

ERL_NIF_INIT(helpers_nif, nif_funcs, load, NULL, NULL, NULL);

} // extern C
