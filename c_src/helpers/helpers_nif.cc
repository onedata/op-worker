#include "../nifpp.h"
#include "helpers/storageHelperFactory.h"

#include <asio.hpp>
#include <asio/executor_work.hpp>

#include <map>
#include <memory>
#include <random>
#include <sstream>
#include <string>
#include <system_error>
#include <tuple>
#include <unordered_map>
#include <vector>

#include <grp.h>
#include <pwd.h>
#include <sys/stat.h>
#include <sys/types.h>

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
    class HelperIOService {
    public:
        asio::io_service service;
        asio::executor_work<asio::io_service::executor_type> work =
            asio::make_work(service);
        std::vector<std::thread> workers;
    };

    std::unordered_map<std::string, std::unique_ptr<HelperIOService>>
        helperServices;

    std::unique_ptr<one::helpers::StorageHelperFactory> SHFactory;

    HelpersNIF()
    {
        helperServices.emplace("AmazonS3", std::make_unique<HelperIOService>());
        helperServices.emplace("Ceph", std::make_unique<HelperIOService>());
        helperServices.emplace("DirectIO", std::make_unique<HelperIOService>());
        helperServices.emplace("Swift", std::make_unique<HelperIOService>());

        SHFactory = std::make_unique<one::helpers::StorageHelperFactory>(
            helperServices["Ceph"]->service,
            helperServices["DirectIO"]->service,
            helperServices["AmazonS3"]->service,
            helperServices["Swift"]->service);
        umask(0);
    }

    ~HelpersNIF()
    {
        for (auto &helperService : helperServices) {
            helperService.second->service.stop();
            for (auto &th : helperService.second->workers) {
                th.join();
            }
        }
    }

    /**
     * Adjusts number of threads held by IO service. Allows only to increase
     * size of thread pool.
     * @param ioService reference to IO service which thread pool should be
     * adjusted
     * @param threads reference to pool of threads held by IO service
     * @param number number of threads that IO service should hold
     * @return true if number of IO service threads has been adjusted
     * (increased) otherwise false.
     */
    bool adjust_io_service_threads(asio::io_service &ioService,
        std::vector<std::thread> &threads, std::size_t number)
    {
        if (threads.size() > number)
            return false;

        for (std::size_t i = 0; i < threads.size() - number; ++i) {
            threads.push_back(std::thread([&]() { ioService.run(); }));
        }

        return true;
    }

} application;

namespace {

/**
 * @defgroup ModeTranslators Maps translating nifpp::str_atom into corresponding
 *           POSIX open mode / flag.
 * @{
 */
const std::unordered_map<nifpp::str_atom, one::helpers::Flag> atom_to_flag{
    {"O_NONBLOCK", one::helpers::Flag::NONBLOCK},
    {"O_APPEND", one::helpers::Flag::APPEND},
    {"O_ASYNC", one::helpers::Flag::ASYNC},
    {"O_FSYNC", one::helpers::Flag::FSYNC},
    {"O_NOFOLLOW", one::helpers::Flag::NOFOLLOW},
    {"O_CREAT", one::helpers::Flag::CREAT},
    {"O_TRUNC", one::helpers::Flag::TRUNC},
    {"O_EXCL", one::helpers::Flag::EXCL},
    {"O_RDONLY", one::helpers::Flag::RDONLY},
    {"O_WRONLY", one::helpers::Flag::WRONLY},
    {"O_RDWR", one::helpers::Flag::RDWR},
    {"S_IFREG", one::helpers::Flag::IFREG},
    {"S_IFCHR", one::helpers::Flag::IFCHR},
    {"S_IFBLK", one::helpers::Flag::IFBLK},
    {"S_IFIFO", one::helpers::Flag::IFIFO},
    {"S_IFSOCK", one::helpers::Flag::IFSOCK}};

one::helpers::FlagsSet translateFlags(std::vector<nifpp::str_atom> atoms)
{
    one::helpers::FlagsSet flags;

    for (const auto &atom : atoms) {
        auto result = atom_to_flag.find(atom);
        if (result != atom_to_flag.end()) {
            flags.insert(result->second);
        }
        else {
            throw std::system_error{
                std::make_error_code(std::errc::invalid_argument)};
        }
    }

    return flags;
}
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

ERL_NIF_TERM set_threads_number(
    ErlNifEnv *env, std::unordered_map<std::string, std::size_t> args)
{
    return handle_errors(env, [&]() {
        std::vector<std::string> names{"AmazonS3", "Ceph", "DirectIO", "Swift"};
        for (const auto &name : names) {
            auto result = args.find(name);
            if (result != args.end()) {
                auto &service = application.helperServices[name]->service;
                auto &workers = application.helperServices[name]->workers;
                if (!application.adjust_io_service_threads(
                        service, workers, result->second)) {
                    return nifpp::make(
                        env, std::make_tuple(error,
                                 std::make_tuple(nifpp::str_atom(
                                                     "wrong_thread_number"),
                                                 name, result->second)));
                }
            }
        }

        return nifpp::make(env, ok);
    });
}

ERL_NIF_TERM new_helper_obj(ErlNifEnv *env, int argc, const ERL_NIF_TERM argv[])
{
    auto helperName = nifpp::get<std::string>(env, argv[0]);
    auto helperArgs = nifpp::get<helper_args_t>(env, argv[1]);
    auto helperObj =
        application.SHFactory->getStorageHelper(helperName, helperArgs);
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

ERL_NIF_TERM new_helper_ctx(ErlNifEnv *env, helper_ptr helperObj)
{
    auto parameters = std::unordered_map<std::string, std::string>({});
    auto ctx = helperObj->createCTX(std::move(parameters));
    auto ctx_resource = nifpp::construct_resource<helper_ctx_ptr>(ctx);
    return nifpp::make(env, std::make_tuple(ok, ctx_resource));
}

ERL_NIF_TERM get_user_ctx(ErlNifEnv *env, helper_ctx_ptr ctx)
{
    return nifpp::make(env, std::make_tuple(ok, ctx->getUserCTX()));
}

ERL_NIF_TERM readdir(NifCTX ctx, const std::string file, const int offset, const int count)
{
    ctx.helperObj->ash_readdir(
        ctx.helperCTX, file, offset, count, [=](const std::vector<std::string> & entries, error_t e) {
            handle_result(ctx, e, entries);
        });

    return nifpp::make(ctx.env, std::make_tuple(ok, ctx.reqId));
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

ERL_NIF_TERM mknod(NifCTX ctx, const std::string file, const mode_t mode,
    std::vector<nifpp::str_atom> flags, const dev_t dev)
{
    ctx.helperObj->ash_mknod(ctx.helperCTX, file, mode,
        translateFlags(std::move(flags)), dev,
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

ERL_NIF_TERM open(
    NifCTX ctx, const std::string file, std::vector<nifpp::str_atom> flags)
{
    ctx.helperObj->ash_open(ctx.helperCTX, file,
        translateFlags(std::move(flags)),
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
    return !(nifpp::register_resource<helper_ptr>(env, nullptr, "helper_ptr") &&
        nifpp::register_resource<helper_ctx_ptr>(
                 env, nullptr, "helper_ctx_ptr") &&
        nifpp::register_resource<fuse_file_info>(
                 env, nullptr, "fuse_file_info"));
}

static ERL_NIF_TERM sh_set_threads_number(
    ErlNifEnv *env, int argc, const ERL_NIF_TERM argv[])
{
    return noctx_wrap(set_threads_number, env, argv);
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

static ERL_NIF_TERM sh_readdir(
    ErlNifEnv *env, int argc, const ERL_NIF_TERM argv[])
{
    return wrap(readdir, env, argv);
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
    {"set_threads_number", 1, sh_set_threads_number},
    {"getattr", 3, sh_getattr}, {"access", 4, sh_access}, {"readdir", 5, sh_readdir},
    {"mknod", 6, sh_mknod}, {"mkdir", 4, sh_mkdir}, {"unlink", 3, sh_unlink},
    {"rmdir", 3, sh_rmdir}, {"symlink", 4, sh_symlink},
    {"rename", 4, sh_rename}, {"link", 4, sh_link}, {"chmod", 4, sh_chmod},
    {"chown", 5, sh_chown}, {"truncate", 4, sh_truncate}, {"open", 4, sh_open},
    {"read", 5, sh_read}, {"write", 5, sh_write}, {"release", 3, sh_release},
    {"flush", 3, sh_flush}, {"fsync", 4, sh_fsync},
    {"username_to_uid", 1, username_to_uid},
    {"groupname_to_gid", 1, groupname_to_gid},
    {"new_helper_obj", 2, new_helper_obj},
    {"new_helper_ctx", 1, sh_new_helper_ctx},
    {"set_user_ctx", 2, sh_set_user_ctx}, {"get_user_ctx", 1, sh_get_user_ctx}};

ERL_NIF_INIT(helpers_nif, nif_funcs, load, NULL, NULL, NULL);

} // extern C
