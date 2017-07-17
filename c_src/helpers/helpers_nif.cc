#include "../nifpp.h"
#include "helpers/storageHelperCreator.h"

#include <asio.hpp>
#include <asio/executor_work.hpp>

#include <chrono>
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

using helper_ptr = one::helpers::StorageHelperPtr;
using file_handle_ptr = one::helpers::FileHandlePtr;
using reqid_t = std::tuple<int, int, int>;
using helper_args_t = std::unordered_map<folly::fbstring, folly::fbstring>;

/**
 * Static resource holder.
 */
struct HelpersNIF {
    struct HelperIOService {
        asio::io_service service;
        asio::executor_work<asio::io_service::executor_type> work =
            asio::make_work(service);
        folly::fbvector<std::thread> workers;
    };

    HelpersNIF(std::unordered_map<folly::fbstring, folly::fbstring> args)
    {
        using namespace one::helpers;

        bufferingEnabled = (args["buffer_helpers"] == "true");

        for (const auto &entry :
            std::unordered_map<folly::fbstring, folly::fbstring>(
                {{CEPH_HELPER_NAME, "ceph_helper_threads_number"},
                    {POSIX_HELPER_NAME, "posix_helper_threads_number"},
                    {S3_HELPER_NAME, "s3_helper_threads_number"},
                    {SWIFT_HELPER_NAME, "swift_helper_threads_number"},
                    {GLUSTERFS_HELPER_NAME,
                        "glusterfs_helper_threads_number"}})) {
            auto threads = std::stoul(args[entry.second].toStdString());
            services.emplace(entry.first, std::make_unique<HelperIOService>());
            auto &service = services[entry.first]->service;
            auto &workers = services[entry.first]->workers;
            for (std::size_t i = 0; i < threads; ++i) {
                workers.push_back(std::thread([&]() { service.run(); }));
            }
        }

        SHCreator = std::make_unique<one::helpers::StorageHelperCreator>(
            services[CEPH_HELPER_NAME]->service,
            services[POSIX_HELPER_NAME]->service,
            services[S3_HELPER_NAME]->service,
            services[SWIFT_HELPER_NAME]->service,
            services[GLUSTERFS_HELPER_NAME]->service,
            std::stoul(args["buffer_scheduler_threads_number"].toStdString()),
            buffering::BufferLimits{
                std::stoul(args["read_buffer_min_size"].toStdString()),
                std::stoul(args["read_buffer_max_size"].toStdString()),
                std::chrono::seconds{std::stoul(
                    args["read_buffer_prefetch_duration"].toStdString())},
                std::stoul(args["write_buffer_min_size"].toStdString()),
                std::stoul(args["write_buffer_max_size"].toStdString()),
                std::chrono::seconds{std::stoul(
                    args["write_buffer_flush_delay"].toStdString())}});

        umask(0);
    }

    ~HelpersNIF()
    {
        for (auto &service : services) {
            service.second->service.stop();
            for (auto &worker : service.second->workers) {
                worker.join();
            }
        }
    }

    bool bufferingEnabled = false;
    std::unordered_map<folly::fbstring, std::unique_ptr<HelperIOService>>
        services;
    std::unique_ptr<one::helpers::StorageHelperCreator> SHCreator;
};

std::unique_ptr<HelpersNIF> application;

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

one::helpers::FlagsSet translateFlags(folly::fbvector<nifpp::str_atom> atoms)
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

template <class T> std::error_code make_sys_error_code(T code)
{
    return std::error_code(static_cast<int>(code), std::system_category());
}

/**
 * @defgroup Errors Maps translating std::error_code to corresponding
 *           POSIX-like code description as atom.
 * @{
 */
std::map<std::error_code, nifpp::str_atom> error_to_atom = {
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
    operator ErlNifEnv *() const { return env.get(); }

    ErlNifEnv *get() const { return env.get(); }

private:
    std::shared_ptr<ErlNifEnv> env;
};

/**
 * NIF context holder for all common operations.
 */
struct NifCTX {
    NifCTX(ErlNifEnv *env)
        : env(env)
        , reqId(std::make_tuple(dist(gen), dist(gen), dist(gen)))
    {
        enif_self(env, &reqPid);
    }

    template <typename T> int send(T &&value) const
    {
        return enif_send(nullptr, &reqPid, localEnv,
            nifpp::make(
                localEnv, std::make_tuple(reqId, std::forward<T>(value))));
    }

    ErlNifEnv *env;
    Env localEnv;
    ErlNifPid reqPid;
    reqid_t reqId;

private:
    static thread_local std::random_device rd;
    static thread_local std::default_random_engine gen;
    static thread_local std::uniform_int_distribution<int> dist;
};
thread_local std::random_device NifCTX::rd{};
thread_local std::default_random_engine NifCTX::gen{NifCTX::rd()};
thread_local std::uniform_int_distribution<int> NifCTX::dist{};

/**
 * Runs given function and returns result or error term.
 */
template <class T> ERL_NIF_TERM handle_errors(ErlNifEnv *env, T &&fun)
{
    try {
        return std::forward<T>(fun)();
    }
    catch (const nifpp::badarg &) {
        return enif_make_badarg(env);
    }
    catch (const std::system_error &e) {
        return nifpp::make(
            env, std::make_tuple(error, nifpp::str_atom{e.code().message()}));
    }
    catch (const std::exception &e) {
        return nifpp::make(
            env, std::make_tuple(error, folly::fbstring{e.what()}));
    }
}

template <typename... Args, std::size_t... I>
ERL_NIF_TERM wrap_helper(ERL_NIF_TERM (*fun)(NifCTX ctx, Args...),
    ErlNifEnv *env, const ERL_NIF_TERM args[], std::index_sequence<I...>)
{
    return handle_errors(env,
        [&]() { return fun(NifCTX{env}, nifpp::get<Args>(env, args[I])...); });
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
 * Handles struct stat value from helpers and sends it to requesting process.
 */
void handle_value(const NifCTX &ctx, struct stat s)
{
    auto record =
        std::make_tuple(nifpp::str_atom("statbuf"), s.st_dev, s.st_ino,
            s.st_mode, s.st_nlink, s.st_uid, s.st_gid, s.st_rdev, s.st_size,
            s.st_atime, s.st_mtime, s.st_ctime, s.st_blksize, s.st_blocks);

    ctx.send(std::make_tuple(ok, std::move(record)));
}

/**
 * Constructs a resource from open file handle and sends it to requesting
 * process.
 */
void handle_value(const NifCTX &ctx, file_handle_ptr handle)
{
    auto resource =
        nifpp::construct_resource<file_handle_ptr>(std::move(handle));

    ctx.send(std::make_tuple(ok, std::move(resource)));
}

/**
 * Handles generic result from helpers and sends it to requesting process.
 */
template <class T> void handle_value(const NifCTX &ctx, T &&response)
{
    ctx.send(std::make_tuple(ok, std::forward<T>(response)));
}

/**
 * Handles void value from helpers and send it to requesting process.
 */
void handle_value(const NifCTX &ctx, folly::Unit) { ctx.send(ok); }

/**
 * Handles helpers callback result.
 */
template <class T> void handle_result(NifCTX ctx, folly::Future<T> future)
{
    future.then([ctx](T &&value) { handle_value(ctx, std::move(value)); })
        .onError([ctx](const std::system_error &e) {
            auto it = error_to_atom.find(e.code());
            nifpp::str_atom reason{e.code().message()};
            if (it != error_to_atom.end())
                reason = it->second;

            ctx.send(std::make_tuple(error, reason));
        })
        .onError([ctx](const std::exception &e) {
            nifpp::str_atom reason{e.what()};
            ctx.send(std::make_tuple(error, reason));
        });
}

/*********************************************************************
*
*                          WRAPPERS (NIF based)
*       All functions below are described in helpers_nif.erl
*
*********************************************************************/

ERL_NIF_TERM get_handle(ErlNifEnv *env, int argc, const ERL_NIF_TERM argv[])
{
    auto name = nifpp::get<folly::fbstring>(env, argv[0]);
    auto params = nifpp::get<helper_args_t>(env, argv[1]);
    auto helper = application->SHCreator->getStorageHelper(
        name, params, application->bufferingEnabled);
    auto resource = nifpp::construct_resource<helper_ptr>(helper);

    return nifpp::make(env, std::make_tuple(ok, resource));
}

ERL_NIF_TERM getattr(NifCTX ctx, helper_ptr helper, folly::fbstring file)
{
    handle_result(ctx, helper->getattr(file));
    return nifpp::make(ctx.env, std::make_tuple(ok, ctx.reqId));
}

ERL_NIF_TERM readdir(NifCTX ctx, helper_ptr helper, folly::fbstring file,
    const off_t offset, const size_t count)
{
    handle_result(ctx, helper->readdir(file, offset, count));
    return nifpp::make(ctx.env, std::make_tuple(ok, ctx.reqId));
}

ERL_NIF_TERM access(
    NifCTX ctx, helper_ptr helper, folly::fbstring file, const int mask)
{
    handle_result(ctx, helper->access(file, mask));
    return nifpp::make(ctx.env, std::make_tuple(ok, ctx.reqId));
}

ERL_NIF_TERM mknod(NifCTX ctx, helper_ptr helper, folly::fbstring file,
    const mode_t mode, folly::fbvector<nifpp::str_atom> flags, const dev_t dev)
{
    handle_result(
        ctx, helper->mknod(file, mode, translateFlags(std::move(flags)), dev));

    return nifpp::make(ctx.env, std::make_tuple(ok, ctx.reqId));
}

ERL_NIF_TERM mkdir(
    NifCTX ctx, helper_ptr helper, folly::fbstring file, const mode_t mode)
{
    handle_result(ctx, helper->mkdir(file, mode));
    return nifpp::make(ctx.env, std::make_tuple(ok, ctx.reqId));
}

ERL_NIF_TERM unlink(NifCTX ctx, helper_ptr helper, folly::fbstring file)
{
    handle_result(ctx, helper->unlink(file));
    return nifpp::make(ctx.env, std::make_tuple(ok, ctx.reqId));
}

ERL_NIF_TERM rmdir(NifCTX ctx, helper_ptr helper, folly::fbstring file)
{
    handle_result(ctx, helper->rmdir(file));
    return nifpp::make(ctx.env, std::make_tuple(ok, ctx.reqId));
}

ERL_NIF_TERM symlink(
    NifCTX ctx, helper_ptr helper, folly::fbstring from, folly::fbstring to)
{
    handle_result(ctx, helper->symlink(from, to));
    return nifpp::make(ctx.env, std::make_tuple(ok, ctx.reqId));
}

ERL_NIF_TERM rename(
    NifCTX ctx, helper_ptr helper, folly::fbstring from, folly::fbstring to)
{
    handle_result(ctx, helper->rename(from, to));
    return nifpp::make(ctx.env, std::make_tuple(ok, ctx.reqId));
}

ERL_NIF_TERM link(
    NifCTX ctx, helper_ptr helper, folly::fbstring from, folly::fbstring to)
{
    handle_result(ctx, helper->link(from, to));
    return nifpp::make(ctx.env, std::make_tuple(ok, ctx.reqId));
}

ERL_NIF_TERM chmod(
    NifCTX ctx, helper_ptr helper, folly::fbstring file, const mode_t mode)
{
    handle_result(ctx, helper->chmod(file, mode));
    return nifpp::make(ctx.env, std::make_tuple(ok, ctx.reqId));
}

ERL_NIF_TERM chown(NifCTX ctx, helper_ptr helper, folly::fbstring file,
    const int uid, const int gid)
{
    handle_result(ctx, helper->chown(file, uid, gid));
    return nifpp::make(ctx.env, std::make_tuple(ok, ctx.reqId));
}

ERL_NIF_TERM truncate(
    NifCTX ctx, helper_ptr helper, folly::fbstring file, const off_t size)
{
    handle_result(ctx, helper->truncate(file, size));
    return nifpp::make(ctx.env, std::make_tuple(ok, ctx.reqId));
}

ERL_NIF_TERM open(NifCTX ctx, helper_ptr helper, folly::fbstring file,
    folly::fbvector<nifpp::str_atom> flags)
{
    handle_result(
        ctx, helper->open(file, translateFlags(std::move(flags)), {}));
    return nifpp::make(ctx.env, std::make_tuple(ok, ctx.reqId));
}

ERL_NIF_TERM read(NifCTX ctx, file_handle_ptr handle, off_t offset, size_t size)
{
    handle_result(ctx, handle->read(offset, size));
    return nifpp::make(ctx.env, std::make_tuple(ok, ctx.reqId));
}

ERL_NIF_TERM write(NifCTX ctx, file_handle_ptr handle, const off_t offset,
    folly::IOBufQueue buf)
{
    handle_result(ctx, handle->write(offset, std::move(buf)));
    return nifpp::make(ctx.env, std::make_tuple(ok, ctx.reqId));
}

ERL_NIF_TERM release(NifCTX ctx, file_handle_ptr handle)
{
    handle_result(ctx, handle->release());
    return nifpp::make(ctx.env, std::make_tuple(ok, ctx.reqId));
}

ERL_NIF_TERM flush(NifCTX ctx, file_handle_ptr handle)
{
    handle_result(ctx, handle->flush());
    return nifpp::make(ctx.env, std::make_tuple(ok, ctx.reqId));
}

ERL_NIF_TERM fsync(NifCTX ctx, file_handle_ptr handle, const int isdatasync)
{
    handle_result(ctx, handle->fsync(isdatasync));
    return nifpp::make(ctx.env, std::make_tuple(ok, ctx.reqId));
}

} // namespace

extern "C" {

static int load(ErlNifEnv *env, void **priv_data, ERL_NIF_TERM load_info)
{
    auto args =
        nifpp::get<std::unordered_map<folly::fbstring, folly::fbstring>>(
            env, load_info);
    application = std::make_unique<HelpersNIF>(std::move(args));

    return !(nifpp::register_resource<helper_ptr>(env, nullptr, "helper_ptr") &&
        nifpp::register_resource<file_handle_ptr>(
            env, nullptr, "file_handle_ptr"));
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

static ErlNifFunc nif_funcs[] = {{"get_handle", 2, get_handle},
    {"getattr", 2, sh_getattr}, {"access", 3, sh_access},
    {"readdir", 4, sh_readdir}, {"mknod", 5, sh_mknod}, {"mkdir", 3, sh_mkdir},
    {"unlink", 2, sh_unlink}, {"rmdir", 2, sh_rmdir},
    {"symlink", 3, sh_symlink}, {"rename", 3, sh_rename}, {"link", 3, sh_link},
    {"chmod", 3, sh_chmod}, {"chown", 4, sh_chown},
    {"truncate", 3, sh_truncate}, {"open", 3, sh_open}, {"read", 3, sh_read},
    {"write", 3, sh_write}, {"release", 1, sh_release}, {"flush", 1, sh_flush},
    {"fsync", 2, sh_fsync}};

ERL_NIF_INIT(helpers_nif, nif_funcs, load, NULL, NULL, NULL);

} // extern C
