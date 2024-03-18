#include "../nifpp.h"
#include "helpers/init.h"
#include "helpers/storageHelperCreator.h"
#include "monitoring/monitoring.h"

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

#include <folly/executors/IOThreadPoolExecutor.h>

namespace {
/**
 * @defgroup StaticAtoms Statically created atoms for ease of usage.
 * @{
 */
nifpp::str_atom ok {"ok"};
nifpp::str_atom error {"error"};
/** @} */

using helper_ptr = one::helpers::StorageHelperPtr;
using file_handle_ptr = one::helpers::FileHandlePtr;
using reqid_t = std::tuple<int, int, int>;
using helper_args_t = std::unordered_map<folly::fbstring, folly::fbstring>;

/**
 * Static resource holder.
 */
struct HelpersNIF {
    HelpersNIF(std::unordered_map<folly::fbstring, folly::fbstring> args)
    {
        using namespace one::helpers;

        bufferingEnabled = (args["buffer_helpers"] == "true");

        for (const auto &entry : std::unordered_map<folly::fbstring,
                 std::pair<folly::fbstring, folly::fbstring>>(
                 {{CEPH_HELPER_NAME, {"ceph_helper_threads_number", "ceph_t"}},
                     {CEPHRADOS_HELPER_NAME,
                         {"cephrados_helper_threads_number", "crados_t"}},
                     {POSIX_HELPER_NAME,
                         {"posix_helper_threads_number", "posix_t"}},
                     {S3_HELPER_NAME, {"s3_helper_threads_number", "s3_t"}},
                     {SWIFT_HELPER_NAME,
                         {"swift_helper_threads_number", "swift_t"}},
                     {GLUSTERFS_HELPER_NAME,
                         {"glusterfs_helper_threads_number", "gluster_t"}},
                     {WEBDAV_HELPER_NAME,
                         {"webdav_helper_threads_number", "webdav_t"}},
                     {XROOTD_HELPER_NAME,
                         {"xrootd_helper_threads_number", "xrootd_t"}},
                     {NFS_HELPER_NAME,
                         {"nfs_helper_threads_number", "nfs_t"}},
                     {NULL_DEVICE_HELPER_NAME,
                         {"nulldevice_helper_threads_number", "nulldev_t"}}})) {
            auto threadNumber =
                std::stoul(args[entry.second.first].toStdString());
            executors.emplace(entry.first,
                std::make_shared<folly::IOThreadPoolExecutor>(threadNumber,
                    std::make_shared<StorageWorkerFactory>(
                        entry.second.second)));
        }

        SHCreator = std::make_unique<one::helpers::StorageHelperCreator<void>>(
            executors[CEPH_HELPER_NAME], executors[CEPHRADOS_HELPER_NAME],
            executors[POSIX_HELPER_NAME], executors[S3_HELPER_NAME],
            executors[SWIFT_HELPER_NAME], executors[GLUSTERFS_HELPER_NAME],
            executors[WEBDAV_HELPER_NAME], executors[XROOTD_HELPER_NAME],
            executors[NFS_HELPER_NAME], executors[NULL_DEVICE_HELPER_NAME],
            std::stoul(args["buffer_scheduler_threads_number"].toStdString()),
            buffering::BufferLimits {
                std::stoul(args["read_buffer_min_size"].toStdString()),
                std::stoul(args["read_buffer_max_size"].toStdString()),
                std::chrono::seconds {std::stoul(
                    args["read_buffer_prefetch_duration"].toStdString())},
                std::stoul(args["write_buffer_min_size"].toStdString()),
                std::stoul(args["write_buffer_max_size"].toStdString()),
                std::chrono::seconds {std::stoul(
                    args["write_buffer_flush_delay"].toStdString())}});

        umask(0);
    }

    ~HelpersNIF()
    {
        for (auto &executor : executors) {
            executor.second->stop();
        }
    }

    bool bufferingEnabled{false};
    std::unordered_map<folly::fbstring,
        std::shared_ptr<folly::IOThreadPoolExecutor>>
        executors;
    std::unique_ptr<one::helpers::StorageHelperCreator<void>> SHCreator;
};

std::unique_ptr<HelpersNIF> application;

namespace {

/**
 * @defgroup ModeTranslators Maps translating nifpp::str_atom into corresponding
 *           POSIX open mode / flag.
 * @{
 */
const std::unordered_map<nifpp::str_atom, one::helpers::Flag> atom_to_flag {
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
            throw std::system_error {
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
    {make_sys_error_code(std::errc::wrong_protocol_type), "eprototype"},
    {make_sys_error_code(EKEYEXPIRED), "ekeyexpired"}};
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
        : env {enif_alloc_env(), enif_free_env}
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
thread_local std::random_device NifCTX::rd {};
thread_local std::default_random_engine NifCTX::gen {NifCTX::rd()};
thread_local std::uniform_int_distribution<int> NifCTX::dist {};

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
            env, std::make_tuple(error, nifpp::str_atom {e.code().message()}));
    }
    catch (const std::exception &e) {
        return nifpp::make(
            env, std::make_tuple(error, folly::fbstring {e.what()}));
    }
}

template <typename... Args, std::size_t... I>
ERL_NIF_TERM wrap_helper(ERL_NIF_TERM (*fun)(NifCTX ctx, Args...),
    ErlNifEnv *env, const ERL_NIF_TERM args[], std::index_sequence<I...>)
{
    return handle_errors(env,
        [&]() { return fun(NifCTX {env}, nifpp::get<Args>(env, args[I])...); });
}

template <typename... Args>
ERL_NIF_TERM wrap(ERL_NIF_TERM (*fun)(NifCTX, Args...), ErlNifEnv *env,
    const ERL_NIF_TERM args[])
{
    return wrap_helper(fun, env, args, std::index_sequence_for<Args...> {});
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
        fun, env, args, std::index_sequence_for<Args...> {});
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
    std::move(future)
        .thenValue(
            [ctx](T &&value) { handle_value(ctx, std::forward<T>(value)); })
        .thenError(folly::tag_t<std::system_error> {},
            [ctx](auto &&e) {
                auto it = error_to_atom.find(e.code());
                nifpp::str_atom reason {e.code().message()};
                if (it != error_to_atom.end())
                    reason = it->second;

                ctx.send(std::make_tuple(error, reason));
            })
        .thenError(folly::tag_t<std::exception> {}, [ctx](auto &&e) {
            nifpp::str_atom reason {e.what()};
            ctx.send(std::make_tuple(error, reason));
        });
}

/**
 * Handles open file handle callback result.
 */
template <class T>
void handle_result(NifCTX ctx, file_handle_ptr handle, folly::Future<T> future)
{
    std::move(future)
        .thenValue([ctx, handle = std::move(handle)](T &&value) {
            handle_value(ctx, std::forward<T>(value));
        })
        .thenError(folly::tag_t<std::system_error> {},
            [ctx](auto &&e) {
                auto it = error_to_atom.find(e.code());
                nifpp::str_atom reason {e.code().message()};
                if (it != error_to_atom.end())
                    reason = it->second;

                ctx.send(std::make_tuple(error, reason));
            })
        .thenError(folly::tag_t<std::exception> {}, [ctx](auto &&e) {
            nifpp::str_atom reason {e.what()};
            ctx.send(std::make_tuple(error, reason));
        });
}

/**
 * Configure performance monitoring metrics based on environment
 * configuration.
 */
static void configurePerformanceMonitoring(
    std::unordered_map<folly::fbstring, folly::fbstring> &args)
{
    if (args.find("helpers_performance_monitoring_enabled") != args.end() &&
        args["helpers_performance_monitoring_enabled"] == "true") {
        if (args["helpers_performance_monitoring_type"] == "graphite") {
            auto config = std::make_shared<
                one::monitoring::GraphiteMonitoringConfiguration>();
            config->fromGraphiteURL("tcp://" +
                args["graphite_host"].toStdString() + ":" +
                args["graphite_port"].toStdString());
            config->namespacePrefix = args["graphite_prefix"].toStdString();
            config->reportingPeriod = std::stoul(
                args["helpers_performance_monitoring_period"].toStdString());
            if (args["helpers_performance_monitoring_level"] == "full") {
                config->reportingLevel = cppmetrics::core::ReportingLevel::Full;
            }
            else {
                config->reportingLevel =
                    cppmetrics::core::ReportingLevel::Basic;
            }

            // Configure and start performance monitoring threads
            one::helpers::configureMonitoring(config, true);

            // Initialize the configuration options counter values
            ONE_METRIC_COUNTER_SET(
                "comp.oneprovider.mod.options.ceph_helper_thread_count",
                std::stoul(args["ceph_helper_threads_number"].toStdString()));
            ONE_METRIC_COUNTER_SET(
                "comp.oneprovider.mod.options.cephrados_helper_thread_count",
                std::stoul(
                    args["cephrados_helper_threads_number"].toStdString()));
            ONE_METRIC_COUNTER_SET(
                "comp.oneprovider.mod.options.posix_helper_thread_count",
                std::stoul(args["posix_helper_threads_number"].toStdString()));
            ONE_METRIC_COUNTER_SET(
                "comp.oneprovider.mod.options.s3_helper_thread_count",
                std::stoul(args["s3_helper_threads_number"].toStdString()));
            ONE_METRIC_COUNTER_SET(
                "comp.oneprovider.mod.options.swift_helper_thread_count",
                std::stoul(args["swift_helper_threads_number"].toStdString()));
            ONE_METRIC_COUNTER_SET(
                "comp.oneprovider.mod.options.glusterfs_helper_thread_count",
                std::stoul(
                    args["glusterfs_helper_threads_number"].toStdString()));
            ONE_METRIC_COUNTER_SET(
                "comp.oneprovider.mod.options.nulldevice_helper_thread_count",
                std::stoul(
                    args["nulldevice_helper_threads_number"].toStdString()));
            ONE_METRIC_COUNTER_SET(
                "comp.oneprovider.mod.options.xrootd_helper_thread_count",
                std::stoul(
                    args["xrootd_helper_threads_number"].toStdString()));
            ONE_METRIC_COUNTER_SET(
                "comp.oneprovider.mod.options.nfs_helper_thread_count",
                std::stoul(
                    args["nfs_helper_threads_number"].toStdString()));
            ONE_METRIC_COUNTER_SET("comp.oneprovider.mod.options.buffer_"
                                   "scheduler_helper_thread_count",
                std::stoul(
                    args["buffer_scheduler_threads_number"].toStdString()));
            ONE_METRIC_COUNTER_SET(
                "comp.oneprovider.mod.options.read_buffer_min_size",
                std::stoul(args["read_buffer_min_size"].toStdString()));
            ONE_METRIC_COUNTER_SET(
                "comp.oneprovider.mod.options.read_buffer_max_size",
                std::stoul(args["read_buffer_max_size"].toStdString()));
            ONE_METRIC_COUNTER_SET(
                "comp.oneprovider.mod.options.write_buffer_min_size",
                std::stoul(args["write_buffer_min_size"].toStdString()));
            ONE_METRIC_COUNTER_SET(
                "comp.oneprovider.mod.options.write_buffer_max_size",
                std::stoul(args["write_buffer_max_size"].toStdString()));
            ONE_METRIC_COUNTER_SET(
                "comp.oneprovider.mod.options.read_buffer_prefetch_duration",
                std::stoul(
                    args["read_buffer_prefetch_duration"].toStdString()));
            ONE_METRIC_COUNTER_SET(
                "comp.oneprovider.mod.options.write_buffer_flush_delay",
                std::stoul(args["write_buffer_flush_delay"].toStdString()));
            ONE_METRIC_COUNTER_SET(
                "comp.oneprovider.mod.options.monitoring_reporting_period",
                std::stoul(args["helpers_performance_monitoring_period"]
                               .toStdString()));
        }
    }
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

ERL_NIF_TERM refresh_params(NifCTX ctx, helper_ptr helper, helper_args_t args)
{
    handle_result(ctx,
        helper->refreshParams(
            one::helpers::StorageHelperParams::create(helper->name(), args)));

    return nifpp::make(ctx.env, std::make_tuple(ok, ctx.reqId));
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

ERL_NIF_TERM listobjects(NifCTX ctx, helper_ptr helper, folly::fbstring prefix,
    folly::fbstring marker, const off_t offset, const size_t count)
{
    handle_result(ctx, helper->listobjects(prefix, marker, offset, count));
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

ERL_NIF_TERM unlink(NifCTX ctx, helper_ptr helper, folly::fbstring file,
    const size_t currentSize)
{
    handle_result(ctx, helper->unlink(file, currentSize));
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

ERL_NIF_TERM truncate(NifCTX ctx, helper_ptr helper, folly::fbstring file,
    const off_t size, const size_t currentSize)
{
    handle_result(ctx, helper->truncate(file, size, currentSize));
    return nifpp::make(ctx.env, std::make_tuple(ok, ctx.reqId));
}

ERL_NIF_TERM setxattr(NifCTX ctx, helper_ptr helper, folly::fbstring file,
    folly::fbstring name, folly::fbstring value, const bool create,
    const bool replace)
{
    handle_result(ctx, helper->setxattr(file, name, value, create, replace));
    return nifpp::make(ctx.env, std::make_tuple(ok, ctx.reqId));
}

ERL_NIF_TERM getxattr(
    NifCTX ctx, helper_ptr helper, folly::fbstring file, folly::fbstring name)
{
    handle_result(ctx, helper->getxattr(file, name));
    return nifpp::make(ctx.env, std::make_tuple(ok, ctx.reqId));
}

ERL_NIF_TERM removexattr(
    NifCTX ctx, helper_ptr helper, folly::fbstring file, folly::fbstring name)
{
    handle_result(ctx, helper->removexattr(file, name));
    return nifpp::make(ctx.env, std::make_tuple(ok, ctx.reqId));
}

ERL_NIF_TERM listxattr(NifCTX ctx, helper_ptr helper, folly::fbstring file)
{
    handle_result(ctx, helper->listxattr(file));
    return nifpp::make(ctx.env, std::make_tuple(ok, ctx.reqId));
}

ERL_NIF_TERM flushbuffer(
    NifCTX ctx, helper_ptr helper, folly::fbstring file, size_t size)
{
    handle_result(ctx, helper->flushBuffer(file, size));
    return nifpp::make(ctx.env, std::make_tuple(ok, ctx.reqId));
}

ERL_NIF_TERM blocksize_for_path(
    NifCTX ctx, helper_ptr helper, folly::fbstring file)
{
    handle_result(ctx, helper->blockSizeForPath(file));
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
    handle_result(ctx, handle, handle->read(offset, size));
    return nifpp::make(ctx.env, std::make_tuple(ok, ctx.reqId));
}

ERL_NIF_TERM write(NifCTX ctx, file_handle_ptr handle, const off_t offset,
    std::pair<const uint8_t *, size_t> data)
{
    folly::IOBufQueue buf {folly::IOBufQueue::cacheChainLength()};

    auto helperBlockSize = handle->helper()->blockSize();
    auto bufferBlockSize =
        (helperBlockSize != 0) ? helperBlockSize : (1U << 31);

    buf.wrapBuffer(data.first, data.second, bufferBlockSize);

    handle_result(ctx, handle, handle->write(offset, std::move(buf), {}));
    return nifpp::make(ctx.env, std::make_tuple(ok, ctx.reqId));
}

ERL_NIF_TERM release(NifCTX ctx, file_handle_ptr handle)
{
    handle_result(ctx, handle, handle->release());
    return nifpp::make(ctx.env, std::make_tuple(ok, ctx.reqId));
}

ERL_NIF_TERM flush(NifCTX ctx, file_handle_ptr handle)
{
    handle_result(ctx, handle, handle->flush());
    return nifpp::make(ctx.env, std::make_tuple(ok, ctx.reqId));
}

ERL_NIF_TERM fsync(NifCTX ctx, file_handle_ptr handle, const int isdatasync)
{
    handle_result(ctx, handle, handle->fsync(isdatasync));
    return nifpp::make(ctx.env, std::make_tuple(ok, ctx.reqId));
}

ERL_NIF_TERM refresh_helper_params(
    NifCTX ctx, file_handle_ptr handle, helper_args_t args)
{
    handle_result(ctx,
        handle->refreshHelperParams(one::helpers::StorageHelperParams::create(
            handle->helper()->name(), args)));
    return nifpp::make(ctx.env, std::make_tuple(ok, ctx.reqId));
}

ERL_NIF_TERM start_monitoring(
    ErlNifEnv *env, int argc, const ERL_NIF_TERM argv[])
{
    ONE_MONITORING_COLLECTOR()->start();
    return nifpp::make(env, std::make_tuple(ok, 1));
}

ERL_NIF_TERM stop_monitoring(
    ErlNifEnv *env, int argc, const ERL_NIF_TERM argv[])
{
    ONE_MONITORING_COLLECTOR()->stop();
    return nifpp::make(env, std::make_tuple(ok, 1));
}

} // namespace

extern "C" {

static int load(ErlNifEnv *env, void **priv_data, ERL_NIF_TERM load_info)
{
    one::helpers::init();

    auto args =
        nifpp::get<std::unordered_map<folly::fbstring, folly::fbstring>>(
            env, load_info);

    configurePerformanceMonitoring(args);

    application = std::make_unique<HelpersNIF>(std::move(args));

    return !(nifpp::register_resource<helper_ptr>(env, nullptr, "helper_ptr") &&
        nifpp::register_resource<file_handle_ptr>(
            env, nullptr, "file_handle_ptr"));
}

static ERL_NIF_TERM sh_refresh_params(
    ErlNifEnv *env, int argc, const ERL_NIF_TERM argv[])
{
    return wrap(refresh_params, env, argv);
}

static ERL_NIF_TERM sh_listobjects(
    ErlNifEnv *env, int argc, const ERL_NIF_TERM argv[])
{
    return wrap(listobjects, env, argv);
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

static ERL_NIF_TERM sh_setxattr(
    ErlNifEnv *env, int argc, const ERL_NIF_TERM argv[])
{
    return wrap(setxattr, env, argv);
}

static ERL_NIF_TERM sh_getxattr(
    ErlNifEnv *env, int argc, const ERL_NIF_TERM argv[])
{
    return wrap(getxattr, env, argv);
}

static ERL_NIF_TERM sh_removexattr(
    ErlNifEnv *env, int argc, const ERL_NIF_TERM argv[])
{
    return wrap(removexattr, env, argv);
}

static ERL_NIF_TERM sh_listxattr(
    ErlNifEnv *env, int argc, const ERL_NIF_TERM argv[])
{
    return wrap(listxattr, env, argv);
}

static ERL_NIF_TERM sh_flushbuffer(
    ErlNifEnv *env, int argc, const ERL_NIF_TERM argv[])
{
    return wrap(flushbuffer, env, argv);
}

static ERL_NIF_TERM sh_blocksize_for_path(
    ErlNifEnv *env, int argc, const ERL_NIF_TERM argv[])
{
    return wrap(blocksize_for_path, env, argv);
}

static ERL_NIF_TERM sh_open(ErlNifEnv *env, int argc, const ERL_NIF_TERM argv[])
{
    return wrap(open, env, argv);
}

static ERL_NIF_TERM sh_read(ErlNifEnv *env, int argc, const ERL_NIF_TERM argv[])
{
    return wrap(read, env, argv);
}

static ERL_NIF_TERM sh_refresh_helper_params(
    ErlNifEnv *env, int argc, const ERL_NIF_TERM argv[])
{
    return wrap(refresh_helper_params, env, argv);
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
    {"get_handle", 2, get_handle, ERL_NIF_DIRTY_JOB_IO_BOUND},
    {"start_monitoring", 0, start_monitoring, ERL_NIF_DIRTY_JOB_IO_BOUND},
    {"stop_monitoring", 0, stop_monitoring, ERL_NIF_DIRTY_JOB_IO_BOUND},
    {"refresh_params", 2, sh_refresh_params, ERL_NIF_DIRTY_JOB_IO_BOUND},
    {"refresh_helper_params", 2, sh_refresh_helper_params,
        ERL_NIF_DIRTY_JOB_IO_BOUND},
    {"getattr", 2, sh_getattr, ERL_NIF_DIRTY_JOB_IO_BOUND},
    {"access", 3, sh_access, ERL_NIF_DIRTY_JOB_IO_BOUND},
    {"readdir", 4, sh_readdir, ERL_NIF_DIRTY_JOB_IO_BOUND},
    {"listobjects", 5, sh_listobjects, ERL_NIF_DIRTY_JOB_IO_BOUND},
    {"mknod", 5, sh_mknod, ERL_NIF_DIRTY_JOB_IO_BOUND},
    {"mkdir", 3, sh_mkdir, ERL_NIF_DIRTY_JOB_IO_BOUND},
    {"unlink", 3, sh_unlink, ERL_NIF_DIRTY_JOB_IO_BOUND},
    {"rmdir", 2, sh_rmdir, ERL_NIF_DIRTY_JOB_IO_BOUND},
    {"symlink", 3, sh_symlink, ERL_NIF_DIRTY_JOB_IO_BOUND},
    {"rename", 3, sh_rename, ERL_NIF_DIRTY_JOB_IO_BOUND},
    {"link", 3, sh_link, ERL_NIF_DIRTY_JOB_IO_BOUND},
    {"chmod", 3, sh_chmod, ERL_NIF_DIRTY_JOB_IO_BOUND},
    {"chown", 4, sh_chown, ERL_NIF_DIRTY_JOB_IO_BOUND},
    {"truncate", 4, sh_truncate, ERL_NIF_DIRTY_JOB_IO_BOUND},
    {"setxattr", 6, sh_setxattr, ERL_NIF_DIRTY_JOB_IO_BOUND},
    {"getxattr", 3, sh_getxattr, ERL_NIF_DIRTY_JOB_IO_BOUND},
    {"removexattr", 3, sh_removexattr, ERL_NIF_DIRTY_JOB_IO_BOUND},
    {"listxattr", 2, sh_listxattr, ERL_NIF_DIRTY_JOB_IO_BOUND},
    {"open", 3, sh_open, ERL_NIF_DIRTY_JOB_IO_BOUND},
    {"read", 3, sh_read, ERL_NIF_DIRTY_JOB_IO_BOUND},
    {"write", 3, sh_write, ERL_NIF_DIRTY_JOB_IO_BOUND},
    {"release", 1, sh_release, ERL_NIF_DIRTY_JOB_IO_BOUND},
    {"flush", 1, sh_flush, ERL_NIF_DIRTY_JOB_IO_BOUND},
    {"fsync", 2, sh_fsync, ERL_NIF_DIRTY_JOB_IO_BOUND},
    {"flushbuffer", 3, sh_flushbuffer, ERL_NIF_DIRTY_JOB_IO_BOUND},
    {"blocksize_for_path", 2, sh_blocksize_for_path,
        ERL_NIF_DIRTY_JOB_IO_BOUND}};

ERL_NIF_INIT(helpers_nif, nif_funcs, load, NULL, NULL, NULL);

} // extern C
