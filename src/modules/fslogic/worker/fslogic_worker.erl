%%%--------------------------------------------------------------------
%%% @author Rafal Slota
%%% @copyright (C) 2015 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%--------------------------------------------------------------------
%%% @doc
%%% This module implements worker_plugin_behaviour callbacks.
%%% Also it decides whether request has to be handled locally or rerouted
%%% to other provider.
%%% @end
%%%--------------------------------------------------------------------
-module(fslogic_worker).
-behaviour(worker_plugin_behaviour).

-include("global_definitions.hrl").
-include("proto/oneclient/proxyio_messages.hrl").
-include("proto/oneprovider/provider_messages.hrl").
-include("modules/events/definitions.hrl").
-include_lib("ctool/include/logging.hrl").
-include_lib("cluster_worker/include/exometer_utils.hrl").
-include_lib("ctool/include/errors.hrl").

-export([supervisor_flags/0, supervisor_children_spec/0]).
-export([init_effective_caches/1]).
-export([init/1, handle/1, cleanup/0]).
-export([init_counters/0, init_report/0]).

% exported for RPC
-export([
    schedule_init_effective_caches/1
]).

%%%===================================================================
%%% Types
%%%===================================================================

-type fuse_request() :: #fuse_request{}.
-type provider_request() :: #provider_request{}.
-type proxyio_request() :: #proxyio_request{}.
-type request() :: fuse_request() | provider_request() | proxyio_request().

-type fuse_response() :: #fuse_response{}.
-type provider_response() :: #provider_response{}.
-type proxyio_response() :: #proxyio_response{}.
-type response() :: fuse_response() | provider_response() | proxyio_response().

-type file() :: file_meta:entry(). %% Type alias for better code organization
-type open_flag() :: helpers:open_flag().
-type posix_permissions() :: file_meta:posix_permissions().
-type file_guid() :: file_id:file_guid().

-export_type([
    request/0, response/0, file/0, open_flag/0, posix_permissions/0,
    file_guid/0, fuse_response/0, provider_response/0, proxyio_response/0, fuse_response_type/0
]).

% requests
-define(PERIODICAL_SPACES_AUTOCLEANING_CHECK, periodical_spaces_autocleaning_check).
-define(REPORT_PROVIDER_RESTART_TO_ATM_WORKFLOW_EXECUTION_LAYER,
    report_provider_restart_to_atm_workflow_execution_layer
).
-define(RERUN_TRANSFERS, rerun_transfers).
-define(RESTART_AUTOCLEANING_RUNS, restart_autocleaning_runs).
-define(INIT_EFFECTIVE_CACHES(Space), {init_effective_caches, Space}).

-define(SHOULD_PERFORM_PERIODICAL_SPACES_AUTOCLEANING_CHECK,
    op_worker:get_env(autocleaning_periodical_spaces_check_enabled, true)).

% delays and intervals
-define(AUTOCLEANING_PERIODICAL_SPACES_CHECK_INTERVAL,
    op_worker:get_env(autocleaning_periodical_spaces_check_interval, timer:minutes(1))).
-define(REPORT_PROVIDER_RESTART_TO_ATM_WORKFLOW_EXECUTION_LAYER_DELAY,
    op_worker:get_env(report_provider_restart_to_atm_workflow_execution_layer_delay, 10000)).
-define(RERUN_TRANSFERS_DELAY,
    op_worker:get_env(rerun_transfers_delay, 10000)).
-define(RESTART_AUTOCLEANING_RUNS_DELAY,
    op_worker:get_env(restart_autocleaning_runs_delay, 10000)).

% exometer macros
-define(EXOMETER_NAME(Param), ?exometer_name(?MODULE, count, Param)).
-define(EXOMETER_TIME_NAME(Param), ?exometer_name(?MODULE, time,
    list_to_atom(atom_to_list(Param) ++ "_time"))).
-define(EXOMETER_COUNTERS, [get_file_attr, get_child_attr, change_mode,
    update_times, delete_file, create_dir, get_file_children,
    get_file_children_attrs, rename, create_file, make_file, open_file, release,
    get_file_location, truncate, synchronize_block,
    synchronize_block_and_compute_checksum, get_xattr, set_xattr, remove_xattr,
    list_xattr, fsync]).
-define(EXOMETER_DEFAULT_DATA_POINTS_NUMBER, 10000).

% This macro is used to disable automatic rerun of transfers in tests
-define(SHOULD_RERUN_TRANSFERS, op_worker:get_env(rerun_transfers, true)).

% This macro is used to disable automatic restart of autocleaning runs in tests
-define(SHOULD_RESTART_AUTOCLEANING_RUNS, op_worker:get_env(autocleaning_restart_runs, true)).

-define(OPERATIONS_AVAILABLE_IN_SHARE_MODE, [
    % Checking perms for operations other than 'read' should result in immediate ?EACCES
    check_perms,
    get_parent,
    % TODO VFS-6057 resolve share path up to share not user root dir
    %%    get_file_path,
    resolve_symlink,

    list_xattr,
    get_xattr,

    % Opening file is available but only in 'read' mode
    open_file,
    open_file_with_extended_info,
    synchronize_block,
    remote_read,
    fsync,
    release,

    read_symlink,

    get_file_attr,
    get_file_details,
    get_file_children,
    get_child_attr,
    get_file_children_attrs,
    get_file_children_details
]).
-define(AVAILABLE_OPERATIONS_IN_OPEN_HANDLE_SHARE_MODE, [
    % Necessary operations for direct-io to work (contains private information
    % like storage id, etc.)
    get_file_location,
    get_helper_params

    | ?OPERATIONS_AVAILABLE_IN_SHARE_MODE
]).

%%%===================================================================
%%% API
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Returns a fslogic worker supervisor flags.
%% @end
%%--------------------------------------------------------------------
-spec supervisor_flags() -> supervisor:sup_flags().
supervisor_flags() ->
    #{strategy => one_for_one, intensity => 1000, period => 3600}.

%%--------------------------------------------------------------------
%% @doc
%% Returns a children spec for a fslogic supervisor.
%% @end
%%--------------------------------------------------------------------
-spec supervisor_children_spec() -> [supervisor:child_spec()].
supervisor_children_spec() ->
    [
        auth_cache:spec(),
        lfm_handles_monitor:spec(),
        transfer_onf_stats_aggregator:spec()
    ].

%%--------------------------------------------------------------------
%% @doc
%% Initializes effective caches on all nodes.
%% @end
%%--------------------------------------------------------------------
-spec init_effective_caches(od_space:id() | all) -> ok.
init_effective_caches(Space) ->
    Nodes = consistent_hashing:get_all_nodes(),
    utils:rpc_multicall(Nodes, ?MODULE, schedule_init_effective_caches, [Space]),
    ok.

%%%===================================================================
%%% worker_plugin_behaviour callbacks
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% {@link worker_plugin_behaviour} callback init/1.
%% @end
%%--------------------------------------------------------------------
-spec init(Args :: term()) -> Result when
    Result :: {ok, State :: worker_host:plugin_state()} | {error, Reason :: term()}.
init(_Args) ->
    permissions_cache:init(),
    init_effective_caches(),
    transfer:init(),
    replica_deletion_master:init_workers_pool(),
    file_registration:init_pool(),
    autocleaning_view_traverse:init_pool(),
    tree_deletion_traverse:init_pool(),
    bulk_download_traverse:init_pool(),
    clproto_serializer:load_msg_defs(),
    archivisation_traverse:init_pool(),

    schedule_rerun_transfers(),
    schedule_provider_restart_report_to_atm_workflow_execution_layer(),
    schedule_restart_autocleaning_runs(),
    schedule_periodical_spaces_autocleaning_check(),

    lists:foreach(fun({Fun, Args}) ->
        case apply(Fun, Args) of
            {ok, _} -> ok;
            {error, already_exists} -> ok
        end
    end, [
        {fun subscription:create_durable_subscription/1,
            [fslogic_event_durable_subscriptions:file_read_subscription()]},
        {fun subscription:create_durable_subscription/1,
            [fslogic_event_durable_subscriptions:file_written_subscription()]},
        {fun session_manager:create_root_session/0, []},
        {fun session_manager:create_guest_session/0, []}
    ]),

    {ok, #{}}.

%%--------------------------------------------------------------------
%% @doc
%% {@link worker_plugin_behaviour} callback handle/1.
%% @end
%%--------------------------------------------------------------------
-spec handle(Request) -> Result when
    Request ::
    ping |
    healthcheck |
    {fuse_request, session:id(), fuse_request()} |
    {provider_request, session:id(), provider_request()} |
    {proxyio_request, session:id(), proxyio_request()},
    Result :: cluster_status:status() | ok | {ok, response()} |
    {error, Reason :: term()} | pong.
handle(ping) ->
    pong;
handle(healthcheck) ->
    ok;
handle(?REPORT_PROVIDER_RESTART_TO_ATM_WORKFLOW_EXECUTION_LAYER) ->
    ?debug("Reporting stale atm workflow executions after provider restart"),
    report_provider_restart_to_atm_workflow_execution(),
    ok;
handle(?RERUN_TRANSFERS) ->
    ?debug("Rerunning unfinished transfers"),
    rerun_transfers(),
    ok;
handle(?RESTART_AUTOCLEANING_RUNS) ->
    ?debug("Restarting unfinished auto-cleaning runs"),
    restart_autocleaning_runs(),
    ok;
handle(?PERIODICAL_SPACES_AUTOCLEANING_CHECK) ->
    case ?SHOULD_PERFORM_PERIODICAL_SPACES_AUTOCLEANING_CHECK of
        true ->
            periodical_spaces_autocleaning_check();
        false ->
            ok
    end,
    schedule_periodical_spaces_autocleaning_check();
handle({fuse_request, SessId, FuseRequest}) ->
    ?debug("fuse_request(~p): ~p", [SessId, FuseRequest]),
    Response = handle_request_and_process_response(SessId, FuseRequest),
    ?debug("fuse_response: ~p", [Response]),
    {ok, Response};
handle({provider_request, SessId, ProviderRequest}) ->
    ?debug("provider_request(~p): ~p", [SessId, ProviderRequest]),
    Response = handle_request_and_process_response(SessId, ProviderRequest),
    ?debug("provider_response: ~p", [Response]),
    {ok, Response};
handle({proxyio_request, SessId, ProxyIORequest}) ->
    ?debug("proxyio_request(~p): ~p", [SessId, fslogic_log:mask_data_in_message(ProxyIORequest)]),
    Response = handle_request_and_process_response(SessId, ProxyIORequest),
    ?debug("proxyio_response: ~p", [fslogic_log:mask_data_in_message(Response)]),
    {ok, Response};
handle({bounded_cache_timer, Msg}) ->
    bounded_cache:check_cache_size(Msg);
handle(?INIT_EFFECTIVE_CACHES(Space)) ->
    paths_cache:init(Space),
    dataset_eff_cache:init(Space),
    archive_recall_cache:init(Space),
    file_meta_links_sync_status_cache:init(Space);
handle(_Request) ->
    ?log_bad_request(_Request),
    {error, wrong_request}.

%%--------------------------------------------------------------------
%% @doc
%% {@link worker_plugin_behaviour} callback cleanup/0
%% @end
%%--------------------------------------------------------------------
-spec cleanup() -> Result when
    Result :: ok | {error, Error},
    Error :: timeout | term().
cleanup() ->
    transfer:cleanup(),
    autocleaning_view_traverse:stop_pool(),
    file_registration:stop_pool(),
    replica_deletion_master:stop_workers_pool(),
    tree_deletion_traverse:stop_pool(),
    bulk_download_traverse:stop_pool(),
    replica_synchronizer:terminate_all(),
    archivisation_traverse:stop_pool(),
    permissions_cache:terminate(),
    ok.

%%%===================================================================
%%% Exometer API
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Initializes all counters.
%% @end
%%--------------------------------------------------------------------
-spec init_counters() -> ok.
init_counters() ->
    Size = application:get_env(?CLUSTER_WORKER_APP_NAME,
        exometer_data_points_number, ?EXOMETER_DEFAULT_DATA_POINTS_NUMBER),
    Counters = lists:map(fun(Name) ->
        {?EXOMETER_NAME(Name), counter}
    end, ?EXOMETER_COUNTERS),
    Counters2 = lists:map(fun(Name) ->
        {?EXOMETER_TIME_NAME(Name), uniform, [{size, Size}]}
    end, ?EXOMETER_COUNTERS),
    ?init_counters(Counters ++ Counters2).

%%--------------------------------------------------------------------
%% @doc
%% Subscribe for reports for all parameters.
%% @end
%%--------------------------------------------------------------------
-spec init_report() -> ok.
init_report() ->
    Reports = lists:map(fun(Name) ->
        {?EXOMETER_NAME(Name), [value]}
    end, ?EXOMETER_COUNTERS),
    Reports2 = lists:map(fun(Name) ->
        {?EXOMETER_TIME_NAME(Name), [min, max, median, mean, n]}
    end, ?EXOMETER_COUNTERS),
    ?init_reports(Reports ++ Reports2).

%%%===================================================================
%%% Internal functions
%%%===================================================================

-spec init_effective_caches() -> ok.
init_effective_caches() ->
    % TODO VFS-7412 refactor effective_value cache
    paths_cache:init_group(),
    dataset_eff_cache:init_group(),
    file_meta_links_sync_status_cache:init_group(),
    archive_recall_cache:init_group(),
    schedule_init_effective_caches(all).


%%--------------------------------------------------------------------
%% @private
%% @doc
%% Handle request and do postprocessing of the response
%% @end
%%--------------------------------------------------------------------
-spec handle_request_and_process_response(session:id(), request()) -> response().
handle_request_and_process_response(SessId, Request) ->
    try
        UserCtx = user_ctx:new(SessId),
        FilePartialCtx = fslogic_request:get_file_partial_ctx(UserCtx, Request),
        Providers = fslogic_request:get_target_providers(UserCtx,
            FilePartialCtx, Request),
        case lists:member(oneprovider:get_id(), Providers) of
            true ->
                handle_request_and_process_response_locally(UserCtx, Request,
                    FilePartialCtx);
            false ->
                handle_request_remotely(UserCtx, Request, Providers)
        end
    catch
        Type2:Error2:Stacktrace ->
            fslogic_errors:handle_error(Request, Type2, Error2, Stacktrace)
    end.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Handle request locally and do postprocessing of the response
%% @end
%%--------------------------------------------------------------------
-spec handle_request_and_process_response_locally(user_ctx:ctx(), request(),
    file_partial_ctx:ctx() | undefined) -> response().
handle_request_and_process_response_locally(UserCtx0, Request, FilePartialCtx) ->
    {FileCtx1, ShareId} = case FilePartialCtx of
        undefined ->
            {undefined, undefined};
        _ ->
            {FileCtx0, _SpaceId0} = file_ctx:new_by_partial_context(FilePartialCtx),
            {FileCtx0, file_ctx:get_share_id_const(FileCtx0)}
    end,
    ok = fslogic_log:report_file_access_operation(Request, user_ctx:get_user_id(UserCtx0), FileCtx1),
    try
        UserCtx1 = case {user_ctx:is_in_open_handle_mode(UserCtx0), ShareId} of
            {false, undefined} ->
                UserCtx0;
            {IsInOpenHandleMode, _} ->
                case is_operation_available_in_share_mode(Request, IsInOpenHandleMode) of
                    true -> ok;
                    false -> throw(?EPERM)
                end,
                case IsInOpenHandleMode of
                    true ->
                        UserCtx0;
                    false ->
                        % Operations concerning shares must be carried with GUEST auth
                        case user_ctx:is_guest(UserCtx0) of
                            true -> UserCtx0;
                            false -> user_ctx:new(?GUEST_SESS_ID)
                        end
                end
        end,
        handle_request_locally(UserCtx1, Request, FileCtx1)
    catch
        Type:Error:Stacktrace ->
            fslogic_errors:handle_error(Request, Type, Error, Stacktrace)
    end.


%% @private
-spec is_operation_available_in_share_mode(request(), IsInOpenHandleMode :: boolean()) ->
    boolean().
is_operation_available_in_share_mode(#fuse_request{fuse_request = #file_request{
    file_request = #open_file{flag = Flag}
}}, _) ->
    Flag == read;
is_operation_available_in_share_mode(#fuse_request{fuse_request = #file_request{
    file_request = #open_file_with_extended_info{flag = Flag}
}}, _) ->
    Flag == read;
is_operation_available_in_share_mode(#provider_request{
    provider_request = #check_perms{flag = Flag}
}, _) ->
    Flag == read;
is_operation_available_in_share_mode(Request, true) ->
    lists:member(get_operation(Request), ?AVAILABLE_OPERATIONS_IN_OPEN_HANDLE_SHARE_MODE);
is_operation_available_in_share_mode(Request, false) ->
    lists:member(get_operation(Request), ?OPERATIONS_AVAILABLE_IN_SHARE_MODE).


%% @private
-spec get_operation(request()) -> atom().
get_operation(#fuse_request{fuse_request = #file_request{file_request = Req}}) ->
    element(1, Req);
get_operation(#fuse_request{fuse_request = Req}) ->
    element(1, Req);
get_operation(#provider_request{provider_request = Req}) ->
    element(1, Req);
get_operation(#proxyio_request{proxyio_request = Req}) ->
    element(1, Req).


%%--------------------------------------------------------------------
%% @private
%% @doc
%% Handle request locally, as it operates on locally supported entity.
%% @end
%%--------------------------------------------------------------------
-spec handle_request_locally(user_ctx:ctx(), request(), file_ctx:ctx() | undefined) -> response().
handle_request_locally(UserCtx, #fuse_request{fuse_request = #file_request{
    file_request = Req
}}, FileCtx) ->
    [ReqName | _] = tuple_to_list(Req),
    ?update_counter(?EXOMETER_NAME(ReqName)),
    Stopwatch = stopwatch:start(),
    Ans = handle_file_request(UserCtx, Req, FileCtx),
    ?update_counter(?EXOMETER_TIME_NAME(ReqName), stopwatch:read_micros(Stopwatch)),
    Ans;
handle_request_locally(UserCtx, #fuse_request{fuse_request = #multipart_upload_request{
    multipart_request = Req
}}, _FileCtx) ->
    handle_multipart_upload_request(UserCtx, Req);
handle_request_locally(UserCtx, #fuse_request{fuse_request = Req}, FileCtx) ->
    handle_fuse_request(UserCtx, Req, FileCtx);
handle_request_locally(UserCtx, #provider_request{provider_request = Req}, FileCtx) ->
    handle_provider_request(UserCtx, Req, FileCtx);
handle_request_locally(UserCtx, #proxyio_request{
    parameters = Parameters,
    proxyio_request = Req
}, FileCtx) ->
    HandleId = maps:get(?PROXYIO_PARAMETER_HANDLE_ID, Parameters, undefined),
    handle_proxyio_request(UserCtx, Req, FileCtx, HandleId).

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Handle request remotely
%% @end
%%--------------------------------------------------------------------
-spec handle_request_remotely(user_ctx:ctx(), request(), [od_provider:id()]) -> response().
handle_request_remotely(_UserCtx, _Req, []) ->
    #status{code = ?ENOTSUP};
handle_request_remotely(UserCtx, Req, Providers) ->
    ProviderId = fslogic_remote:get_provider_to_route(Providers),
    fslogic_remote:route(UserCtx, ProviderId, Req).

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Processes a FUSE request and returns a response.
%% @end
%%--------------------------------------------------------------------
-spec handle_fuse_request(user_ctx:ctx(), fuse_request_type(), file_ctx:ctx() | undefined) ->
    fuse_response().
handle_fuse_request(UserCtx, #resolve_guid{}, FileCtx) ->
    guid_req:resolve_guid(UserCtx, FileCtx);
handle_fuse_request(UserCtx, #resolve_guid_by_canonical_path{}, FileCtx) ->
    guid_req:resolve_guid(UserCtx, FileCtx);
handle_fuse_request(UserCtx, #resolve_guid_by_relative_path{
    path = Path
}, RelRootCtx) ->
    guid_req:resolve_guid_by_relative_path(UserCtx, RelRootCtx, Path);
handle_fuse_request(UserCtx, #ensure_dir{
    path = Path,
    mode = Mode
}, RelRootCtx) ->
    guid_req:ensure_dir(UserCtx, RelRootCtx, Path, Mode);
handle_fuse_request(UserCtx, #get_helper_params{
    storage_id = StorageId,
    space_id = SpaceId,
    helper_mode = HelperMode
}, undefined) ->
    storage_req:get_helper_params(UserCtx, StorageId, SpaceId, HelperMode);
handle_fuse_request(UserCtx, #create_storage_test_file{
    file_guid = Guid,
    storage_id = StorageId
}, undefined) ->
    storage_req:create_storage_test_file(UserCtx, Guid, StorageId);
handle_fuse_request(UserCtx, #verify_storage_test_file{
    space_id = SpaceId,
    storage_id = StorageId,
    file_id = FileId,
    file_content = FileContent
}, undefined) ->
    Session = user_ctx:get_session_id(UserCtx),
    case storage_req:verify_storage_test_file(UserCtx, SpaceId,
        StorageId, FileId, FileContent) of
        #fuse_response{status = #status{code = ?OK}} = Ans ->
            session:set_direct_io(Session, SpaceId, true),
            Ans;
        Error ->
            session:set_direct_io(Session, SpaceId, false),
            Error
    end;
handle_fuse_request(UserCtx, #get_fs_stats{}, FileCtx) ->
    attr_req:get_fs_stats(UserCtx, FileCtx).
    


%%--------------------------------------------------------------------
%% @private
%% @doc
%% Processes a file request and returns a response.
%% @end
%%--------------------------------------------------------------------
-spec handle_file_request(user_ctx:ctx(), file_request_type(), file_ctx:ctx()) ->
    fuse_response().
handle_file_request(UserCtx, #get_file_attr{optional_attrs = OptionalAttrs}, FileCtx) ->
    attr_req:get_file_attr(UserCtx, FileCtx, OptionalAttrs);
handle_file_request(UserCtx, #get_file_references{}, FileCtx) ->
    attr_req:get_file_references(UserCtx, FileCtx);
handle_file_request(UserCtx, #get_file_details{}, FileCtx) ->
    attr_req:get_file_details(UserCtx, FileCtx);
handle_file_request(UserCtx, #get_child_attr{name = Name, optional_attrs = OptionalAttrs}, ParentFileCtx) ->
    attr_req:get_child_attr(UserCtx, ParentFileCtx, Name, OptionalAttrs);
handle_file_request(UserCtx, #change_mode{mode = Mode}, FileCtx) ->
    attr_req:chmod(UserCtx, FileCtx, Mode);
handle_file_request(UserCtx, #update_times{atime = ATime, mtime = MTime, ctime = CTime}, FileCtx) ->
    attr_req:update_times(UserCtx, FileCtx, ATime, MTime, CTime);
handle_file_request(UserCtx, #delete_file{silent = Silent}, FileCtx) ->
    delete_req:delete(UserCtx, FileCtx, Silent);
handle_file_request(UserCtx, #move_to_trash{emit_events = EmitEvents}, FileCtx) ->
    delete_req:delete_using_trash(UserCtx, FileCtx, EmitEvents);
handle_file_request(UserCtx, #create_dir{name = Name, mode = Mode}, ParentFileCtx) ->
    dir_req:mkdir(UserCtx, ParentFileCtx, Name, Mode);
handle_file_request(UserCtx, #get_file_children{listing_options = ListingOpts}, FileCtx) ->
    dir_req:get_children(UserCtx, FileCtx, ListingOpts);
handle_file_request(UserCtx, #get_file_children_attrs{
    listing_options = ListingOpts, 
    optional_attrs = OptionalAttrs
}, FileCtx) ->
    dir_req:get_children_attrs(UserCtx, FileCtx, ListingOpts, OptionalAttrs);
handle_file_request(UserCtx, #get_file_children_details{listing_options = ListingOpts}, FileCtx) ->
    dir_req:get_children_details(UserCtx, FileCtx, ListingOpts);
handle_file_request(UserCtx, #rename{
    target_parent_guid = TargetParentGuid,
    target_name = TargetName
}, SourceFileCtx) ->
    TargetParentFileCtx = file_ctx:new_by_guid(TargetParentGuid),
    rename_req:rename(UserCtx, SourceFileCtx, TargetParentFileCtx, TargetName);
handle_file_request(UserCtx, #create_file{name = Name, flag = Flag, mode = Mode}, ParentFileCtx) ->
    file_req:create_file(UserCtx, ParentFileCtx, Name, Mode, Flag);
handle_file_request(UserCtx, #storage_file_created{}, FileCtx) ->
    file_req:storage_file_created(UserCtx, FileCtx);
handle_file_request(UserCtx, #make_file{name = Name, mode = Mode}, ParentFileCtx) ->
    file_req:make_file(UserCtx, ParentFileCtx, Name, Mode);
handle_file_request(UserCtx, #make_link{target_parent_guid = TargetParentGuid, target_name = Name}, TargetFileCtx) ->
    TargetParentFileCtx = file_ctx:new_by_guid(TargetParentGuid),
    file_req:make_link(UserCtx, TargetFileCtx, TargetParentFileCtx, Name);
handle_file_request(UserCtx, #make_symlink{target_name = Name, link = Link}, ParentFileCtx) ->
    file_req:make_symlink(UserCtx, ParentFileCtx, Name, Link);
handle_file_request(UserCtx, #open_file{flag = Flag}, FileCtx) ->
    file_req:open_file(UserCtx, FileCtx, Flag);
handle_file_request(UserCtx, #open_file_with_extended_info{flag = Flag}, FileCtx) ->
    file_req:open_file_with_extended_info(UserCtx, FileCtx, Flag);
handle_file_request(UserCtx, #release{handle_id = HandleId}, FileCtx) ->
    file_req:release(UserCtx, FileCtx, HandleId);
handle_file_request(UserCtx, #get_file_location{}, FileCtx) ->
    file_req:get_file_location(UserCtx, FileCtx);
handle_file_request(UserCtx, #read_symlink{}, FileCtx) ->
    symlink_req:read(UserCtx, FileCtx);
handle_file_request(UserCtx, #resolve_symlink{}, FileCtx) ->
    symlink_req:resolve(UserCtx, FileCtx);
handle_file_request(UserCtx, #truncate{size = Size}, FileCtx) ->
    truncate_req:truncate(UserCtx, FileCtx, Size);
handle_file_request(UserCtx, #synchronize_block{block = Block, prefetch = Prefetch,
    priority = Priority}, FileCtx) ->
    sync_req:synchronize_block(UserCtx, FileCtx, Block, Prefetch, undefined, Priority);
handle_file_request(UserCtx, #block_synchronization_request{block = Block, prefetch = Prefetch,
    priority = Priority}, FileCtx) ->
    sync_req:request_block_synchronization(UserCtx, FileCtx, Block, Prefetch, undefined, Priority);
handle_file_request(UserCtx, #synchronize_block_and_compute_checksum{block = Block,
    prefetch = Prefetch, priority = Priority}, FileCtx) ->
    sync_req:synchronize_block_and_compute_checksum(UserCtx, FileCtx, Block, Prefetch, Priority);
handle_file_request(UserCtx, #get_xattr{
    name = XattrName,
    inherited = Inherited
}, FileCtx) ->
    xattr_req:get_xattr(UserCtx, FileCtx, XattrName, Inherited);
handle_file_request(UserCtx, #set_xattr{
    xattr = Xattr,
    create = Create,
    replace = Replace
}, FileCtx) ->
    xattr_req:set_xattr(UserCtx, FileCtx, Xattr, Create, Replace);
handle_file_request(UserCtx, #remove_xattr{name = XattrName}, FileCtx) ->
    xattr_req:remove_xattr(UserCtx, FileCtx, XattrName);
handle_file_request(UserCtx, #list_xattr{
    inherited = Inherited,
    show_internal = ShowInternal
}, FileCtx) ->
    xattr_req:list_xattr(UserCtx, FileCtx, Inherited, ShowInternal);
handle_file_request(UserCtx, #fsync{
    data_only = DataOnly,
    handle_id = HandleId
}, FileCtx) ->
    file_req:fsync(UserCtx, FileCtx, DataOnly, HandleId);
handle_file_request(UserCtx, #report_file_written{offset = Offset, size = Size}, FileCtx) ->
    file_req:report_file_written(UserCtx, FileCtx, Offset, Size);
handle_file_request(UserCtx, #report_file_read{offset = Offset, size = Size}, FileCtx) ->
    file_req:report_file_read(UserCtx, FileCtx, Offset, Size);
handle_file_request(UserCtx, #get_recursive_file_list{
    listing_options = Options,
    optional_attrs = OptionalAttrs
}, FileCtx) ->
    dir_req:list_recursively(UserCtx, FileCtx, Options, OptionalAttrs);
handle_file_request(UserCtx, #get_file_attr_by_path{path = RelativePath, optional_attrs = OptionalAttrs}, RootFileCtx) ->
    attr_req:get_file_attr_by_path(UserCtx, RootFileCtx, RelativePath, OptionalAttrs);
handle_file_request(UserCtx, #create_path{path = Path}, RootFileCtx) ->
    dir_req:create_dir_at_path(UserCtx, RootFileCtx, Path).

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Processes provider request and returns a response.
%% @end
%%--------------------------------------------------------------------
-spec handle_provider_request(user_ctx:ctx(), provider_request_type(), file_ctx:ctx()) ->
    provider_response().
handle_provider_request(UserCtx, #get_parent{}, FileCtx) ->
    guid_req:get_parent(UserCtx, FileCtx);
handle_provider_request(UserCtx, #get_file_path{}, FileCtx) ->
    guid_req:get_file_path(UserCtx, FileCtx);
handle_provider_request(UserCtx, #get_acl{}, FileCtx) ->
    acl_req:get_acl(UserCtx, FileCtx);
handle_provider_request(UserCtx, #set_acl{acl = #acl{value = Acl}}, FileCtx) ->
    acl_req:set_acl(UserCtx, FileCtx, Acl);
handle_provider_request(UserCtx, #remove_acl{}, FileCtx) ->
    acl_req:remove_acl(UserCtx, FileCtx);
handle_provider_request(UserCtx, #check_perms{flag = Flag}, FileCtx) ->
    permission_req:check_perms(UserCtx, FileCtx, Flag).


%%--------------------------------------------------------------------
%% @private
%% @doc
%% Processes proxyio request and returns a response.
%% @end
%%--------------------------------------------------------------------
-spec handle_proxyio_request(user_ctx:ctx(), proxyio_request_type(), file_ctx:ctx(),
    HandleId :: storage_driver:handle_id()) -> proxyio_response().
handle_proxyio_request(UserCtx, #remote_write{byte_sequence = ByteSequences}, FileCtx,
    HandleId) ->
    read_write_req:write(UserCtx, FileCtx, HandleId, ByteSequences);
handle_proxyio_request(UserCtx, #remote_read{offset = Offset, size = Size}, FileCtx,
    HandleId) ->
    read_write_req:read(UserCtx, FileCtx, HandleId, Offset, Size).


%%--------------------------------------------------------------------
%% @private
%% @doc
%% Processes a multipart upload request and returns a response.
%% @end
%%--------------------------------------------------------------------
handle_multipart_upload_request(UserCtx, #create_multipart_upload{space_id = SpaceId, path = Path}) ->
    multipart_upload_req:create(UserCtx, SpaceId, Path);
handle_multipart_upload_request(_UserCtx, #upload_multipart_part{multipart_upload_id = UploadId, part = Part}) ->
    multipart_upload_req:upload_part(UploadId, Part);
handle_multipart_upload_request(_UserCtx, #list_multipart_parts{
    multipart_upload_id = UploadId,
    limit = Limit,
    part_marker = PartMarker
}) ->
    multipart_upload_req:list_parts(UploadId, Limit, PartMarker);
handle_multipart_upload_request(UserCtx, #abort_multipart_upload{multipart_upload_id = UploadId}) ->
    multipart_upload_req:abort(UserCtx, UploadId);
handle_multipart_upload_request(UserCtx, #complete_multipart_upload{multipart_upload_id = UploadId}) ->
    multipart_upload_req:complete(UserCtx, UploadId);
handle_multipart_upload_request(UserCtx, #list_multipart_uploads{
    space_id = SpaceId,
    limit = Limit,
    index_token = IndexToken
}) ->
    multipart_upload_req:list(UserCtx, SpaceId, Limit, IndexToken).


-spec schedule_provider_restart_report_to_atm_workflow_execution_layer() -> ok.
schedule_provider_restart_report_to_atm_workflow_execution_layer() ->
    schedule(
        ?REPORT_PROVIDER_RESTART_TO_ATM_WORKFLOW_EXECUTION_LAYER,
        ?REPORT_PROVIDER_RESTART_TO_ATM_WORKFLOW_EXECUTION_LAYER_DELAY
    ).

-spec schedule_rerun_transfers() -> ok.
schedule_rerun_transfers() ->
    schedule(?RERUN_TRANSFERS, ?RERUN_TRANSFERS_DELAY).

-spec schedule_restart_autocleaning_runs() -> ok.
schedule_restart_autocleaning_runs() ->
    schedule(?RESTART_AUTOCLEANING_RUNS, ?RESTART_AUTOCLEANING_RUNS_DELAY).

-spec schedule_periodical_spaces_autocleaning_check() -> ok.
schedule_periodical_spaces_autocleaning_check() ->
    schedule(?PERIODICAL_SPACES_AUTOCLEANING_CHECK, ?AUTOCLEANING_PERIODICAL_SPACES_CHECK_INTERVAL).

schedule_init_effective_caches(Space) ->
    schedule(?INIT_EFFECTIVE_CACHES(Space), 0).

-spec schedule(term(), non_neg_integer()) -> ok.
schedule(Request, Timeout) ->
    erlang:send_after(Timeout, ?MODULE, {sync_timer, Request}),
    ok.


-spec periodical_spaces_autocleaning_check() -> ok.
periodical_spaces_autocleaning_check() ->
    try provider_logic:get_spaces() of
        {ok, SpaceIds} ->
            MyNode = node(),
            lists:foreach(fun(SpaceId) ->
                case datastore_key:any_responsible_node(SpaceId) of
                    MyNode -> autocleaning_api:check(SpaceId);
                    _ -> ok
                end
            end, SpaceIds);
        ?ERROR_UNREGISTERED_ONEPROVIDER ->
            ?debug("Skipping spaces cleanup due to unregistered provider");
        ?ERROR_NO_CONNECTION_TO_ONEZONE ->
            ?debug("Skipping spaces cleanup due to no connection to Onezone");
        Error = {error, _} ->
            ?error("Unable to trigger spaces auto-cleaning check due to: ~p", [Error])
    catch
        Error2:Reason:Stacktrace ->
            ?error_stacktrace("Unable to trigger spaces auto-cleaning check due to: ~p", [{Error2, Reason}], Stacktrace)
    end.

-spec report_provider_restart_to_atm_workflow_execution() -> ok.
report_provider_restart_to_atm_workflow_execution() ->
    try provider_logic:get_spaces() of
        {ok, SpaceIds} ->
            lists:foreach(fun atm_workflow_execution_api:report_provider_restart/1, SpaceIds);
        ?ERROR_UNREGISTERED_ONEPROVIDER ->
            schedule_provider_restart_report_to_atm_workflow_execution_layer();
        ?ERROR_NO_CONNECTION_TO_ONEZONE ->
            schedule_provider_restart_report_to_atm_workflow_execution_layer();
        Error = {error, _} ->
            ?error(
                "Unable to report provider restart to atm workflow execution layer due to: ~p",
                [Error]
            )
    catch Class:Reason:Stacktrace ->
        ?error_stacktrace(
            "Unable to report provider restart to atm workflow execution layer due to: ~p",
            [{Class, Reason}],
            Stacktrace
        )
    end.

-spec rerun_transfers() -> ok.
rerun_transfers() ->
    case ?SHOULD_RERUN_TRANSFERS of
        true ->
            try provider_logic:get_spaces() of
                {ok, SpaceIds} ->
                    lists:foreach(fun(SpaceId) ->
                        Restarted = transfer:rerun_not_ended_transfers(SpaceId),
                        ?debug("Restarted following transfers: ~p", [Restarted])
                    end, SpaceIds);
                ?ERROR_UNREGISTERED_ONEPROVIDER ->
                    schedule_rerun_transfers();
                ?ERROR_NO_CONNECTION_TO_ONEZONE ->
                    schedule_rerun_transfers();
                Error = {error, _} ->
                    ?error("Unable to rerun transfers due to: ~p", [Error])
            catch
                Error2:Reason:Stacktrace ->
                    ?error_stacktrace("Unable to rerun transfers due to: ~p", [{Error2, Reason}], Stacktrace)
            end;
        false ->
            ok
    end.

-spec restart_autocleaning_runs() -> ok.
restart_autocleaning_runs() ->
    case ?SHOULD_RESTART_AUTOCLEANING_RUNS of
        true ->
            try provider_logic:get_spaces() of
                {ok, SpaceIds} ->
                    lists:foreach(fun(SpaceId) ->
                        autocleaning_api:restart_autocleaning_run(SpaceId)
                    end, SpaceIds);
                ?ERROR_UNREGISTERED_ONEPROVIDER ->
                    schedule_restart_autocleaning_runs();
                ?ERROR_NO_CONNECTION_TO_ONEZONE ->
                    schedule_restart_autocleaning_runs();
                Error = {error, _} ->
                    ?error("Unable to restart auto-cleaning runs due to: ~p", [Error])
            catch
                Error2:Reason:Stacktrace ->
                    ?error_stacktrace("Unable to restart autocleaning-runs due to: ~p", [{Error2, Reason}], Stacktrace)
            end;
        false ->
            ok
    end.
