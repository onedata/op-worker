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
-export([init_paths_caches/1]).
-export([init/1, handle/1, cleanup/0]).
-export([init_counters/0, init_report/0]).

% exported for RPC
-export([schedule_init_paths_caches/1]).

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
-type ext_file() :: file_meta:entry() | {guid, file_guid()}.
-type open_flag() :: helpers:open_flag().
-type posix_permissions() :: file_meta:posix_permissions().
-type file_guid() :: file_id:file_guid().
-type file_guid_or_path() :: {guid, file_guid()} | {path, file_meta:path()}.

-export_type([request/0, response/0, file/0, ext_file/0, open_flag/0, posix_permissions/0,
    file_guid/0, file_guid_or_path/0, fuse_response/0, provider_response/0, proxyio_response/0, fuse_response_type/0]).

% requests
-define(INVALIDATE_PERMISSIONS_CACHE, invalidate_permissions_cache).
-define(PERIODICAL_SPACES_AUTOCLEANING_CHECK, periodical_spaces_autocleaning_check).
-define(RERUN_TRANSFERS, rerun_transfers).
-define(RESTART_AUTOCLEANING_RUNS, restart_autocleaning_runs).
-define(INIT_PATHS_CACHES(Space), {init_paths_caches, Space}).

-define(SHOULD_PERFORM_PERIODICAL_SPACES_AUTOCLEANING_CHECK,
    application:get_env(?APP_NAME, autocleaning_periodical_spaces_check_enabled, true)).

% delays and intervals
-define(INVALIDATE_PERMISSIONS_CACHE_INTERVAL,
    application:get_env(?APP_NAME, invalidate_permissions_cache_interval, timer:seconds(30))).
-define(AUTOCLEANING_PERIODICAL_SPACES_CHECK_INTERVAL,
    application:get_env(?APP_NAME, autocleaning_periodical_spaces_check_interval, timer:minutes(1))).
-define(RERUN_TRANSFERS_DELAY,
    application:get_env(?APP_NAME, rerun_transfers_delay, 10000)).
-define(RESTART_AUTOCLEANING_RUNS_DELAY,
    application:get_env(?APP_NAME, restart_autocleaning_runs_delay, 10000)).

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
-define(SHOULD_RERUN_TRANSFERS, application:get_env(?APP_NAME, rerun_transfers, true)).

% This macro is used to disable automatic restart of autocleaning runs in tests
-define(SHOULD_RESTART_AUTOCLEANING_RUNS, application:get_env(?APP_NAME, autocleaning_restart_runs, true)).

-define(AVAILABLE_SHARE_OPERATIONS, [
    check_perms,
    get_parent,
    % TODO VFS-6057 resolve share path up to share not user root dir
    %%    get_file_path,

    list_xattr,
    get_xattr,
    get_metadata,

    open_file,
    open_file_with_extended_info,
    synchronize_block,
    remote_read,
    fsync,
    release,

    get_file_attr,
    get_file_details,
    get_file_children,
    get_child_attr,
    get_file_children_attrs,
    get_file_children_details
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
        file_upload_manager:spec(),
        transfer_onf_stats_aggregator:spec()
    ].

%%--------------------------------------------------------------------
%% @doc
%% Initializes paths caches on all nodes.
%% @end
%%--------------------------------------------------------------------
-spec init_paths_caches(od_space:id() | all) -> ok.
init_paths_caches(Space) ->
    lists:foreach(fun(Node) ->
        rpc:call(Node, ?MODULE, schedule_init_paths_caches, [Space])
    end, consistent_hashing:get_all_nodes()).

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
    paths_cache:init_group(),
    schedule_init_paths_caches(all),

    transfer:init(),
    replica_deletion_master:init_workers_pool(),
    file_registration:init_pool(),
    autocleaning_view_traverse:init_pool(),
    tree_deletion_traverse:init_pool(),
    clproto_serializer:load_msg_defs(),

    schedule_invalidate_permissions_cache(),
    schedule_rerun_transfers(),
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
handle(?INVALIDATE_PERMISSIONS_CACHE) ->
    ?debug("Invalidating permissions cache"),
    invalidate_permissions_cache(),
    schedule_invalidate_permissions_cache();
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
handle(?INIT_PATHS_CACHES(Space)) ->
    paths_cache:init_caches(Space);
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
    replica_synchronizer:terminate_all(),
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
        Type2:Error2 ->
            fslogic_errors:handle_error(Request, Type2, Error2)
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
    try
        UserCtx1 = case ShareId of
            undefined ->
                UserCtx0;
            _ ->
                Operation = get_operation(Request),
                case lists:member(Operation, ?AVAILABLE_SHARE_OPERATIONS) of
                    true -> ok;
                    false -> throw(?EACCES)
                end,
                % Operations concerning shares must be carried with GUEST auth
                case user_ctx:is_guest(UserCtx0) of
                    true -> UserCtx0;
                    false -> user_ctx:new(?GUEST_SESS_ID)
                end
        end,
        handle_request_locally(UserCtx1, Request, FileCtx1)
    catch
        Type:Error ->
            fslogic_errors:handle_error(Request, Type, Error)
    end.

%% @private
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
    file_request = Req}}, FileCtx) ->
    [ReqName | _] = tuple_to_list(Req),
    ?update_counter(?EXOMETER_NAME(ReqName)),
    Stopwatch = stopwatch:start(),
    Ans = handle_file_request(UserCtx, Req, FileCtx),
    ?update_counter(?EXOMETER_TIME_NAME(ReqName), stopwatch:read_micros(Stopwatch)),
    Ans;
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
    ProviderId = fslogic_remote:get_provider_to_reroute(Providers),
    fslogic_remote:reroute(UserCtx, ProviderId, Req).

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
handle_file_request(UserCtx, #get_file_attr{include_replication_status = IncludeReplicationStatus,
    include_link_count = IncludeLinkCount}, FileCtx) ->
    attr_req:get_file_attr(UserCtx, FileCtx, IncludeReplicationStatus, IncludeLinkCount);
handle_file_request(UserCtx, #get_file_details{}, FileCtx) ->
    attr_req:get_file_details(UserCtx, FileCtx);
handle_file_request(UserCtx, #get_child_attr{name = Name,
    include_replication_status = IncludeReplicationStatus, include_link_count = IncludeLinkCount}, ParentFileCtx) ->
    attr_req:get_child_attr(UserCtx, ParentFileCtx, Name, IncludeReplicationStatus, IncludeLinkCount);
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
handle_file_request(UserCtx, #get_file_children{
    offset = Offset,
    size = Size,
    index_token = Token,
    index_startid = StartId
}, FileCtx) ->
    ListOpts = #{
        offset => Offset,
        size => Size,
        token => Token,
        last_name => StartId
    },
    dir_req:get_children(UserCtx, FileCtx, ListOpts);
handle_file_request(UserCtx, #get_file_children_attrs{offset = Offset, size = Size, index_token = Token,
    include_replication_status = IncludeReplicationStatus, include_link_count = IncludeLinkCount}, FileCtx) ->
    ListOpts = #{
        offset => Offset,
        size => Size,
        token => Token
    },
    dir_req:get_children_attrs(UserCtx, FileCtx, ListOpts, IncludeReplicationStatus, IncludeLinkCount);
handle_file_request(UserCtx, #get_file_children_details{
    offset = Offset,
    size = Size,
    index_startid = StartId
}, FileCtx) ->
    ListOpts = #{
        offset => Offset,
        size => Size,
        last_name => StartId
    },
    dir_req:get_children_details(UserCtx, FileCtx, ListOpts);
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
    file_req:read_symlink(UserCtx, FileCtx);
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
    file_req:fsync(UserCtx, FileCtx, DataOnly, HandleId).

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Processes provider request and returns a response.
%% @end
%%--------------------------------------------------------------------
-spec handle_provider_request(user_ctx:ctx(), provider_request_type(), file_ctx:ctx()) ->
    provider_response().
handle_provider_request(UserCtx, #get_file_distribution{}, FileCtx) ->
    sync_req:get_file_distribution(UserCtx, FileCtx);
handle_provider_request(UserCtx, #schedule_file_transfer{
    replicating_provider_id = ReplicatingProviderId,
    evicting_provider_id = EvictingProviderId,
    callback = Callback
}, FileCtx) ->
    transfer_req:schedule_file_transfer(
        UserCtx, FileCtx,
        ReplicatingProviderId, EvictingProviderId,
        Callback
    );
handle_provider_request(UserCtx, #schedule_view_transfer{
    replicating_provider_id = ReplicatingProviderId,
    evicting_provider_id = EvictingProviderId,
    view_name = ViewName,
    query_view_params = QueryViewParams,
    callback = Callback
}, FileCtx) ->
    transfer_req:schedule_view_transfer(
        UserCtx, FileCtx,
        ReplicatingProviderId, EvictingProviderId,
        ViewName, QueryViewParams,
        Callback
    );
handle_provider_request(UserCtx, #schedule_file_replication{
    block = _Block, target_provider_id = TargetProviderId, callback = Callback,
    view_name = ViewName, query_view_params = QueryViewParams
}, FileCtx) ->
    sync_req:schedule_file_replication(UserCtx, FileCtx, TargetProviderId,
        Callback, ViewName, QueryViewParams
    );
handle_provider_request(UserCtx, #schedule_replica_invalidation{
    source_provider_id = SourceProviderId, target_provider_id = TargetProviderId,
    view_name = ViewName, query_view_params = QueryViewParams
}, FileCtx) ->
    replica_eviction_req:schedule_replica_eviction(UserCtx, FileCtx,
        SourceProviderId, TargetProviderId, ViewName, QueryViewParams
    );
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
handle_provider_request(UserCtx, #get_transfer_encoding{}, FileCtx) ->
    cdmi_metadata_req:get_transfer_encoding(UserCtx, FileCtx);
handle_provider_request(UserCtx, #set_transfer_encoding{value = Value}, FileCtx) ->
    cdmi_metadata_req:set_transfer_encoding(UserCtx, FileCtx, Value, false, false);
handle_provider_request(UserCtx, #get_cdmi_completion_status{}, FileCtx) ->
    cdmi_metadata_req:get_cdmi_completion_status(UserCtx, FileCtx);
handle_provider_request(UserCtx, #set_cdmi_completion_status{value = Value}, FileCtx) ->
    cdmi_metadata_req:set_cdmi_completion_status(UserCtx, FileCtx, Value, false, false);
handle_provider_request(UserCtx, #get_mimetype{}, FileCtx) ->
    cdmi_metadata_req:get_mimetype(UserCtx, FileCtx);
handle_provider_request(UserCtx, #set_mimetype{value = Value}, FileCtx) ->
    cdmi_metadata_req:set_mimetype(UserCtx, FileCtx, Value, false, false);
handle_provider_request(UserCtx, #get_metadata{
    type = Type,
    query = Query,
    inherited = Inherited
}, FileCtx) ->
    metadata_req:get_metadata(UserCtx, FileCtx, Type, Query, Inherited);
handle_provider_request(UserCtx, #set_metadata{
    metadata = #metadata{type = Type, value = Value},
    query = Query
}, FileCtx) ->
    metadata_req:set_metadata(UserCtx, FileCtx, Type, Value, Query, false, false);
handle_provider_request(UserCtx, #remove_metadata{type = Type}, FileCtx) ->
    metadata_req:remove_metadata(UserCtx, FileCtx, Type);
handle_provider_request(UserCtx, #check_perms{flag = Flag}, FileCtx) ->
    permission_req:check_perms(UserCtx, FileCtx, Flag);
handle_provider_request(UserCtx, #create_share{name = Name, description = Description}, FileCtx) ->
    share_req:create_share(UserCtx, FileCtx, Name, Description);
handle_provider_request(UserCtx, #remove_share{share_id = ShareId}, FileCtx) ->
    share_req:remove_share(UserCtx, FileCtx, ShareId);
handle_provider_request(UserCtx, #add_qos_entry{
    expression = Expression, replicas_num = ReplicasNum, entry_type = EntryType
}, FileCtx) ->
    qos_req:add_qos_entry(UserCtx, FileCtx, Expression, ReplicasNum, EntryType);
handle_provider_request(UserCtx, #get_effective_file_qos{}, FileCtx) ->
    qos_req:get_effective_file_qos(UserCtx, FileCtx);
handle_provider_request(UserCtx, #get_qos_entry{id = QosEntryId}, FileCtx) ->
    qos_req:get_qos_entry(UserCtx, FileCtx, QosEntryId);
handle_provider_request(UserCtx, #remove_qos_entry{id = QosEntryId}, FileCtx) ->
    qos_req:remove_qos_entry(UserCtx, FileCtx, QosEntryId);
handle_provider_request(UserCtx, #check_qos_status{qos_id = QosEntryId}, FileCtx) ->
    qos_req:check_status(UserCtx, FileCtx, QosEntryId).

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


-spec schedule_invalidate_permissions_cache() -> ok.
schedule_invalidate_permissions_cache() ->
    schedule(?INVALIDATE_PERMISSIONS_CACHE, ?INVALIDATE_PERMISSIONS_CACHE_INTERVAL).

-spec schedule_rerun_transfers() -> ok.
schedule_rerun_transfers() ->
    schedule(?RERUN_TRANSFERS, ?RERUN_TRANSFERS_DELAY).

-spec schedule_restart_autocleaning_runs() -> ok.
schedule_restart_autocleaning_runs() ->
    schedule(?RESTART_AUTOCLEANING_RUNS, ?RESTART_AUTOCLEANING_RUNS_DELAY).

-spec schedule_periodical_spaces_autocleaning_check() -> ok.
schedule_periodical_spaces_autocleaning_check() ->
    schedule(?PERIODICAL_SPACES_AUTOCLEANING_CHECK, ?AUTOCLEANING_PERIODICAL_SPACES_CHECK_INTERVAL).

schedule_init_paths_caches(Space) ->
    schedule(?INIT_PATHS_CACHES(Space), 0).

-spec schedule(term(), non_neg_integer()) -> ok.
schedule(Request, Timeout) ->
    erlang:send_after(Timeout, ?MODULE, {sync_timer, Request}),
    ok.

-spec invalidate_permissions_cache() -> ok.
invalidate_permissions_cache() ->
    try
        permissions_cache:invalidate_on_node()
    catch
        _:Reason ->
            ?error_stacktrace("Failed to invalidate permissions cache due to: ~p", [Reason])
    end.

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
        Error2:Reason ->
            ?error_stacktrace("Unable to trigger spaces auto-cleaning check due to: ~p", [{Error2, Reason}])
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
                Error2:Reason ->
                    ?error_stacktrace("Unable to rerun transfers due to: ~p", [{Error2, Reason}])
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
                Error2:Reason ->
                    ?error_stacktrace("Unable to restart autocleaning-runs due to: ~p", [{Error2, Reason}])
            end;
        false ->
            ok
    end.
