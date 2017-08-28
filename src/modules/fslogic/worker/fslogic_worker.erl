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
%%% to other priovider.
%%% @end
%%%--------------------------------------------------------------------
-module(fslogic_worker).
-behaviour(worker_plugin_behaviour).

-include("global_definitions.hrl").
-include("proto/oneclient/proxyio_messages.hrl").
-include("proto/oneprovider/provider_messages.hrl").
-include("modules/events/definitions.hrl").
-include_lib("ctool/include/logging.hrl").

-export([init/1, handle/1, cleanup/0]).

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
-type file_guid() :: binary().
-type file_guid_or_path() :: {guid, file_guid()} | {path, file_meta:path()}.

-export_type([request/0, response/0, file/0, ext_file/0, open_flag/0,
    posix_permissions/0, file_guid/0, file_guid_or_path/0]).

-define(INVALIDATE_PERMISSIONS_CACHE_INTERVAL, application:get_env(?APP_NAME,
    invalidate_permissions_cache_interval, timer:seconds(30))).

-define(SPACES_CLEANUP_INTERVAL, application:get_env(?APP_NAME,
    spaces_cleanup_interval, timer:hours(1))).

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
    case application:get_env(?APP_NAME, start_rtransfer_on_init) of
        {ok, true} -> rtransfer_config:start_rtransfer();
        _ -> ok
    end,

    erlang:send_after(?INVALIDATE_PERMISSIONS_CACHE_INTERVAL, self(),
        {sync_timer, invalidate_permissions_cache}
    ),

    erlang:send_after(?SPACES_CLEANUP_INTERVAL, self(),
        {sync_timer, spaces_cleanup}
    ),

    lists:foreach(fun({Fun, Args}) ->
        case apply(Fun, Args) of
            {ok, _} -> ok;
            {error, already_exists} -> ok
        end
    end, [
        {fun subscription:create/1, [fslogic_event_subscriptions:file_read_subscription()]},
        {fun subscription:create/1, [fslogic_event_subscriptions:file_written_subscription()]},
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
    Result :: nagios_handler:healthcheck_response() | ok | {ok, response()} |
    {error, Reason :: term()} | pong.
handle(ping) ->
    pong;
handle(healthcheck) ->
    ok;
handle(invalidate_permissions_cache) ->
    try
        permissions_cache:invalidate()
    catch
        _:Reason ->
            ?error_stacktrace("Failed to invalidate permissions cache due to: ~p", [Reason])
    end,
    erlang:send_after(?INVALIDATE_PERMISSIONS_CACHE_INTERVAL, self(),
        {sync_timer, invalidate_permissions_cache}
    ),
    ok;
handle(spaces_cleanup) ->
    space_cleanup:periodic_cleanup(),
    erlang:send_after(?INVALIDATE_PERMISSIONS_CACHE_INTERVAL, self(),
        {sync_timer, invalidate_permissions_cache}
    ),
    ok;
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
    ok.

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
    Response = try
        handle_request(SessId, Request)
    catch
        Type:Error ->
            fslogic_errors:handle_error(Request, Type, Error)
    end,

    try %todo TL move this storage_sync logic out of here
        UserCtx = user_ctx:new(SessId),
        process_response(UserCtx, Request, Response)
    catch
        Type2:Error2 ->
            fslogic_errors:handle_error(Request, Type2, Error2)
    end.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Analyze request data and handle it locally or remotely.
%% @end
%%--------------------------------------------------------------------
-spec handle_request(session:id(), request()) -> response().
handle_request(SessId, Request) ->
    UserCtx = user_ctx:new(SessId),
    FilePartialCtx = fslogic_request:get_file_partial_ctx(UserCtx, Request),
    Providers = fslogic_request:get_target_providers(UserCtx, FilePartialCtx, Request),

    case lists:member(oneprovider:get_provider_id(), Providers) of
        true ->
            FileCtx = case FilePartialCtx of
                undefined ->
                    undefined;
                _ ->
                    file_ctx:new_by_partial_context(FilePartialCtx)
            end,
            handle_request_locally(UserCtx, Request, FileCtx);
        false ->
            handle_request_remotely(UserCtx, Request, Providers)
    end.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Handle request locally, as it operates on locally supported entity.
%% @end
%%--------------------------------------------------------------------
-spec handle_request_locally(user_ctx:ctx(), request(), file_ctx:ctx() | undefined) -> response().
handle_request_locally(UserCtx, #fuse_request{fuse_request = #file_request{file_request = Req}}, FileCtx) ->
    handle_file_request(UserCtx, Req, FileCtx);
handle_request_locally(UserCtx, #fuse_request{fuse_request = Req}, FileCtx)  ->
    handle_fuse_request(UserCtx, Req, FileCtx);
handle_request_locally(UserCtx, #provider_request{provider_request = Req}, FileCtx)  ->
    handle_provider_request(UserCtx, Req, FileCtx);
handle_request_locally(UserCtx, #proxyio_request{
    storage_id = StorageId,
    file_id = FileId,
    parameters = Parameters,
    proxyio_request = Req
}, FileCtx)  ->
    HandleId = maps:get(?PROXYIO_PARAMETER_HANDLE_ID, Parameters, undefined),
    handle_proxyio_request(UserCtx, Req, FileCtx, HandleId, StorageId, FileId).

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Handle request remotely
%% @end
%%--------------------------------------------------------------------
-spec handle_request_remotely(user_ctx:ctx(), request(), [od_provider:id()]) -> response().
handle_request_remotely(UserCtx, Req, Providers)  ->
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
handle_fuse_request(UserCtx, #get_helper_params{
    storage_id = SID,
    force_proxy_io = ForceProxy
}, undefined) ->
    storage_req:get_helper_params(UserCtx, SID, ForceProxy);
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
    storage_req:verify_storage_test_file(UserCtx, SpaceId, StorageId, FileId, FileContent).

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Processes a file request and returns a response.
%% @end
%%--------------------------------------------------------------------
-spec handle_file_request(user_ctx:ctx(), file_request_type(), file_ctx:ctx()) ->
    fuse_response().
handle_file_request(UserCtx, #get_file_attr{}, FileCtx) ->
    attr_req:get_file_attr(UserCtx, FileCtx);
handle_file_request(UserCtx, #get_child_attr{name = Name}, ParentFileCtx) ->
    attr_req:get_child_attr(UserCtx, ParentFileCtx, Name);
handle_file_request(UserCtx, #change_mode{mode = Mode}, FileCtx) ->
    attr_req:chmod(UserCtx, FileCtx, Mode);
handle_file_request(UserCtx, #update_times{atime = ATime, mtime = MTime, ctime = CTime}, FileCtx) ->
    attr_req:update_times(UserCtx, FileCtx, ATime, MTime, CTime);
handle_file_request(UserCtx, #delete_file{silent = Silent}, FileCtx) ->
    delete_req:delete(UserCtx, FileCtx, Silent);
handle_file_request(UserCtx, #create_dir{name = Name, mode = Mode}, ParentFileCtx) ->
    dir_req:mkdir(UserCtx, ParentFileCtx, Name, Mode);
handle_file_request(UserCtx, #get_file_children{offset = Offset, size = Size}, FileCtx) ->
    dir_req:read_dir(UserCtx, FileCtx, Offset, Size);
handle_file_request(UserCtx, #get_file_children_attrs{offset = Offset, size = Size}, FileCtx) ->
    dir_req:read_dir_plus(UserCtx, FileCtx, Offset, Size);
handle_file_request(UserCtx, #rename{
    target_parent_guid = TargetParentGuid,
    target_name = TargetName
}, SourceFileCtx) ->
    TargetParentFileCtx = file_ctx:new_by_guid(TargetParentGuid),
    rename_req:rename(UserCtx, SourceFileCtx, TargetParentFileCtx, TargetName);
handle_file_request(UserCtx, #create_file{name = Name, flag = Flag, mode = Mode}, ParentFileCtx) ->
    file_req:create_file(UserCtx, ParentFileCtx, Name, Mode, Flag);
handle_file_request(UserCtx, #make_file{name = Name, mode = Mode}, ParentFileCtx) ->
    file_req:make_file(UserCtx, ParentFileCtx, Name, Mode);
handle_file_request(UserCtx, #open_file{flag = Flag}, FileCtx) ->
    file_req:open_file(UserCtx, FileCtx, Flag);
handle_file_request(UserCtx, #release{handle_id = HandleId}, FileCtx) ->
    file_req:release(UserCtx, FileCtx, HandleId);
handle_file_request(UserCtx, #get_file_location{}, FileCtx) ->
    file_req:get_file_location(UserCtx, FileCtx);
handle_file_request(UserCtx, #truncate{size = Size}, FileCtx) ->
    truncate_req:truncate(UserCtx, FileCtx, Size);
handle_file_request(UserCtx, #synchronize_block{block = Block, prefetch = Prefetch}, FileCtx) ->
    sync_req:synchronize_block(UserCtx, FileCtx, Block, Prefetch);
handle_file_request(UserCtx, #synchronize_block_and_compute_checksum{block = Block}, FileCtx) ->
    sync_req:synchronize_block_and_compute_checksum(UserCtx, FileCtx, Block);
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
handle_provider_request(UserCtx, #replicate_file{block = Block}, FileCtx) ->
    sync_req:replicate_file(UserCtx, FileCtx, Block);
handle_provider_request(UserCtx, #invalidate_file_replica{
    migration_provider_id = MigrationProviderId
}, FileCtx) ->
    sync_req:invalidate_file_replica(UserCtx, FileCtx, MigrationProviderId);
handle_provider_request(UserCtx, #get_parent{}, FileCtx) ->
    guid_req:get_parent(UserCtx, FileCtx);
handle_provider_request(UserCtx, #get_file_path{}, FileCtx) ->
    guid_req:get_file_path(UserCtx, FileCtx);
handle_provider_request(UserCtx, #get_acl{}, FileCtx) ->
    acl_req:get_acl(UserCtx, FileCtx);
handle_provider_request(UserCtx, #set_acl{acl = Acl}, FileCtx) ->
    acl_req:set_acl(UserCtx, FileCtx, Acl, false, false);
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
    names = Names,
    inherited = Inherited
}, FileCtx) ->
    metadata_req:get_metadata(UserCtx, FileCtx, Type, Names, Inherited);
handle_provider_request(UserCtx, #set_metadata{
    metadata = #metadata{type = Type, value = Value},
    names = Names
}, FileCtx) ->
    metadata_req:set_metadata(UserCtx, FileCtx, Type, Value, Names, false, false);
handle_provider_request(UserCtx, #remove_metadata{type = Type}, FileCtx) ->
    metadata_req:remove_metadata(UserCtx, FileCtx, Type);
handle_provider_request(UserCtx, #check_perms{flag = Flag}, FileCtx) ->
    permission_req:check_perms(UserCtx, FileCtx, Flag);
handle_provider_request(UserCtx, #create_share{name = Name}, FileCtx) ->
    share_req:create_share(UserCtx, FileCtx, Name);
handle_provider_request(UserCtx, #remove_share{}, FileCtx) ->
    share_req:remove_share(UserCtx, FileCtx).

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Processes proxyio request and returns a response.
%% @end
%%--------------------------------------------------------------------
-spec handle_proxyio_request(user_ctx:ctx(), proxyio_request_type(), file_ctx:ctx(),
    HandleId :: storage_file_manager:handle_id(), StorageId :: storage:id(),
    FileId :: helpers:file_id()) -> proxyio_response().
handle_proxyio_request(UserCtx, #remote_write{byte_sequence = ByteSequences}, FileCtx,
    HandleId, StorageId, FileId) ->
    read_write_req:write(UserCtx, FileCtx, HandleId, StorageId, FileId, ByteSequences);
handle_proxyio_request(UserCtx, #remote_read{offset = Offset, size = Size}, FileCtx,
    HandleId, StorageId, FileId) ->
    read_write_req:read(UserCtx, FileCtx, HandleId, StorageId, FileId, Offset, Size).

%%--------------------------------------------------------------------
%% @todo refactor
%% @private
%% @doc
%% Do posthook for request response
%% @end
%%--------------------------------------------------------------------
-spec process_response(user_ctx:ctx(), request(), response()) -> response().
process_response(UserCtx, #fuse_request{fuse_request = #file_request{
    file_request = #get_child_attr{name = FileName},
    context_guid = ParentGuid
}} = Request, #fuse_response{status = #status{code = ?ENOENT}} = Response) ->
    SessId = user_ctx:get_session_id(UserCtx),
    Path0 = fslogic_uuid:uuid_to_path(SessId, fslogic_uuid:guid_to_uuid(ParentGuid)),
    {ok, Tokens0} = fslogic_path:split_skipping_dots(Path0),
    Tokens = Tokens0 ++ [FileName],
    Path = fslogic_path:join(Tokens),
    case enoent_handling:get_canonical_file_entry(UserCtx, Tokens) of
        {path, P} ->
            {ok, Tokens1} = fslogic_path:split_skipping_dots(P),
            case Tokens1 of
                [<<?DIRECTORY_SEPARATOR>>, SpaceId | _] ->
                    Data = #{response => Response, path => Path, ctx => UserCtx, space_id => SpaceId, request => Request},
                    Init = space_sync_worker:init(enoent_handling, SpaceId, undefined, Data),
                    space_sync_worker:run(Init);
                _ -> Response
            end;
        _ ->
            Response
    end;
process_response(UserCtx, #fuse_request{fuse_request = #resolve_guid{path = Path}} = Request,
    #fuse_response{status = #status{code = ?ENOENT}} = Response) ->
    {ok, Tokens} = fslogic_path:split_skipping_dots(Path),
    case enoent_handling:get_canonical_file_entry(UserCtx, Tokens) of
        {path, P} ->
            {ok, Tokens1} = fslogic_path:split_skipping_dots(P),
            case Tokens1 of
                [<<?DIRECTORY_SEPARATOR>>, SpaceId | _] ->
                    Data = #{response => Response, path => Path, ctx => UserCtx, space_id => SpaceId, request => Request},
                    Init = space_sync_worker:init(enoent_handling, SpaceId, undefined, Data),
                    space_sync_worker:run(Init);
                _ -> Response
            end;
        _ ->
            Response
    end;
process_response(_, _, Response) ->
    Response.

