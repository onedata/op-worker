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

-export_type([file/0, ext_file/0, open_flag/0, posix_permissions/0,
    file_guid/0, file_guid_or_path/0]).

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

    lists:foreach(fun({Fun, Args}) ->
        case apply(Fun, Args) of
            {ok, _} -> ok;
            {error, already_exists} -> ok
        end
    end, [
        {fun subscription:create/1, [fslogic_subscriptions:file_read_subscription()]},
        {fun subscription:create/1, [fslogic_subscriptions:file_written_subscription()]},
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
    ?debug("proxyio_request(~p): ~p", [SessId, ProxyIORequest]),
    Response = handle_request_and_process_response(SessId, ProxyIORequest),
    ?debug("proxyio_response: ~p", [Response]),
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
        Ctx = user_ctx:new(SessId),
        process_response(Ctx, Request, Response)
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
    Ctx = user_ctx:new(SessId),
    File = fslogic_request:get_file(Ctx, Request),
    {File2, Request2} = fslogic_request:update_target_guid_if_file_is_phantom(File, Request),
    Providers = fslogic_request:get_target_providers(Ctx, File2, Request2),

    case lists:member(oneprovider:get_provider_id(), Providers) of
        true ->
            File3 = file_ctx:fill_guid(File2),
            handle_request_locally(Ctx, Request2, File3);
        false ->
            handle_request_remotely(Ctx, Request2, Providers)
    end.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Handle request locally, as it operates on locally supported entity.
%% @end
%%--------------------------------------------------------------------
-spec handle_request_locally(user_ctx:ctx(), request(), file_ctx:ctx()) -> response().
handle_request_locally(Ctx, Req = #fuse_request{}, File)  ->
    handle_fuse_request(Ctx, Req, File);
handle_request_locally(Ctx, Req = #provider_request{}, File)  ->
    handle_provider_request(Ctx, Req, File);
handle_request_locally(Ctx, Req = #proxyio_request{}, File)  ->
    handle_proxyio_request(Ctx, Req, File).

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Handle request remotely
%% @end
%%--------------------------------------------------------------------
-spec handle_request_remotely(user_ctx:ctx(), request(), [od_provider:id()]) -> response().
handle_request_remotely(Ctx, Req, Providers)  ->
    ProviderId = fslogic_remote:get_provider_to_reroute(Providers),
    fslogic_remote:reroute(Ctx, ProviderId, Req).

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Processes a FUSE request and returns a response.
%% @end
%%--------------------------------------------------------------------
-spec handle_fuse_request(user_ctx:ctx(), fuse_request(), file_ctx:ctx()) ->
    fuse_response().
handle_fuse_request(Ctx, #fuse_request{fuse_request = #resolve_guid{}}, File) ->
    guid_req:resolve_guid(Ctx, File);
handle_fuse_request(Ctx, #fuse_request{fuse_request = #get_helper_params{storage_id = SID,
    force_proxy_io = ForceProxy}}, undefined) ->
    storage_req:get_helper_params(Ctx, SID, ForceProxy);
handle_fuse_request(Ctx, #fuse_request{fuse_request = #create_storage_test_file{file_uuid = Guid,
    storage_id = StorageId}}, undefined) ->
    storage_req:create_storage_test_file(Ctx, Guid, StorageId);
handle_fuse_request(Ctx, #fuse_request{fuse_request = #verify_storage_test_file{space_id = SpaceId,
    storage_id = StorageId, file_id = FileId, file_content = FileContent}}, undefined) ->
    storage_req:verify_storage_test_file(Ctx, SpaceId, StorageId, FileId, FileContent);
handle_fuse_request(Ctx, #fuse_request{fuse_request = #file_request{} = FileRequest}, File) ->
    handle_file_request(Ctx, FileRequest, File).

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Processes a file request and returns a response.
%% @end
%%--------------------------------------------------------------------
-spec handle_file_request(user_ctx:ctx(), #file_request{}, file_ctx:ctx()) ->
    fuse_response().
handle_file_request(Ctx, #file_request{file_request = #get_file_attr{}}, File) ->
    attr_req:get_file_attr(Ctx, File);
handle_file_request(Ctx, #file_request{file_request = #get_child_attr{name = Name}}, ParentFile) ->
    attr_req:get_child_attr(Ctx, ParentFile, Name);
handle_file_request(Ctx, #file_request{file_request = #change_mode{mode = Mode}}, File) ->
    attr_req:chmod(Ctx, File, Mode);
handle_file_request(Ctx, #file_request{file_request = #update_times{atime = ATime, mtime = MTime, ctime = CTime}}, File) ->
    attr_req:update_times(Ctx, File, ATime, MTime, CTime);
handle_file_request(Ctx, #file_request{file_request = #delete_file{silent = Silent}}, File) ->
    delete_req:delete(Ctx, File, Silent);
handle_file_request(Ctx, #file_request{file_request = #create_dir{name = Name, mode = Mode}}, ParentFile) ->
    dir_req:mkdir(Ctx, ParentFile, Name, Mode);
handle_file_request(Ctx, #file_request{file_request = #get_file_children{offset = Offset, size = Size}}, File) ->
    dir_req:read_dir(Ctx, File, Offset, Size);
handle_file_request(Ctx, #file_request{file_request = #rename{target_parent_uuid = TargetParentGuid, target_name = TargetName}}, SourceFile) ->
    TargetParentFile = file_ctx:new_by_guid(TargetParentGuid),
    rename_req:rename(Ctx, SourceFile, TargetParentFile, TargetName);
handle_file_request(Ctx, #file_request{file_request = #create_file{name = Name, flag = Flag, mode = Mode}}, ParentFile) ->
    file_req:create_file(Ctx, ParentFile, Name, Mode, Flag);
handle_file_request(Ctx, #file_request{file_request = #make_file{name = Name, mode = Mode}}, ParentFile) ->
    file_req:make_file(Ctx, ParentFile, Name, Mode);
handle_file_request(Ctx, #file_request{file_request = #open_file{flag = Flag}}, File) ->
    file_req:open_file(Ctx, File, Flag);
handle_file_request(Ctx, #file_request{file_request = #release{handle_id = HandleId}}, File) ->
    file_req:release(Ctx, File, HandleId);
handle_file_request(Ctx, #file_request{file_request = #get_file_location{}}, File) ->
    file_req:get_file_location(Ctx, File);
handle_file_request(Ctx, #file_request{file_request = #truncate{size = Size}}, File) ->
    truncate_req:truncate(Ctx, File, Size);
handle_file_request(Ctx, #file_request{file_request = #synchronize_block{block = Block, prefetch = Prefetch}}, File) ->
    synchronization_req:synchronize_block(Ctx, File, Block, Prefetch);
handle_file_request(Ctx, #file_request{file_request = #synchronize_block_and_compute_checksum{block = Block}}, File) ->
    synchronization_req:synchronize_block_and_compute_checksum(Ctx, File, Block).

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Processes provider request and returns a response.
%% @end
%%--------------------------------------------------------------------
-spec handle_provider_request(user_ctx:ctx(), provider_request(), file_ctx:ctx()) ->
    provider_response().
handle_provider_request(Ctx, #provider_request{provider_request = #get_file_distribution{}}, File) ->
    synchronization_req:get_file_distribution(Ctx, File);
handle_provider_request(Ctx, #provider_request{provider_request = #replicate_file{block = Block}}, File) ->
    synchronization_req:replicate_file(Ctx, File, Block);
handle_provider_request(Ctx, #provider_request{provider_request = #get_parent{}}, File) ->
    guid_req:get_parent(Ctx, File);
handle_provider_request(Ctx, #provider_request{provider_request = #get_file_path{}}, File) ->
    guid_req:get_file_path(Ctx, File);
handle_provider_request(Ctx, #provider_request{provider_request = #get_xattr{name = XattrName, inherited = Inherited}}, File) ->
    xattr_req:get_xattr(Ctx, File, XattrName, Inherited);
handle_provider_request(Ctx, #provider_request{provider_request = #set_xattr{xattr = Xattr}}, File) ->
    xattr_req:set_xattr(Ctx, File, Xattr);
handle_provider_request(Ctx, #provider_request{provider_request = #remove_xattr{name = XattrName}}, File) ->
    xattr_req:remove_xattr(Ctx, File, XattrName);
handle_provider_request(Ctx, #provider_request{provider_request = #list_xattr{inherited = Inherited, show_internal = ShowInternal}}, File) ->
    xattr_req:list_xattr(Ctx, File, Inherited, ShowInternal);
handle_provider_request(Ctx, #provider_request{provider_request = #get_acl{}}, File) ->
    acl_req:get_acl(Ctx, File);
handle_provider_request(Ctx, #provider_request{provider_request = #set_acl{acl = Acl}}, File) ->
    acl_req:set_acl(Ctx, File, Acl);
handle_provider_request(Ctx, #provider_request{provider_request = #remove_acl{}}, File) ->
    acl_req:remove_acl(Ctx, File);
handle_provider_request(Ctx, #provider_request{provider_request = #get_transfer_encoding{}}, File) ->
    cdmi_metadata_req:get_transfer_encoding(Ctx, File);
handle_provider_request(Ctx, #provider_request{provider_request = #set_transfer_encoding{value = Value}}, File) ->
    cdmi_metadata_req:set_transfer_encoding(Ctx, File, Value);
handle_provider_request(Ctx, #provider_request{provider_request = #get_cdmi_completion_status{}}, File) ->
    cdmi_metadata_req:get_cdmi_completion_status(Ctx, File);
handle_provider_request(Ctx, #provider_request{provider_request = #set_cdmi_completion_status{value = Value}}, File) ->
    cdmi_metadata_req:set_cdmi_completion_status(Ctx, File, Value);
handle_provider_request(Ctx, #provider_request{provider_request = #get_mimetype{}}, File) ->
    cdmi_metadata_req:get_mimetype(Ctx, File);
handle_provider_request(Ctx, #provider_request{provider_request = #set_mimetype{value = Value}}, File) ->
    cdmi_metadata_req:set_mimetype(Ctx, File, Value);
handle_provider_request(Ctx, #provider_request{provider_request = #get_metadata{type = Type, names = Names, inherited = Inherited}}, File) ->
    metadata_req:get_metadata(Ctx, File, Type, Names, Inherited);
handle_provider_request(Ctx, #provider_request{provider_request = #set_metadata{metadata = #metadata{type = Type, value = Value}, names = Names}}, File) ->
    metadata_req:set_metadata(Ctx, File, Type, Value, Names);
handle_provider_request(Ctx, #provider_request{provider_request = #remove_metadata{type = Type}}, File) ->
    metadata_req:remove_metadata(Ctx, File, Type);
handle_provider_request(Ctx, #provider_request{provider_request = #check_perms{flag = Flag}}, File) ->
    permission_req:check_perms(Ctx, File, Flag);
handle_provider_request(Ctx, #provider_request{provider_request = #create_share{name = Name}}, File) ->
    share_req:create_share(Ctx, File, Name);
handle_provider_request(Ctx, #provider_request{provider_request = #remove_share{}}, File) ->
    share_req:remove_share(Ctx, File);
handle_provider_request(_Ctx, Req, _File) ->
    ?log_bad_request(Req),
    erlang:error({invalid_request, Req}).

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Processes proxyio request and returns a response.
%% @end
%%--------------------------------------------------------------------
-spec handle_proxyio_request(user_ctx:ctx(), proxyio_request(), file_ctx:ctx()) ->
    proxyio_response().
handle_proxyio_request(Ctx, #proxyio_request{
    storage_id = SID,
    file_id = FID,
    proxyio_request = #remote_write{byte_sequence = ByteSequences},
    parameters = Parameters
}, File) ->
    HandleId = maps:get(?PROXYIO_PARAMETER_HANDLE_ID, Parameters, undefined),
    read_write_req:write(Ctx, File, HandleId, SID, FID, ByteSequences);
handle_proxyio_request(Ctx, #proxyio_request{
    parameters = Parameters,
    storage_id = SID,
    file_id = FID,
    proxyio_request = #remote_read{offset = Offset, size = Size}
}, File) ->
    HandleId = maps:get(?PROXYIO_PARAMETER_HANDLE_ID, Parameters, undefined),
    read_write_req:read(Ctx, File, HandleId, SID, FID, Offset, Size).

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Do posthook for request response
%% @end
%%--------------------------------------------------------------------
-spec process_response(user_ctx:ctx(), request(), response()) -> response().
process_response(Context, #fuse_request{fuse_request = #file_request{file_request = #get_child_attr{name = FileName}, context_guid = ParentGuid}} = Request,
    #fuse_response{status = #status{code = ?ENOENT}} = Response) ->
    SessId = user_ctx:get_session_id(Context),
    {ok, Path0} = fslogic_path:gen_path({uuid, fslogic_uuid:guid_to_uuid(ParentGuid)}, SessId),
    {ok, Tokens0} = fslogic_path:tokenize_skipping_dots(Path0),
    Tokens = Tokens0 ++ [FileName],
    Path = fslogic_path:join(Tokens),
    case fslogic_path:get_canonical_file_entry(Context, Tokens) of
        {path, P} ->
            {ok, Tokens1} = fslogic_path:tokenize_skipping_dots(P),
            case Tokens1 of
                [<<?DIRECTORY_SEPARATOR>>, SpaceId | _] ->
                    Data = #{response => Response, path => Path, ctx => Context, space_id => SpaceId, request => Request},
                    Init = space_sync_worker:init(enoent_handling, SpaceId, undefined, Data),
                    space_sync_worker:run(Init);
                _ -> Response
            end;
        _ ->
            Response
    end;
process_response(Context, #fuse_request{fuse_request = #resolve_guid{path = Path}} = Request,
    #fuse_response{status = #status{code = ?ENOENT}} = Response) ->
    {ok, Tokens} = fslogic_path:tokenize_skipping_dots(Path),
    case fslogic_path:get_canonical_file_entry(Context, Tokens) of
        {path, P} ->
            {ok, Tokens1} = fslogic_path:tokenize_skipping_dots(P),
            case Tokens1 of
                [<<?DIRECTORY_SEPARATOR>>, SpaceId | _] ->
                    Data = #{response => Response, path => Path, ctx => Context, space_id => SpaceId, request => Request},
                    Init = space_sync_worker:init(enoent_handling, SpaceId, undefined, Data),
                    space_sync_worker:run(Init);
                _ -> Response
            end;
        _ ->
            Response
    end;
process_response(_, _, Response) ->
    Response.

