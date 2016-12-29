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
-type open_flag() :: rdwr | write | read.
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
        Ctx = fslogic_context:new(SessId),
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
    Ctx = fslogic_context:new(SessId),
    {File, Ctx2} = fslogic_request:get_file(Ctx, Request),
    {File2, Request2} = fslogic_request:update_target_guid_if_file_is_phantom(File, Request),
    {Providers, Ctx3} = fslogic_request:get_target_providers(Ctx2, File2, Request2),

    case lists:member(oneprovider:get_provider_id(), Providers) of
        true ->
            {Ctx4, _File3} = fslogic_request:update_share_info_in_context(Ctx3, File2),
            handle_request_locally(Ctx4, Request2); %todo pass file to this function
        false ->
            handle_request_remotely(Ctx3, Request2, Providers)
    end.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Handle request locally, as it operates on locally supported entity.
%% @end
%%--------------------------------------------------------------------
-spec handle_request_locally(fslogic_context:ctx(), request()) -> response().
handle_request_locally(Ctx, Req = #fuse_request{})  ->
    handle_fuse_request(Ctx, Req);
handle_request_locally(Ctx, Req = #provider_request{})  ->
    handle_provider_request(Ctx, Req);
handle_request_locally(Ctx, Req = #proxyio_request{})  ->
    handle_proxyio_request(Ctx, Req).

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Handle request remotely
%% @end
%%--------------------------------------------------------------------
-spec handle_request_remotely(fslogic_context:ctx(), request(), [od_provider:id()]) -> response().
handle_request_remotely(Ctx, Req, Providers)  ->
    ProviderId = fslogic_remote:get_provider_to_reroute(Providers),
    fslogic_remote:reroute(Ctx, ProviderId, Req).

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Processes a FUSE request and returns a response.
%% @end
%%--------------------------------------------------------------------
-spec handle_fuse_request(fslogic_context:ctx(), fuse_request()) ->
    fuse_response().
handle_fuse_request(Ctx, #fuse_request{fuse_request = #resolve_guid{path = Path}}) ->
    {ok, Tokens} = fslogic_path:tokenize_skipping_dots(Path),
    CanonicalFileEntry = fslogic_path:get_canonical_file_entry(Ctx, Tokens),
    fslogic_req_generic:get_file_attr(Ctx, CanonicalFileEntry);
handle_fuse_request(Ctx, #fuse_request{fuse_request = #get_helper_params{storage_id = SID, force_proxy_io = ForceProxy}}) ->
    fslogic_req_regular:get_helper_params(Ctx, SID, ForceProxy);
handle_fuse_request(Ctx, #fuse_request{fuse_request = #create_storage_test_file{} = Req}) ->
    SessId = fslogic_context:get_session_id(Ctx),
    fuse_config_manager:create_storage_test_file(SessId, Req);
handle_fuse_request(Ctx, #fuse_request{fuse_request = #verify_storage_test_file{} = Req}) ->
    SessId = fslogic_context:get_session_id(Ctx),
    fuse_config_manager:verify_storage_test_file(SessId, Req);
handle_fuse_request(Ctx, #fuse_request{fuse_request = #file_request{} = FileRequest}) ->
    handle_file_request(Ctx, FileRequest).

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Processes a file request and returns a response.
%% @end
%%--------------------------------------------------------------------
-spec handle_file_request(fslogic_context:ctx(), #file_request{}) ->
    fuse_response().
handle_file_request(Ctx, #file_request{context_guid = Guid, file_request = #get_file_attr{}}) ->
    fslogic_req_generic:get_file_attr(Ctx, {uuid, fslogic_uuid:guid_to_uuid(Guid)});
handle_file_request(Ctx, #file_request{context_guid = Guid, file_request = #get_child_attr{name = Name}}) ->
    fslogic_req_special:get_child_attr(Ctx, {uuid, fslogic_uuid:guid_to_uuid(Guid)}, Name);
handle_file_request(Ctx, #file_request{context_guid = Guid, file_request = #delete_file{silent = Silent}}) ->
    fslogic_req_generic:delete(Ctx, {uuid, fslogic_uuid:guid_to_uuid(Guid)}, Silent);
handle_file_request(Ctx, #file_request{context_guid = ParentGuid, file_request = #create_dir{name = Name, mode = Mode}}) ->
    fslogic_req_special:mkdir(Ctx, {uuid, fslogic_uuid:guid_to_uuid(ParentGuid)}, Name, Mode);
handle_file_request(Ctx, #file_request{context_guid = Guid, file_request = #get_file_children{offset = Offset, size = Size}}) ->
    fslogic_req_special:read_dir(Ctx, {uuid, fslogic_uuid:guid_to_uuid(Guid)}, Offset, Size);
handle_file_request(Ctx, #file_request{context_guid = Guid, file_request = #change_mode{mode = Mode}}) ->
    fslogic_req_generic:chmod(Ctx, {uuid, fslogic_uuid:guid_to_uuid(Guid)}, Mode);
handle_file_request(Ctx, #file_request{context_guid = Guid, file_request = #rename{target_parent_uuid = TargetParentGuid, target_name = TargetName}}) ->
    SessId = fslogic_context:get_session_id(Ctx),
    %% Use lfm_files wrapper for fslogic as the target uuid may not be local
    {ok, TargetParentPath} = lfm_files:get_file_path(SessId, TargetParentGuid),
    TargetPath = fslogic_path:join([<<?DIRECTORY_SEPARATOR>>, TargetParentPath, TargetName]),
    fslogic_rename:rename(Ctx, {uuid, fslogic_uuid:guid_to_uuid(Guid)}, TargetPath);
handle_file_request(Ctx, #file_request{context_guid = Guid, file_request = #update_times{atime = ATime, mtime = MTime, ctime = CTime}}) ->
    fslogic_req_generic:update_times(Ctx, {uuid, fslogic_uuid:guid_to_uuid(Guid)}, ATime, MTime, CTime);
handle_file_request(Ctx, #file_request{context_guid = ParentGuid, file_request = #create_file{name = Name,
    flag = Flag, mode = Mode}}) ->
    fslogic_req_regular:create_file(Ctx, {uuid, fslogic_uuid:guid_to_uuid(ParentGuid)}, Name, Mode, Flag);
handle_file_request(Ctx, #file_request{context_guid = ParentGuid, file_request = #make_file{name = Name, mode = Mode}}) ->
    fslogic_req_regular:make_file(Ctx, {uuid, fslogic_uuid:guid_to_uuid(ParentGuid)}, Name, Mode);
handle_file_request(Ctx, #file_request{context_guid = Guid, file_request = #open_file{flag = Flag}}) ->
    fslogic_req_regular:open_file(Ctx, {uuid, fslogic_uuid:guid_to_uuid(Guid)}, Flag);
handle_file_request(Ctx, #file_request{context_guid = Guid, file_request = #get_file_location{}}) ->
    fslogic_req_regular:get_file_location(Ctx, {uuid, fslogic_uuid:guid_to_uuid(Guid)});
handle_file_request(Ctx, #file_request{context_guid = Guid, file_request = #truncate{size = Size}}) ->
    fslogic_req_regular:truncate(Ctx, {uuid, fslogic_uuid:guid_to_uuid(Guid)}, Size);
handle_file_request(Ctx, #file_request{context_guid = Guid, file_request = #release{handle_id = HandleId}}) ->
    fslogic_req_regular:release(Ctx, fslogic_uuid:guid_to_uuid(Guid), HandleId);
handle_file_request(Ctx, #file_request{context_guid = Guid,
    file_request = #synchronize_block{block = Block, prefetch = Prefetch}}) ->
    fslogic_req_regular:synchronize_block(Ctx, {uuid, fslogic_uuid:guid_to_uuid(Guid)}, Block, Prefetch);
handle_file_request(Ctx, #file_request{context_guid = Guid,
    file_request = #synchronize_block_and_compute_checksum{block = Block}}) ->
    fslogic_req_regular:synchronize_block_and_compute_checksum(Ctx, {uuid, fslogic_uuid:guid_to_uuid(Guid)}, Block).

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Processes provider request and returns a response.
%% @end
%%--------------------------------------------------------------------
-spec handle_provider_request(fslogic_context:ctx(), provider_request()) ->
    provider_response().
handle_provider_request(Ctx, #provider_request{context_guid = Guid, provider_request = #get_parent{}}) ->
    fslogic_req_regular:get_parent(Ctx, {uuid, fslogic_uuid:guid_to_uuid(Guid)});
handle_provider_request(Ctx, #provider_request{context_guid = Guid, provider_request = #get_xattr{name = XattrName, inherited = Inherited}}) ->
    fslogic_req_generic:get_xattr(Ctx, {uuid, fslogic_uuid:guid_to_uuid(Guid)}, XattrName, Inherited);
handle_provider_request(Ctx, #provider_request{context_guid = Guid, provider_request = #set_xattr{xattr = Xattr}}) ->
    fslogic_req_generic:set_xattr(Ctx, {uuid, fslogic_uuid:guid_to_uuid(Guid)}, Xattr);
handle_provider_request(Ctx, #provider_request{context_guid = Guid, provider_request = #remove_xattr{name = XattrName}}) ->
    fslogic_req_generic:remove_xattr(Ctx, {uuid, fslogic_uuid:guid_to_uuid(Guid)}, XattrName);
handle_provider_request(Ctx, #provider_request{context_guid = Guid, provider_request = #list_xattr{inherited = Inherited, show_internal = ShowInternal}}) ->
    fslogic_req_generic:list_xattr(Ctx, {uuid, fslogic_uuid:guid_to_uuid(Guid)}, Inherited, ShowInternal);
handle_provider_request(Ctx, #provider_request{context_guid = Guid, provider_request = #get_acl{}}) ->
    fslogic_req_generic:get_acl(Ctx, {uuid, fslogic_uuid:guid_to_uuid(Guid)});
handle_provider_request(Ctx, #provider_request{context_guid = Guid, provider_request = #set_acl{acl = Acl}}) ->
    fslogic_req_generic:set_acl(Ctx, {uuid, fslogic_uuid:guid_to_uuid(Guid)}, Acl);
handle_provider_request(Ctx, #provider_request{context_guid = Guid, provider_request = #remove_acl{}}) ->
    fslogic_req_generic:remove_acl(Ctx, {uuid, fslogic_uuid:guid_to_uuid(Guid)});
handle_provider_request(Ctx, #provider_request{context_guid = Guid, provider_request = #get_transfer_encoding{}}) ->
    fslogic_req_generic:get_transfer_encoding(Ctx, {uuid, fslogic_uuid:guid_to_uuid(Guid)});
handle_provider_request(Ctx, #provider_request{context_guid = Guid, provider_request = #set_transfer_encoding{value = Value}}) ->
    fslogic_req_generic:set_transfer_encoding(Ctx, {uuid, fslogic_uuid:guid_to_uuid(Guid)}, Value);
handle_provider_request(Ctx, #provider_request{context_guid = Guid, provider_request = #get_cdmi_completion_status{}}) ->
    fslogic_req_generic:get_cdmi_completion_status(Ctx, {uuid, fslogic_uuid:guid_to_uuid(Guid)});
handle_provider_request(Ctx, #provider_request{context_guid = Guid, provider_request = #set_cdmi_completion_status{value = Value}}) ->
    fslogic_req_generic:set_cdmi_completion_status(Ctx, {uuid, fslogic_uuid:guid_to_uuid(Guid)}, Value);
handle_provider_request(Ctx, #provider_request{context_guid = Guid, provider_request = #get_mimetype{}}) ->
    fslogic_req_generic:get_mimetype(Ctx, {uuid, fslogic_uuid:guid_to_uuid(Guid)});
handle_provider_request(Ctx, #provider_request{context_guid = Guid, provider_request = #set_mimetype{value = Value}}) ->
    fslogic_req_generic:set_mimetype(Ctx, {uuid, fslogic_uuid:guid_to_uuid(Guid)}, Value);
handle_provider_request(Ctx, #provider_request{context_guid = Guid, provider_request = #get_file_path{}}) ->
    fslogic_req_generic:get_file_path(Ctx, {uuid, fslogic_uuid:guid_to_uuid(Guid)});
handle_provider_request(Ctx, #provider_request{context_guid = Guid, provider_request = #get_file_distribution{}}) ->
    fslogic_req_regular:get_file_distribution(Ctx, {uuid, fslogic_uuid:guid_to_uuid(Guid)});
handle_provider_request(Ctx, #provider_request{context_guid = Guid, provider_request = #replicate_file{block = Block}}) ->
    fslogic_req_generic:replicate_file(Ctx, {uuid, fslogic_uuid:guid_to_uuid(Guid)}, Block);
handle_provider_request(Ctx, #provider_request{context_guid = Guid, provider_request = #get_metadata{type = Type, names = Names, inherited = Inherited}}) ->
    fslogic_req_generic:get_metadata(Ctx, {uuid, fslogic_uuid:guid_to_uuid(Guid)}, Type, Names, Inherited);
handle_provider_request(Ctx, #provider_request{context_guid = Guid, provider_request = #set_metadata{metadata =
#metadata{type = Type, value = Value}, names = Names}}) ->
    fslogic_req_generic:set_metadata(Ctx, {uuid, fslogic_uuid:guid_to_uuid(Guid)}, Type, Value, Names);
handle_provider_request(Ctx, #provider_request{context_guid = Guid, provider_request = #remove_metadata{type = Type}}) ->
    fslogic_req_generic:remove_metadata(Ctx, {uuid, fslogic_uuid:guid_to_uuid(Guid)}, Type);
handle_provider_request(Ctx, #provider_request{context_guid = Guid, provider_request = #check_perms{flag = Flag}}) ->
    fslogic_req_generic:check_perms(Ctx, {uuid, fslogic_uuid:guid_to_uuid(Guid)}, Flag);
handle_provider_request(Ctx, #provider_request{context_guid = Guid, provider_request = #create_share{name = Name}}) ->
    fslogic_req_generic:create_share(Ctx, {uuid, fslogic_uuid:guid_to_uuid(Guid)}, Name);
handle_provider_request(Ctx, #provider_request{context_guid = Guid, provider_request = #remove_share{}}) ->
    fslogic_req_generic:remove_share(Ctx, {uuid, fslogic_uuid:guid_to_uuid(Guid)});
handle_provider_request(Ctx, #provider_request{context_guid = Guid, provider_request = #copy{target_path = TargetPath}}) ->
    fslogic_copy:copy(Ctx, {uuid, fslogic_uuid:guid_to_uuid(Guid)}, TargetPath);
handle_provider_request(_Ctx, Req = #provider_request{context_guid = _Guid, provider_request = #fsync{}}) ->
    erlang:error({invalid_request, Req}). %todo handle fsync

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Processes proxyio request and returns a response.
%% @end
%%--------------------------------------------------------------------
-spec handle_proxyio_request(fslogic_context:ctx(), proxyio_request()) ->
    proxyio_response().
handle_proxyio_request(Ctx, #proxyio_request{
    parameters = Parameters = #{?PROXYIO_PARAMETER_FILE_GUID := FileGuid}, storage_id = SID, file_id = FID,
    proxyio_request = #remote_write{byte_sequence = ByteSequences}}) ->
    SessId = fslogic_context:get_session_id(Ctx),
    ShareId = file_info:get_share_id(Ctx),
    FileUUID = fslogic_uuid:guid_to_uuid(FileGuid),
    fslogic_proxyio:write(SessId, Parameters#{?PROXYIO_PARAMETER_FILE_GUID := FileUUID,
        ?PROXYIO_PARAMETER_SHARE_ID => ShareId}, SID, FID, ByteSequences);
handle_proxyio_request(Ctx, #proxyio_request{
    parameters = Parameters = #{?PROXYIO_PARAMETER_FILE_GUID := FileGuid}, storage_id = SID, file_id = FID,
    proxyio_request = #remote_read{offset = Offset, size = Size}}) ->
    SessId = fslogic_context:get_session_id(Ctx),
    ShareId = file_info:get_share_id(Ctx),
    FileUUID = fslogic_uuid:guid_to_uuid(FileGuid),
    fslogic_proxyio:read(SessId, Parameters#{?PROXYIO_PARAMETER_FILE_GUID := FileUUID,
        ?PROXYIO_PARAMETER_SHARE_ID => ShareId}, SID, FID, Offset, Size);
handle_proxyio_request(_CTX, Req) ->
    ?log_bad_request(Req),
    erlang:error({invalid_request, Req}).

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Do posthook for request response
%% @end
%%--------------------------------------------------------------------
-spec process_response(fslogic_context:ctx(), request(), response()) -> response().
process_response(Context, #fuse_request{fuse_request = #file_request{file_request = #get_child_attr{name = FileName}, context_guid = ParentGuid}} = Request,
    #fuse_response{status = #status{code = ?ENOENT}} = Response) ->
    SessId = fslogic_context:get_session_id(Context),
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

