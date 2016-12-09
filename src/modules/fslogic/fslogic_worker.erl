%%%--------------------------------------------------------------------
%%% @author Rafal Slota
%%% @copyright (C) 2015 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%--------------------------------------------------------------------
%%% @doc This module implements worker_plugin_behaviour callbacks.
%%%      Also it decides whether request has to be handled locally or rerouted
%%%      to other priovider.
%%% @end
%%%--------------------------------------------------------------------
-module(fslogic_worker).
-behaviour(worker_plugin_behaviour).

-include("global_definitions.hrl").
-include("modules/fslogic/fslogic_common.hrl").
-include("proto/oneclient/fuse_messages.hrl").
-include("proto/oneclient/common_messages.hrl").
-include("proto/oneclient/proxyio_messages.hrl").
-include("proto/oneclient/server_messages.hrl").
-include("proto/oneprovider/provider_messages.hrl").
-include("modules/events/definitions.hrl").
-include("proto/common/credentials.hrl").
-include_lib("ctool/include/oz/oz_spaces.hrl").
-include_lib("ctool/include/posix/errors.hrl").
-include_lib("ctool/include/logging.hrl").
-include_lib("ctool/include/global_definitions.hrl").


-export([init/1, handle/1, cleanup/0, handle_fuse_request/2]).

%%%===================================================================
%%% Types
%%%===================================================================

-type ctx() :: #fslogic_ctx{}.
-type file() :: file_meta:entry(). %% Type alias for better code organization
-type ext_file() :: file_meta:entry() | {guid, file_guid()}.
-type open_flag() :: rdwr | write | read.
-type posix_permissions() :: file_meta:posix_permissions().
-type file_guid() :: binary().
-type file_guid_or_path() :: {guid, file_guid()} | {path, file_meta:path()}.
-type request_type() :: fuse_request | file_request | provider_request |
    proxyio_request.

-export_type([ctx/0, file/0, ext_file/0, open_flag/0, posix_permissions/0,
    file_guid/0, file_guid_or_path/0, request_type/0]).

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

    lists:foreach(fun(Sub) ->
        case event:subscribe(Sub) of
            {ok, _SubId} -> ok;
            {error, already_exists} -> ok
        end
    end, [
        fslogic_event_subscriptions:read_subscription(fun handle_read_events/2),
        fslogic_event_subscriptions:write_subscription(fun handle_write_events/2)
    ]),

    case session_manager:create_root_session() of
        {ok, _} -> ok;
        {error, already_exists} -> ok
    end,

    case session_manager:create_guest_session() of
        {ok, _} -> ok;
        {error, already_exists} -> ok
    end,

    {ok, #{sub_id => ?FSLOGIC_SUB_ID}}.

%%--------------------------------------------------------------------
%% @doc
%% {@link worker_plugin_behaviour} callback handle/1.
%% @end
%%--------------------------------------------------------------------
-spec handle(Request) -> Result when
    Request ::
    ping |
    healthcheck |
    {fuse_request, SessId :: session:id(), FuseRequest :: #fuse_request{}} |
    {file_request, SessId :: session:id(), FileRequest :: #file_request{}} |
    {provider_request, SessId :: session:id(), ProviderRequest :: #provider_request{}} |
    {proxyio_request, SessId :: session:id(), ProxyIORequest :: #proxyio_request{}},
    Result :: nagios_handler:healthcheck_response() | ok | {ok, Response} |
    {error, Reason} | pong,
    Response :: term(),
    Reason :: term().
handle(ping) ->
    pong;
handle(healthcheck) ->
    ok;
handle({fuse_request, SessId, FuseRequest}) ->
    ?debug("fuse_request(~p): ~p", [SessId, FuseRequest]),
    Response = run_and_catch_exceptions(fun handle_fuse_request/2, fslogic_context:new(SessId), FuseRequest, fuse_request),
    ?debug("fuse_response: ~p", [Response]),
    Response;
handle({file_request, SessId, FileRequest}) ->
    ?debug("file_request(~p): ~p", [SessId, FileRequest]),
    Response = run_and_catch_exceptions(fun handle_fuse_request/2, fslogic_context:new(SessId), FileRequest, file_request),
    ?debug("fuse_response: ~p", [Response]),
    Response;
handle({provider_request, SessId, ProviderRequest}) ->
    ?debug("provider_request(~p): ~p", [SessId, ProviderRequest]),
    Response = run_and_catch_exceptions(fun handle_provider_request/2, fslogic_context:new(SessId), ProviderRequest, provider_request),
    ?debug("provider_response: ~p", [Response]),
    Response;
handle({proxyio_request, SessId, ProxyIORequest}) ->
    ?debug("proxyio_request(~p): ~p", [SessId, ProxyIORequest]),
    Response = run_and_catch_exceptions(fun handle_proxyio_request/2, fslogic_context:new(SessId), ProxyIORequest, proxyio_request),
    ?debug("proxyio_response: ~p", [Response]),
    Response;
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
%% Handles or forwards request. In case of error translates it using
%% fslogic_errors:translate_error/1 function.
%% @end
%%--------------------------------------------------------------------
-spec run_and_catch_exceptions(Function :: function(), ctx(), any(),
    request_type()) ->
    #fuse_response{} | #provider_response{} | #proxyio_response{}.
run_and_catch_exceptions(Function, Context, Request, RequestType) ->
    Response = try
        UserRootDir = fslogic_uuid:user_root_dir_uuid(fslogic_context:get_user_id(Context)), %todo TL store it in request context
        {NextCTX, Providers, UpdatedRequest} =
            case request_to_file_entry_or_provider(Context, Request) of
                {space, SpaceId} ->
                    #fslogic_ctx{session_id = SessionId} = Context,
                    {ok, #document{value = #od_space{providers = ProviderIds}}} =
                        od_space:get_or_fetch(SessionId, SpaceId),
                    case {ProviderIds, lists:member(oneprovider:get_provider_id(), ProviderIds)} of
                        {_, true} ->
                            {Context, [oneprovider:get_provider_id()], Request};
                        {[_ | _], false} ->
                            {Context, ProviderIds, Request};
                        {[], _} ->
                            throw(unsupported_space)
                    end;
                {file, Entry} ->
                    resolve_provider_for_file(Context, Entry, Request, UserRootDir);
                {provider, ProvId} ->
                    {Context, [ProvId], Request}
            end,

        Self = oneprovider:get_provider_id(),

        case lists:member(Self, Providers) of
            true ->
                apply(Function, [NextCTX, UpdatedRequest]);
            false ->
                PrePostProcessResponse =
                    try fslogic_remote:prerouting(NextCTX, UpdatedRequest, Providers) of
                        {ok, {reroute, Self, Request1}} ->  %% Request should be handled locally for some reason
                            {ok, apply(Function, [NextCTX, Request1])};
                        {ok, {reroute, RerouteToProvider, Request1}} ->
                            {ok, fslogic_remote:reroute(NextCTX, RerouteToProvider, Request1)};
                        {error, PreRouteError} ->
                            ?error("Cannot initialize reouting for request ~p due to error in prerouting handler: ~p", [UpdatedRequest, PreRouteError]),
                            throw({unable_to_reroute_message, {prerouting_error, PreRouteError}})

                    catch
                        Type:Reason0 ->
                            ?error_stacktrace("Unable to process remote fslogic request due to: ~p", [{Type, Reason0}]),
                            {error, {Type, Reason0}}
                    end,
                case fslogic_remote:postrouting(NextCTX, PrePostProcessResponse, UpdatedRequest) of
                    undefined ->
                        throw({unable_to_reroute_message, PrePostProcessResponse});
                    LocalResponse -> LocalResponse
                end
        end
    catch
        Reason ->
            %% Manually thrown error, normal interrupt case.
            report_error(Request, RequestType, Reason, debug, erlang:get_stacktrace());
        error:{badmatch, Reason} ->
            %% Bad Match assertion - something went wrong, but it could be expected (e.g. file not found assertion).
            report_error(Request, RequestType, Reason, warning, erlang:get_stacktrace());
        error:{case_clause, Reason} ->
            %% Case Clause assertion - something went seriously wrong and we should know about it.
            report_error(Request, RequestType, Reason, error, erlang:get_stacktrace());
        error:Reason ->
            %% Something went horribly wrong. This should not happen.
            report_error(Request, RequestType, Reason, error, erlang:get_stacktrace())
    end,


    try
        process_response(Context, Request, Response)
    catch
        Reason1 ->
            %% Manually thrown error, normal interrupt case.
            report_error(Request, RequestType, Reason1, debug, erlang:get_stacktrace());
        error:{badmatch, Reason1} ->
            %% Bad Match assertion - something went wrong, but it could be expected (e.g. file not found assertion).
            report_error(Request, RequestType, Reason1, warning, erlang:get_stacktrace());
        error:{case_clause, Reason1} ->
            %% Case Clause assertion - something went seriously wrong and we should know about it.
            report_error(Request, RequestType, Reason1, error, erlang:get_stacktrace());
        error:Reason1 ->
            %% Something went horribly wrong. This should not happen.
            report_error(Request, RequestType, Reason1, error, erlang:get_stacktrace())
    end.

process_response(Context, #fuse_request{fuse_request = #file_request{file_request = #get_child_attr{name = FileName}, context_guid = ParentGUID}} = Request,
    #fuse_response{status = #status{code = ?ENOENT}} = Response) ->
    SessId = fslogic_context:get_session_id(Context),
    {ok, Path0} = fslogic_path:gen_path({uuid, fslogic_uuid:guid_to_uuid(ParentGUID)}, SessId),
    {ok, Tokens0} = fslogic_path:verify_file_path(Path0),
    Tokens = Tokens0 ++ [FileName],
    Path = fslogic_path:join(Tokens),
    case fslogic_path:get_canonical_file_entry(Context, Tokens) of
        {path, P} ->
            {ok, Tokens1} = fslogic_path:verify_file_path(P),
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
    {ok, Tokens} = fslogic_path:verify_file_path(Path),
    case fslogic_path:get_canonical_file_entry(Context, Tokens) of
        {path, P} ->
            {ok, Tokens1} = fslogic_path:verify_file_path(P),
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

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Resolves provider for given request. File targeted by the request
%% may be changed by redirection.
%% @end
%%--------------------------------------------------------------------
-spec resolve_provider_for_file(fslogic_worker:ctx(),
    file_meta:entry() | {guid, fslogic_worker:file_guid()}, any(),
    file_meta:uuid()) -> {fslogic_worker:ctx(), [binary()], any()}.
resolve_provider_for_file(Context, Entry, Request, UserRootDir) ->
    case file_meta:to_uuid(Entry) of
        {ok, UserRootDir} ->
            {Context, [oneprovider:get_provider_id()], Request};
        _ ->
            #fslogic_ctx{space_id = SpaceId, session_id = SessionId} = NewCtx =
                fslogic_context:set_space_and_share_id(Context, Entry),

            {ok, #document{value = #od_space{providers = ProviderIds}}} = od_space:get_or_fetch(SessionId, SpaceId),
            case {ProviderIds, lists:member(oneprovider:get_provider_id(), ProviderIds)} of
                {_, true} ->
                    {ok, Uuid} = file_meta:to_uuid(Entry),
                    case file_meta:get(Uuid) of
                        {error, {not_found, file_meta}} ->
                            {ok, NewGuid} = file_meta:get_guid_from_phantom_file(Uuid),
                            UpdatedRequest = change_file_in_request(Request, NewGuid),
                            resolve_provider_for_file(NewCtx, {guid, NewGuid}, UpdatedRequest, UserRootDir);
                        _ ->
                            {NewCtx, [oneprovider:get_provider_id()], Request}
                    end;
                {[_ | _], false} ->
                    {NewCtx, ProviderIds, Request};
                {[], _} ->
                    throw(unsupported_space)
            end
    end.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Changes target GUID of given request
%% @end
%%--------------------------------------------------------------------
-spec change_file_in_request(any(), fslogic_worker:file_guid()) -> any().
change_file_in_request(#fuse_request{fuse_request = #file_request{} = FileRequest} = Request, GUID) ->
    Request#fuse_request{fuse_request = change_file_in_request(FileRequest, GUID)};
change_file_in_request(#file_request{} = Request, GUID) ->
    Request#file_request{context_guid = GUID};
change_file_in_request(#provider_request{} = Request, GUID) ->
    Request#provider_request{context_guid = GUID};
change_file_in_request(#proxyio_request{parameters = #{?PROXYIO_PARAMETER_FILE_UUID := _} = Parameters} = Request, GUID) ->
    Request#proxyio_request{parameters = Parameters#{?PROXYIO_PARAMETER_FILE_UUID => GUID}};
change_file_in_request(Request, _) ->
    Request.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Returns a FUSE response with translated error description.
%% Logs an error with given log level.
%% @end
%%--------------------------------------------------------------------
-spec report_error(Request :: any(), Type :: request_type(), Error :: term(),
    LogLevel :: debug | warning | error, Stacktrace :: term()) ->
    #fuse_response{} | #provider_response{} | #proxyio_response{}.
report_error(Request, RequestType, Error, LogLevel, Stacktrace) ->
    Status = #status{code = Code, description = Description} =
        fslogic_errors:gen_status_message(Error),
    MsgFormat =
        "Cannot process request ~p due to error: ~p (code: ~p)~nStacktrace: ~p",
    FormatArgs = [Request, Description, Code, Stacktrace],
    case LogLevel of
        debug -> ?debug_stacktrace(MsgFormat, FormatArgs);
%%      info -> ?info(MsgFormat, FormatArgs);  %% Not used right now
        warning -> ?warning_stacktrace(MsgFormat, FormatArgs);
        error -> ?error_stacktrace(MsgFormat, FormatArgs)
    end,
    error_response(RequestType, Status).

%%--------------------------------------------------------------------
%% @doc
%% Returns response with given status, matching given request.
%% @end
%%--------------------------------------------------------------------
-spec error_response(request_type(), #status{}) ->
    #fuse_response{} | #provider_response{} | #proxyio_response{}.
error_response(fuse_request, Status) ->
    #fuse_response{status = Status};
error_response(file_request, Status) ->
    #fuse_response{status = Status};
error_response(provider_request, Status) ->
    #provider_response{status = Status};
error_response(proxyio_request, Status) ->
    #proxyio_response{status = Status}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Processes a FUSE request and returns a response.
%% @end
%%--------------------------------------------------------------------
-spec handle_fuse_request(Ctx :: fslogic_worker:ctx(), Request :: #fuse_request{} | #file_request{}) ->
    FuseResponse :: #fuse_response{}.
handle_fuse_request(Ctx, #fuse_request{fuse_request = #resolve_guid{path = Path}}) ->
    {ok, Tokens} = fslogic_path:verify_file_path(Path),
    CanonicalFileEntry = fslogic_path:get_canonical_file_entry(Ctx, Tokens),
    fslogic_req_generic:get_file_attr(Ctx, CanonicalFileEntry);
handle_fuse_request(Ctx, #fuse_request{fuse_request = #get_helper_params{storage_id = SID, force_proxy_io = ForceProxy}}) ->
    fslogic_req_regular:get_helper_params(Ctx, SID, ForceProxy);
handle_fuse_request(#fslogic_ctx{session_id = SessId}, #fuse_request{fuse_request = #create_storage_test_file{} = Req}) ->
    fuse_config_manager:create_storage_test_file(SessId, Req);
handle_fuse_request(#fslogic_ctx{session_id = SessId}, #fuse_request{fuse_request = #verify_storage_test_file{} = Req}) ->
    fuse_config_manager:verify_storage_test_file(SessId, Req);
handle_fuse_request(Ctx, #fuse_request{fuse_request = #file_request{} = FileRequest}) ->
    handle_fuse_request(Ctx, FileRequest);

handle_fuse_request(Ctx, #file_request{context_guid = GUID, file_request = #get_file_attr{}}) ->
    fslogic_req_generic:get_file_attr(Ctx, {uuid, fslogic_uuid:guid_to_uuid(GUID)});
handle_fuse_request(Ctx, #file_request{context_guid = GUID, file_request = #get_child_attr{name = Name}}) ->
    fslogic_req_special:get_child_attr(Ctx, {uuid, fslogic_uuid:guid_to_uuid(GUID)}, Name);
handle_fuse_request(Ctx, #file_request{context_guid = GUID, file_request = #delete_file{silent = Silent}}) ->
    fslogic_req_generic:delete(Ctx, {uuid, fslogic_uuid:guid_to_uuid(GUID)}, Silent);
handle_fuse_request(Ctx, #file_request{context_guid = ParentGUID, file_request = #create_dir{name = Name, mode = Mode}}) ->
    fslogic_req_special:mkdir(Ctx, {uuid, fslogic_uuid:guid_to_uuid(ParentGUID)}, Name, Mode);
handle_fuse_request(Ctx, #file_request{context_guid = GUID, file_request = #get_file_children{offset = Offset, size = Size}}) ->
    fslogic_req_special:read_dir(Ctx, {uuid, fslogic_uuid:guid_to_uuid(GUID)}, Offset, Size);
handle_fuse_request(Ctx, #file_request{context_guid = GUID, file_request = #change_mode{mode = Mode}}) ->
    fslogic_req_generic:chmod(Ctx, {uuid, fslogic_uuid:guid_to_uuid(GUID)}, Mode);
handle_fuse_request(#fslogic_ctx{session_id = SessId} = Ctx, #file_request{context_guid = GUID, file_request = #rename{target_parent_uuid = TargetParentGuid, target_name = TargetName}}) ->
    %% Use lfm_files wrapper for fslogic as the target uuid may not be local
    {ok, TargetParentPath} = lfm_files:get_file_path(SessId, TargetParentGuid),
    TargetPath = fslogic_path:join([<<?DIRECTORY_SEPARATOR>>, TargetParentPath, TargetName]),
    fslogic_rename:rename(Ctx, {uuid, fslogic_uuid:guid_to_uuid(GUID)}, TargetPath);
handle_fuse_request(Ctx, #file_request{context_guid = GUID, file_request = #update_times{atime = ATime, mtime = MTime, ctime = CTime}}) ->
    fslogic_req_generic:update_times(Ctx, {uuid, fslogic_uuid:guid_to_uuid(GUID)}, ATime, MTime, CTime);
handle_fuse_request(Ctx, #file_request{context_guid = ParentGUID, file_request = #create_file{name = Name,
    flag = Flag, mode = Mode}}) ->
    NewCtx = fslogic_context:set_space_and_share_id(Ctx, {guid, ParentGUID}),
    fslogic_req_regular:create_file(NewCtx, {uuid, fslogic_uuid:guid_to_uuid(ParentGUID)}, Name, Mode, Flag);
handle_fuse_request(Ctx, #file_request{context_guid = ParentGUID, file_request = #make_file{name = Name, mode = Mode}}) ->
    NewCtx = fslogic_context:set_space_and_share_id(Ctx, {guid, ParentGUID}),
    fslogic_req_regular:make_file(NewCtx, {uuid, fslogic_uuid:guid_to_uuid(ParentGUID)}, Name, Mode);
handle_fuse_request(Ctx, #file_request{context_guid = GUID, file_request = #open_file{flag = Flag}}) ->
    NewCtx = fslogic_context:set_space_and_share_id(Ctx, {guid, GUID}),
    fslogic_req_regular:open_file(NewCtx, {uuid, fslogic_uuid:guid_to_uuid(GUID)}, Flag);
handle_fuse_request(Ctx, #file_request{context_guid = GUID, file_request = #get_file_location{}}) ->
    NewCtx = fslogic_context:set_space_and_share_id(Ctx, {guid, GUID}),
    fslogic_req_regular:get_file_location(NewCtx, {uuid, fslogic_uuid:guid_to_uuid(GUID)});
handle_fuse_request(Ctx, #file_request{context_guid = GUID, file_request = #truncate{size = Size}}) ->
    fslogic_req_regular:truncate(Ctx, {uuid, fslogic_uuid:guid_to_uuid(GUID)}, Size);
handle_fuse_request(Ctx, #file_request{context_guid = GUID, file_request = #release{handle_id = HandleId}}) ->
    fslogic_req_regular:release(Ctx, fslogic_uuid:guid_to_uuid(GUID), HandleId);
handle_fuse_request(Ctx, #file_request{context_guid = GUID,
    file_request = #synchronize_block{block = Block, prefetch = Prefetch}}) ->
    fslogic_req_regular:synchronize_block(Ctx, {uuid, fslogic_uuid:guid_to_uuid(GUID)}, Block, Prefetch);
handle_fuse_request(Ctx, #file_request{context_guid = GUID,
    file_request = #synchronize_block_and_compute_checksum{block = Block}}) ->
    fslogic_req_regular:synchronize_block_and_compute_checksum(Ctx, {uuid, fslogic_uuid:guid_to_uuid(GUID)}, Block);

handle_fuse_request(_Ctx, Req) ->
    ?log_bad_request(Req),
    erlang:error({invalid_request, Req}).

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Processes provider request and returns a response.
%% @end
%%--------------------------------------------------------------------
-spec handle_provider_request(Ctx :: fslogic_worker:ctx(), ProviderRequest :: #provider_request{}) ->
    ProviderResponse :: #provider_response{}.
handle_provider_request(Ctx, #provider_request{context_guid = GUID, provider_request = #get_parent{}}) ->
    fslogic_req_regular:get_parent(Ctx, {uuid, fslogic_uuid:guid_to_uuid(GUID)});
handle_provider_request(Ctx, #provider_request{context_guid = GUID, provider_request = #get_xattr{name = XattrName, inherited = Inherited}}) ->
    fslogic_req_generic:get_xattr(Ctx, {uuid, fslogic_uuid:guid_to_uuid(GUID)}, XattrName, Inherited);
handle_provider_request(Ctx, #provider_request{context_guid = GUID, provider_request = #set_xattr{xattr = Xattr}}) ->
    fslogic_req_generic:set_xattr(Ctx, {uuid, fslogic_uuid:guid_to_uuid(GUID)}, Xattr);
handle_provider_request(Ctx, #provider_request{context_guid = GUID, provider_request = #remove_xattr{name = XattrName}}) ->
    fslogic_req_generic:remove_xattr(Ctx, {uuid, fslogic_uuid:guid_to_uuid(GUID)}, XattrName);
handle_provider_request(Ctx, #provider_request{context_guid = GUID, provider_request = #list_xattr{inherited = Inherited, show_internal = ShowInternal}}) ->
    fslogic_req_generic:list_xattr(Ctx, {uuid, fslogic_uuid:guid_to_uuid(GUID)}, Inherited, ShowInternal);
handle_provider_request(Ctx, #provider_request{context_guid = GUID, provider_request = #get_acl{}}) ->
    fslogic_req_generic:get_acl(Ctx, {uuid, fslogic_uuid:guid_to_uuid(GUID)});
handle_provider_request(Ctx, #provider_request{context_guid = GUID, provider_request = #set_acl{acl = Acl}}) ->
    fslogic_req_generic:set_acl(Ctx, {uuid, fslogic_uuid:guid_to_uuid(GUID)}, Acl);
handle_provider_request(Ctx, #provider_request{context_guid = GUID, provider_request = #remove_acl{}}) ->
    fslogic_req_generic:remove_acl(Ctx, {uuid, fslogic_uuid:guid_to_uuid(GUID)});
handle_provider_request(Ctx, #provider_request{context_guid = GUID, provider_request = #get_transfer_encoding{}}) ->
    fslogic_req_generic:get_transfer_encoding(Ctx, {uuid, fslogic_uuid:guid_to_uuid(GUID)});
handle_provider_request(Ctx, #provider_request{context_guid = GUID, provider_request = #set_transfer_encoding{value = Value}}) ->
    fslogic_req_generic:set_transfer_encoding(Ctx, {uuid, fslogic_uuid:guid_to_uuid(GUID)}, Value);
handle_provider_request(Ctx, #provider_request{context_guid = GUID, provider_request = #get_cdmi_completion_status{}}) ->
    fslogic_req_generic:get_cdmi_completion_status(Ctx, {uuid, fslogic_uuid:guid_to_uuid(GUID)});
handle_provider_request(Ctx, #provider_request{context_guid = GUID, provider_request = #set_cdmi_completion_status{value = Value}}) ->
    fslogic_req_generic:set_cdmi_completion_status(Ctx, {uuid, fslogic_uuid:guid_to_uuid(GUID)}, Value);
handle_provider_request(Ctx, #provider_request{context_guid = GUID, provider_request = #get_mimetype{}}) ->
    fslogic_req_generic:get_mimetype(Ctx, {uuid, fslogic_uuid:guid_to_uuid(GUID)});
handle_provider_request(Ctx, #provider_request{context_guid = GUID, provider_request = #set_mimetype{value = Value}}) ->
    fslogic_req_generic:set_mimetype(Ctx, {uuid, fslogic_uuid:guid_to_uuid(GUID)}, Value);
handle_provider_request(Ctx, #provider_request{context_guid = GUID, provider_request = #get_file_path{}}) ->
    fslogic_req_generic:get_file_path(Ctx, {uuid, fslogic_uuid:guid_to_uuid(GUID)});
handle_provider_request(Ctx, #provider_request{context_guid = GUID, provider_request = #get_file_distribution{}}) ->
    fslogic_req_regular:get_file_distribution(Ctx, {uuid, fslogic_uuid:guid_to_uuid(GUID)});
handle_provider_request(Ctx, #provider_request{context_guid = GUID, provider_request = #replicate_file{block = Block}}) ->
    NewCtx = fslogic_context:set_space_and_share_id(Ctx, {guid, GUID}),
    fslogic_req_generic:replicate_file(NewCtx, {uuid, fslogic_uuid:guid_to_uuid(GUID)}, Block);
handle_provider_request(Ctx, #provider_request{context_guid = GUID, provider_request = #get_metadata{type = Type, names = Names, inherited = Inherited}}) ->
    fslogic_req_generic:get_metadata(Ctx, {uuid, fslogic_uuid:guid_to_uuid(GUID)}, Type, Names, Inherited);
handle_provider_request(Ctx, #provider_request{context_guid = GUID, provider_request = #set_metadata{metadata =
#metadata{type = Type, value = Value}, names = Names}}) ->
    fslogic_req_generic:set_metadata(Ctx, {uuid, fslogic_uuid:guid_to_uuid(GUID)}, Type, Value, Names);
handle_provider_request(Ctx, #provider_request{context_guid = GUID, provider_request = #remove_metadata{type = Type}}) ->
    fslogic_req_generic:remove_metadata(Ctx, {uuid, fslogic_uuid:guid_to_uuid(GUID)}, Type);
handle_provider_request(Ctx, #provider_request{context_guid = GUID, provider_request = #check_perms{flag = Flag}}) ->
    fslogic_req_generic:check_perms(Ctx, {uuid, fslogic_uuid:guid_to_uuid(GUID)}, Flag);
handle_provider_request(Ctx, #provider_request{context_guid = GUID, provider_request = #create_share{name = Name}}) ->
    fslogic_req_generic:create_share(Ctx, {uuid, fslogic_uuid:guid_to_uuid(GUID)}, Name);
handle_provider_request(Ctx, #provider_request{context_guid = GUID, provider_request = #remove_share{}}) ->
    fslogic_req_generic:remove_share(Ctx, {uuid, fslogic_uuid:guid_to_uuid(GUID)});
handle_provider_request(Ctx, #provider_request{context_guid = GUID, provider_request = #copy{target_path = TargetPath}}) ->
    fslogic_copy:copy(Ctx, {uuid, fslogic_uuid:guid_to_uuid(GUID)}, TargetPath);
handle_provider_request(_Ctx, Req) ->
    ?log_bad_request(Req),
    erlang:error({invalid_request, Req}).

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Processes proxyio request and returns a response.
%% @end
%%--------------------------------------------------------------------
-spec handle_proxyio_request(Ctx :: fslogic_worker:ctx(), ProxyIORequest :: #proxyio_request{}) ->
    ProxyIOResponse :: #proxyio_response{}.
handle_proxyio_request(#fslogic_ctx{session_id = SessionId, share_id = ShareId}, #proxyio_request{
    parameters = Parameters = #{?PROXYIO_PARAMETER_FILE_UUID := FileGUID}, storage_id = SID, file_id = FID,
    proxyio_request = #remote_write{byte_sequence = ByteSequences}}) ->
    FileUUID = fslogic_uuid:guid_to_uuid(FileGUID),
    fslogic_proxyio:write(SessionId, Parameters#{?PROXYIO_PARAMETER_FILE_UUID := FileUUID,
        ?PROXYIO_PARAMETER_SHARE_ID => ShareId}, SID, FID, ByteSequences);
handle_proxyio_request(#fslogic_ctx{session_id = SessionId, share_id = ShareId}, #proxyio_request{
    parameters = Parameters = #{?PROXYIO_PARAMETER_FILE_UUID := FileGUID}, storage_id = SID, file_id = FID,
    proxyio_request = #remote_read{offset = Offset, size = Size}}) ->
    FileUUID = fslogic_uuid:guid_to_uuid(FileGUID),
    fslogic_proxyio:read(SessionId, Parameters#{?PROXYIO_PARAMETER_FILE_UUID := FileUUID,
        ?PROXYIO_PARAMETER_SHARE_ID => ShareId}, SID, FID, Offset, Size);
handle_proxyio_request(_CTX, Req) ->
    ?log_bad_request(Req),
    erlang:error({invalid_request, Req}).


%%--------------------------------------------------------------------
%% @private
%% @doc
%% Processes write events and returns a response.
%% @end
%%--------------------------------------------------------------------
-spec handle_write_events(Evts :: [event:event()], Ctx :: maps:map()) ->
    [ok | {error, Reason :: term()}].
handle_write_events(Evts, #{session_id := SessId} = Ctx) ->
    Results = lists:map(fun(Ev) ->
        try_handle_event(fun() -> handle_write_event(Ev, SessId) end)
    end, Evts),

    case Ctx of
        #{notify := NotifyFun} ->
            NotifyFun(#server_message{message_body = #status{code = ?OK}});
        _ -> ok
    end,

    Results.


%%--------------------------------------------------------------------
%% @private
%% @doc
%% Processes a write event and returns a response.
%% @end
%%--------------------------------------------------------------------
-spec handle_write_event(event:event(), session:id()) ->
    ok | {error, Reason :: term()}.
handle_write_event(Event, SessId) ->
    #event{object = #write_event{size = Size, blocks = Blocks,
        file_uuid = FileGUID, file_size = FileSize}, counter = Counter} = Event,

    {FileUUID, SpaceId} = fslogic_uuid:unpack_guid(FileGUID),
    {ok, #document{value = #session{identity = #user_identity{
        user_id = UserId}}}} = session:get(SessId),
    monitoring_event:emit_write_statistics(SpaceId, UserId, Size, Counter),

    UpdatedBlocks = lists:map(fun(#file_block{file_id = FileId, storage_id = StorageId} = Block) ->
        {ValidFileId, ValidStorageId} = file_location:validate_block_data(FileUUID, FileId, StorageId),
        Block#file_block{file_id = ValidFileId, storage_id = ValidStorageId}
    end, Blocks),

    case replica_updater:update(FileUUID, UpdatedBlocks, FileSize, true, undefined) of
        {ok, size_changed} ->
            {ok, #document{value = #session{identity = #user_identity{
                user_id = UserId}}}} = session:get(SessId),
            fslogic_times:update_mtime_ctime({uuid, FileUUID}, UserId),
            fslogic_event:emit_file_attr_update({uuid, FileUUID}, [SessId]),
            fslogic_event:emit_file_location_update({uuid, FileUUID}, [SessId]);
        {ok, size_not_changed} ->
            {ok, #document{value = #session{identity = #user_identity{
                user_id = UserId}}}} = session:get(SessId),
            fslogic_times:update_mtime_ctime({uuid, FileUUID}, UserId),
            fslogic_event:emit_file_location_update({uuid, FileUUID}, [SessId]);
        {error, Reason} ->
            ?error("Unable to update blocks for file ~p due to: ~p.", [FileUUID, Reason]),
            {error, Reason}
    end.


%%--------------------------------------------------------------------
%% @private
%% @doc
%% Processes read events and returns a response.
%% @end
%%--------------------------------------------------------------------
-spec handle_read_events(Evts :: [event:event()], Ctx :: maps:map()) ->
    [ok | {error, Reason :: term()}].
handle_read_events(Evts, #{session_id := SessId} = _Ctx) ->
    lists:map(fun(Ev) ->
        try_handle_event(fun() -> handle_read_event(Ev, SessId) end)
    end, Evts).


%%--------------------------------------------------------------------
%% @private
%% @doc
%% Processes a read event and returns a response.
%% @end
%%--------------------------------------------------------------------
-spec handle_read_event(event:event(), session:id()) ->
    ok | {error, Reason :: term()}.
handle_read_event(Event, SessId) ->
    #event{object = #read_event{file_uuid = FileGUID, size = Size},
        counter = Counter} = Event,

    {FileUUID, SpaceId} = fslogic_uuid:unpack_guid(FileGUID),
    {ok, #document{value = #session{identity = #user_identity{
        user_id = UserId}}}} = session:get(SessId),
    monitoring_event:emit_read_statistics(SpaceId, UserId, Size, Counter),

    {ok, #document{value = #session{identity = #user_identity{
        user_id = UserId}}}} = session:get(SessId),
    fslogic_times:update_atime({uuid, FileUUID}, UserId).

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Map given request to file-scope, provider-scope or space-scope.
%% @end
%%--------------------------------------------------------------------
-spec request_to_file_entry_or_provider(fslogic_worker:ctx(), #fuse_request{}
| #file_request{} | #provider_request{} | #proxyio_request{}) ->
    {file, file_meta:entry() | {guid, fslogic_worker:file_guid()}} | {provider, oneprovider:id()} | {space, SpaceId :: binary()}.
request_to_file_entry_or_provider(Ctx, #fuse_request{fuse_request = #resolve_guid{path = Path}}) ->
    {ok, Tokens} = fslogic_path:verify_file_path(Path),
    case fslogic_path:get_canonical_file_entry(Ctx, Tokens) of
        {path, P} = FileEntry ->
            {ok, Tokens1} = fslogic_path:verify_file_path(P),
            case Tokens1 of
                [<<?DIRECTORY_SEPARATOR>>, _SpaceId] ->
                    %% Handle root space dir locally
                    {provider, oneprovider:get_provider_id()};
                [<<?DIRECTORY_SEPARATOR>>, SpaceId | _] ->
                    {space, SpaceId};
                _ ->
                    {file, FileEntry}
            end;
        OtherFileEntry ->
            {file, OtherFileEntry}
    end;
request_to_file_entry_or_provider(Ctx, #fuse_request{fuse_request = #file_request{} = FileRequest}) ->
    request_to_file_entry_or_provider(Ctx, FileRequest);
request_to_file_entry_or_provider(_Ctx, #fuse_request{}) ->
    {provider, oneprovider:get_provider_id()};
request_to_file_entry_or_provider(_Ctx, #file_request{context_guid = FileGUID, file_request = #get_file_attr{}}) ->
    case catch fslogic_uuid:space_dir_uuid_to_spaceid(fslogic_uuid:guid_to_uuid(FileGUID)) of
        SpaceId when is_binary(SpaceId) ->
            %% Handle root space dir locally
            {provider, oneprovider:get_provider_id()};
        _ ->
            case file_force_proxy:get(FileGUID) of
                {ok, #document{value = #file_force_proxy{provider_id = ProviderId}}} ->
                    {provider, ProviderId};
                _ ->
                    {file, {guid, FileGUID}}
            end
    end;
request_to_file_entry_or_provider(_Ctx, #file_request{context_guid = ContextGuid}) ->
    case file_force_proxy:get(ContextGuid) of
        {ok, #document{value = #file_force_proxy{provider_id = ProviderId}}} ->
            {provider, ProviderId};
        _ ->
            {file, {guid, ContextGuid}}
    end;
request_to_file_entry_or_provider(_Ctx, #provider_request{provider_request = #replicate_file{provider_id = ProviderId}}) ->
    {provider, ProviderId};
request_to_file_entry_or_provider(_Ctx, #provider_request{context_guid = ContextGuid}) ->
    {file, {guid, ContextGuid}};
request_to_file_entry_or_provider(#fslogic_ctx{}, #proxyio_request{parameters = #{?PROXYIO_PARAMETER_FILE_UUID := FileGUID}}) ->
    {file, {guid, FileGUID}};

request_to_file_entry_or_provider(_Ctx, Req) ->
    ?log_bad_request(Req),
    erlang:error({invalid_request, Req}).


-spec try_handle_event(fun(() -> HandleResult)) -> HandleResult
    when HandleResult :: ok | {error, Reason :: any()}.
try_handle_event(HandleFun) ->
    try HandleFun()
    catch
        Type:Reason -> {error, {Type, Reason}}
    end.
