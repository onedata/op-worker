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
-type open_flags() :: rdwr | write | read.
-type posix_permissions() :: file_meta:posix_permissions().
-type file_guid() :: binary().
-type file_guid_or_path() :: {guid, file_guid()} | {path, file_meta:path()}.
-type request_type() :: fuse_request | provider_request | proxyio_request.

-export_type([ctx/0, file/0, ext_file/0, open_flags/0, posix_permissions/0,
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
    {ok, WriteCounterThreshold} = application:get_env(?APP_NAME, default_write_event_counter_threshold),
    {ok, WriteTimeThreshold} = application:get_env(?APP_NAME, default_write_event_time_threshold_miliseconds),
    {ok, WriteSizeThreshold} = application:get_env(?APP_NAME, default_write_event_size_threshold),
    WriteSubId = ?FSLOGIC_SUB_ID,
    WriteSub = #subscription{
        id = WriteSubId,
        object = #write_subscription{
            counter_threshold = WriteCounterThreshold,
            time_threshold = WriteTimeThreshold,
            size_threshold = WriteSizeThreshold
        },
        event_stream = ?WRITE_EVENT_STREAM#event_stream_definition{
            metadata = {0, 0}, %% {Counter, Size}
            emission_time = WriteTimeThreshold,
            emission_rule =
                fun({Counter, Size}) ->
                    Counter > WriteCounterThreshold orelse Size > WriteSizeThreshold
                end,
            transition_rule =
                fun({Counter, Size}, #event{counter = C, object = #write_event{size = S}}) ->
                    {Counter + C, Size + S}
                end,
            init_handler = event_utils:send_subscription_handler(),
            event_handler = fun(Evts, Ctx) ->
                monitoring_updates:handle_write_events_for_monitoring(Evts, Ctx),
                handle_write_events(Evts, Ctx)
            end,
            terminate_handler = event_utils:send_subscription_cancellation_handler()
        }
    },

    case application:get_env(?APP_NAME, start_rtransfer_on_init) of
        {ok, true} ->
            rtransfer_config:start_rtransfer();
        _ ->
            ok
    end,

    case event:subscribe(WriteSub) of
        {ok, WriteSubId} ->
            ok;
        {error, already_exists} ->
            ok
    end,

    ReadSub = event_subscriptions:read_subscription(
        fun(Evts, Ctx) ->
            monitoring_updates:handle_read_events_for_monitoring(Evts, Ctx),
            handle_read_events(Evts, Ctx)
        end),

    case event:subscribe(ReadSub) of
        {ok, _ReadSubId} ->
            ok;
        {error, already_exists} ->
            ok
    end,

    {ok, FileAccessedThreshold} = application:get_env(?APP_NAME,
        default_file_accessed_event_counter_threshold),
    {ok, FileAccessedTimeThreshold} = application:get_env(?APP_NAME,
        default_file_accessed_event_time_threshold_miliseconds),
    FileAccessedSub = #subscription{
        object = #file_accessed_subscription{
            counter_threshold = FileAccessedThreshold,
            time_threshold = FileAccessedTimeThreshold
        },
        event_stream = ?FILE_ACCESSED_EVENT_STREAM#event_stream_definition{
            metadata = 0,
            emission_rule = fun(_) -> true end,
            init_handler = event_utils:send_subscription_handler(),
            event_handler = fun(Evts, Ctx) ->
                handle_file_accessed_events(Evts, Ctx)
            end,
            terminate_handler = event_utils:send_subscription_cancellation_handler()
        }
    },

    case event:subscribe(FileAccessedSub) of
        {ok, _FileAccessedSubId} ->
            ok;
        {error, already_exists} ->
            ok
    end,

    {ok, #{sub_id => WriteSubId}}.

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
run_and_catch_exceptions(Function, Context, Request, Type) ->
    try
        UserRootDir = fslogic_uuid:user_root_dir_uuid(fslogic_context:get_user_id(Context)),
        {NextCTX, Providers, UpdatedRequest} =
            case request_to_file_entry_or_provider(Context, Request) of
                {space, SpaceId} ->
                    #fslogic_ctx{session_id = SessionId} = Context,
                    {ok, #document{value = #space_info{providers = ProviderIds}}} =
                        space_info:get_or_fetch(SessionId, SpaceId),
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
                    undefined -> throw({unable_to_reroute_message, PrePostProcessResponse});
                    LocalResponse -> LocalResponse
                end
        end
    catch
        Reason ->
            %% Manually thrown error, normal interrupt case.
            report_error(Request, Type, Reason, debug);
        error:{badmatch, Reason} ->
            %% Bad Match assertion - something went wrong, but it could be expected (e.g. file not found assertion).
            report_error(Request, Type, Reason, warning);
        error:{case_clause, Reason} ->
            %% Case Clause assertion - something went seriously wrong and we should know about it.
            report_error(Request, Type, Reason, error);
        error:Reason ->
            %% Something went horribly wrong. This should not happen.
            report_error(Request, Type, Reason, error)
    end.

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
                fslogic_context:set_space_id(Context, Entry),

            {ok, #document{value = #space_info{providers = ProviderIds}}} = space_info:get_or_fetch(SessionId, SpaceId),
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
change_file_in_request(#fuse_request{fuse_request = #get_file_attr{} = IRequest} = Request, GUID) ->
    Request#fuse_request{fuse_request = IRequest#get_file_attr{entry = {guid, GUID}}};
change_file_in_request(#fuse_request{fuse_request = #delete_file{} = IRequest} = Request, GUID) ->
    Request#fuse_request{fuse_request = IRequest#delete_file{uuid = GUID}};
change_file_in_request(#fuse_request{fuse_request = #create_dir{} = IRequest} = Request, GUID) ->
    Request#fuse_request{fuse_request = IRequest#create_dir{parent_uuid = GUID}};
change_file_in_request(#fuse_request{fuse_request = #get_file_children{} = IRequest} = Request, GUID) ->
    Request#fuse_request{fuse_request = IRequest#get_file_children{uuid = GUID}};
change_file_in_request(#fuse_request{fuse_request = #change_mode{} = IRequest} = Request, GUID) ->
    Request#fuse_request{fuse_request = IRequest#change_mode{uuid = GUID}};
change_file_in_request(#fuse_request{fuse_request = #rename{} = IRequest} = Request, GUID) ->
    Request#fuse_request{fuse_request = IRequest#rename{uuid = GUID}};
change_file_in_request(#fuse_request{fuse_request = #update_times{} = IRequest} = Request, GUID) ->
    Request#fuse_request{fuse_request = IRequest#update_times{uuid = GUID}};
change_file_in_request(#fuse_request{fuse_request = #get_new_file_location{} = IRequest} = Request, GUID) ->
    Request#fuse_request{fuse_request = IRequest#get_new_file_location{parent_uuid = GUID}};
change_file_in_request(#fuse_request{fuse_request = #get_file_location{} = IRequest} = Request, GUID) ->
    Request#fuse_request{fuse_request = IRequest#get_file_location{uuid = GUID}};
change_file_in_request(#fuse_request{fuse_request = #truncate{} = IRequest} = Request, GUID) ->
    Request#fuse_request{fuse_request = IRequest#truncate{uuid = GUID}};
change_file_in_request(#fuse_request{fuse_request = #synchronize_block{} = IRequest} = Request, GUID) ->
    Request#fuse_request{fuse_request = IRequest#synchronize_block{uuid = GUID}};
change_file_in_request(#fuse_request{fuse_request = #synchronize_block_and_compute_checksum{} = IRequest} = Request, GUID) ->
    Request#fuse_request{fuse_request = IRequest#synchronize_block_and_compute_checksum{uuid = GUID}};
change_file_in_request(#fuse_request{fuse_request = #release{} = IRequest} = Request, GUID) ->
    Request#fuse_request{fuse_request = IRequest#release{uuid = GUID}};
change_file_in_request(#provider_request{provider_request = #get_parent{} = IRequest} = Request, GUID) ->
    Request#provider_request{provider_request = IRequest#get_parent{uuid = GUID}};
change_file_in_request(#provider_request{provider_request = #get_xattr{} = IRequest} = Request, GUID) ->
    Request#provider_request{provider_request = IRequest#get_xattr{uuid = GUID}};
change_file_in_request(#provider_request{provider_request = #set_xattr{} = IRequest} = Request, GUID) ->
    Request#provider_request{provider_request = IRequest#set_xattr{uuid = GUID}};
change_file_in_request(#provider_request{provider_request = #remove_xattr{} = IRequest} = Request, GUID) ->
    Request#provider_request{provider_request = IRequest#remove_xattr{uuid = GUID}};
change_file_in_request(#provider_request{provider_request = #list_xattr{} = IRequest} = Request, GUID) ->
    Request#provider_request{provider_request = IRequest#list_xattr{uuid = GUID}};
change_file_in_request(#provider_request{provider_request = #get_acl{} = IRequest} = Request, GUID) ->
    Request#provider_request{provider_request = IRequest#get_acl{uuid = GUID}};
change_file_in_request(#provider_request{provider_request = #set_acl{} = IRequest} = Request, GUID) ->
    Request#provider_request{provider_request = IRequest#set_acl{uuid = GUID}};
change_file_in_request(#provider_request{provider_request = #remove_acl{} = IRequest} = Request, GUID) ->
    Request#provider_request{provider_request = IRequest#remove_acl{uuid = GUID}};
change_file_in_request(#provider_request{provider_request = #get_transfer_encoding{} = IRequest} = Request, GUID) ->
    Request#provider_request{provider_request = IRequest#get_transfer_encoding{uuid = GUID}};
change_file_in_request(#provider_request{provider_request = #set_transfer_encoding{} = IRequest} = Request, GUID) ->
    Request#provider_request{provider_request = IRequest#set_transfer_encoding{uuid = GUID}};
change_file_in_request(#provider_request{provider_request = #get_cdmi_completion_status{} = IRequest} = Request, GUID) ->
    Request#provider_request{provider_request = IRequest#get_cdmi_completion_status{uuid = GUID}};
change_file_in_request(#provider_request{provider_request = #set_cdmi_completion_status{} = IRequest} = Request, GUID) ->
    Request#provider_request{provider_request = IRequest#set_cdmi_completion_status{uuid = GUID}};
change_file_in_request(#provider_request{provider_request = #get_mimetype{} = IRequest} = Request, GUID) ->
    Request#provider_request{provider_request = IRequest#get_mimetype{uuid = GUID}};
change_file_in_request(#provider_request{provider_request = #set_mimetype{} = IRequest} = Request, GUID) ->
    Request#provider_request{provider_request = IRequest#set_mimetype{uuid = GUID}};
change_file_in_request(#provider_request{provider_request = #get_file_path{} = IRequest} = Request, GUID) ->
    Request#provider_request{provider_request = IRequest#get_file_path{uuid = GUID}};
change_file_in_request(#provider_request{provider_request = #fsync{} = IRequest} = Request, GUID) ->
    Request#provider_request{provider_request = IRequest#fsync{uuid = GUID}};
change_file_in_request(#provider_request{provider_request = #get_file_distribution{} = IRequest} = Request, GUID) ->
    Request#provider_request{provider_request = IRequest#get_file_distribution{uuid = GUID}};
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
    LogLevel :: debug | warning | error) ->
    #fuse_response{} | #provider_response{} | #proxyio_response{}.
report_error(Request, Type, Error, LogLevel) ->
    Status = #status{code = Code, description = Description} =
        fslogic_errors:gen_status_message(Error),
    MsgFormat = "Cannot process request ~p due to error: ~p (code: ~p)",
    case LogLevel of
        debug -> ?debug_stacktrace(MsgFormat, [Request, Description, Code]);
%%      info -> ?info(MsgFormat, [Request, Description, Code]);  %% Not used right now
        warning -> ?warning_stacktrace(MsgFormat, [Request, Description, Code]);
        error -> ?error_stacktrace(MsgFormat, [Request, Description, Code])
    end,
    error_response(Type, Status).

%%--------------------------------------------------------------------
%% @doc
%% Returns response with given status, matching given request.
%% @end
%%--------------------------------------------------------------------
-spec error_response(request_type(), #status{}) ->
    #fuse_response{} | #provider_response{} | #proxyio_response{}.
error_response(fuse_request, Status) ->
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
-spec handle_fuse_request(Ctx :: fslogic_worker:ctx(), FuseRequest :: #fuse_request{}) ->
    FuseResponse :: #fuse_response{}.
handle_fuse_request(Ctx, #fuse_request{fuse_request = #get_file_attr{entry = {path, Path}}}) ->
    {ok, Tokens} = fslogic_path:verify_file_path(Path),
    CanonicalFileEntry = fslogic_path:get_canonical_file_entry(Ctx, Tokens),
    fslogic_req_generic:get_file_attr(Ctx, CanonicalFileEntry);
handle_fuse_request(Ctx, #fuse_request{fuse_request = #get_file_attr{entry = {guid, GUID}}}) ->
    fslogic_req_generic:get_file_attr(Ctx, {uuid, fslogic_uuid:file_guid_to_uuid(GUID)});
handle_fuse_request(Ctx, #fuse_request{fuse_request = #get_file_attr{entry = Entry}}) ->
    fslogic_req_generic:get_file_attr(Ctx, Entry);
handle_fuse_request(Ctx, #fuse_request{fuse_request = #delete_file{uuid = GUID, silent = Silent}}) ->
    fslogic_req_generic:delete(Ctx, {uuid, fslogic_uuid:file_guid_to_uuid(GUID)}, Silent);
handle_fuse_request(Ctx, #fuse_request{fuse_request = #create_dir{parent_uuid = ParentGUID, name = Name, mode = Mode}}) ->
    fslogic_req_special:mkdir(Ctx, {uuid, fslogic_uuid:file_guid_to_uuid(ParentGUID)}, Name, Mode);
handle_fuse_request(Ctx, #fuse_request{fuse_request = #get_file_children{uuid = GUID, offset = Offset, size = Size}}) ->
    fslogic_req_special:read_dir(Ctx, {uuid, fslogic_uuid:file_guid_to_uuid(GUID)}, Offset, Size);
handle_fuse_request(Ctx, #fuse_request{fuse_request = #change_mode{uuid = GUID, mode = Mode}}) ->
    fslogic_req_generic:chmod(Ctx, {uuid, fslogic_uuid:file_guid_to_uuid(GUID)}, Mode);
handle_fuse_request(Ctx, #fuse_request{fuse_request = #rename{uuid = GUID, target_path = TargetPath}}) ->
    fslogic_rename:rename(Ctx, {uuid, fslogic_uuid:file_guid_to_uuid(GUID)}, TargetPath);
handle_fuse_request(Ctx, #fuse_request{fuse_request = #update_times{uuid = UUID, atime = ATime, mtime = MTime, ctime = CTime}}) ->
    fslogic_req_generic:update_times(Ctx, {uuid, fslogic_uuid:file_guid_to_uuid(UUID)}, ATime, MTime, CTime);
handle_fuse_request(Ctx, #fuse_request{fuse_request = #get_new_file_location{name = Name, parent_uuid = ParentUUID,
    flags = Flags, mode = Mode}}) ->
    NewCtx = fslogic_context:set_space_id(Ctx, {guid, ParentUUID}),
    fslogic_req_regular:get_new_file_location(NewCtx, {uuid, fslogic_uuid:file_guid_to_uuid(ParentUUID)}, Name, Mode, Flags);
handle_fuse_request(Ctx, #fuse_request{fuse_request = #get_file_location{uuid = UUID, flags = Flags}}) ->
    NewCtx = fslogic_context:set_space_id(Ctx, {guid, UUID}),
    fslogic_req_regular:get_file_location(NewCtx, {uuid, fslogic_uuid:file_guid_to_uuid(UUID)}, Flags);
handle_fuse_request(Ctx, #fuse_request{fuse_request = #truncate{uuid = UUID, size = Size}}) ->
    fslogic_req_regular:truncate(Ctx, {uuid, fslogic_uuid:file_guid_to_uuid(UUID)}, Size);
handle_fuse_request(Ctx, #fuse_request{fuse_request = #release{handle_id = HandleId}}) ->
    fslogic_req_regular:release(Ctx, HandleId);
handle_fuse_request(Ctx, #fuse_request{fuse_request = #get_helper_params{storage_id = SID, force_proxy_io = ForceProxy}}) ->
    fslogic_req_regular:get_helper_params(Ctx, SID, ForceProxy);
handle_fuse_request(Ctx, #fuse_request{fuse_request = #synchronize_block{uuid = UUID, block = Block, prefetch = Prefetch}}) ->
    fslogic_req_regular:synchronize_block(Ctx, {uuid, fslogic_uuid:file_guid_to_uuid(UUID)}, Block, Prefetch);
handle_fuse_request(Ctx, #fuse_request{fuse_request = #synchronize_block_and_compute_checksum{uuid = UUID, block = Block}}) ->
    fslogic_req_regular:synchronize_block_and_compute_checksum(Ctx, {uuid, fslogic_uuid:file_guid_to_uuid(UUID)}, Block);
handle_fuse_request(Ctx, #fuse_request{fuse_request = #create_storage_test_file{storage_id = SID, file_uuid = FileUUID}}) ->
    fuse_config_manager:create_storage_test_file(Ctx, SID, fslogic_uuid:file_guid_to_uuid(FileUUID));
handle_fuse_request(_Ctx, #fuse_request{fuse_request = #verify_storage_test_file{storage_id = SID, space_uuid = SpaceUUID,
    file_id = FileId, file_content = FileContent}}) ->
    fuse_config_manager:verify_storage_test_file(SID, SpaceUUID, FileId, FileContent);
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
handle_provider_request(Ctx, #provider_request{provider_request = #get_parent{uuid = GUID}}) ->
    fslogic_req_regular:get_parent(Ctx, {uuid, fslogic_uuid:file_guid_to_uuid(GUID)});
handle_provider_request(Ctx, #provider_request{provider_request = #get_xattr{uuid = UUID, name = XattrName}}) ->
    fslogic_req_generic:get_xattr(Ctx, {uuid, fslogic_uuid:file_guid_to_uuid(UUID)}, XattrName);
handle_provider_request(Ctx, #provider_request{provider_request = #set_xattr{uuid = UUID, xattr = Xattr}}) ->
    fslogic_req_generic:set_xattr(Ctx, {uuid, fslogic_uuid:file_guid_to_uuid(UUID)}, Xattr);
handle_provider_request(Ctx, #provider_request{provider_request = #remove_xattr{uuid = UUID, name = XattrName}}) ->
    fslogic_req_generic:remove_xattr(Ctx, {uuid, fslogic_uuid:file_guid_to_uuid(UUID)}, XattrName);
handle_provider_request(Ctx, #provider_request{provider_request = #list_xattr{uuid = UUID}}) ->
    fslogic_req_generic:list_xattr(Ctx, {uuid, fslogic_uuid:file_guid_to_uuid(UUID)});
handle_provider_request(Ctx, #provider_request{provider_request = #get_acl{uuid = UUID}}) ->
    fslogic_req_generic:get_acl(Ctx, {uuid, fslogic_uuid:file_guid_to_uuid(UUID)});
handle_provider_request(Ctx, #provider_request{provider_request = #set_acl{uuid = UUID, acl = Acl}}) ->
    fslogic_req_generic:set_acl(Ctx, {uuid, fslogic_uuid:file_guid_to_uuid(UUID)}, Acl);
handle_provider_request(Ctx, #provider_request{provider_request = #remove_acl{uuid = UUID}}) ->
    fslogic_req_generic:remove_acl(Ctx, {uuid, fslogic_uuid:file_guid_to_uuid(UUID)});
handle_provider_request(Ctx, #provider_request{provider_request = #get_transfer_encoding{uuid = UUID}}) ->
    fslogic_req_generic:get_transfer_encoding(Ctx, {uuid, fslogic_uuid:file_guid_to_uuid(UUID)});
handle_provider_request(Ctx, #provider_request{provider_request = #set_transfer_encoding{uuid = UUID, value = Value}}) ->
    fslogic_req_generic:set_transfer_encoding(Ctx, {uuid, fslogic_uuid:file_guid_to_uuid(UUID)}, Value);
handle_provider_request(Ctx, #provider_request{provider_request = #get_cdmi_completion_status{uuid = UUID}}) ->
    fslogic_req_generic:get_cdmi_completion_status(Ctx, {uuid, fslogic_uuid:file_guid_to_uuid(UUID)});
handle_provider_request(Ctx, #provider_request{provider_request = #set_cdmi_completion_status{uuid = UUID, value = Value}}) ->
    fslogic_req_generic:set_cdmi_completion_status(Ctx, {uuid, fslogic_uuid:file_guid_to_uuid(UUID)}, Value);
handle_provider_request(Ctx, #provider_request{provider_request = #get_mimetype{uuid = UUID}}) ->
    fslogic_req_generic:get_mimetype(Ctx, {uuid, fslogic_uuid:file_guid_to_uuid(UUID)});
handle_provider_request(Ctx, #provider_request{provider_request = #set_mimetype{uuid = UUID, value = Value}}) ->
    fslogic_req_generic:set_mimetype(Ctx, {uuid, fslogic_uuid:file_guid_to_uuid(UUID)}, Value);
handle_provider_request(Ctx, #provider_request{provider_request = #get_file_path{uuid = FileGUID}}) ->
    fslogic_req_generic:get_file_path(Ctx, fslogic_uuid:file_guid_to_uuid(FileGUID));
handle_provider_request(Ctx, #provider_request{provider_request = #get_file_distribution{uuid = FileGUID}}) ->
    fslogic_req_regular:get_file_distribution(Ctx, {uuid, fslogic_uuid:file_guid_to_uuid(FileGUID)});
handle_provider_request(Ctx, #provider_request{provider_request = #replicate_file{uuid = FileGUID, block = Block}}) ->
    fslogic_req_generic:replicate_file(Ctx, {uuid, fslogic_uuid:file_guid_to_uuid(FileGUID)}, Block);
handle_provider_request(Ctx, #provider_request{provider_request = #get_metadata{uuid = FileGUID, type = Type, names = Names}}) ->
    fslogic_req_generic:get_metadata(Ctx, {uuid, fslogic_uuid:file_guid_to_uuid(FileGUID)}, Type, Names);
handle_provider_request(Ctx, #provider_request{provider_request = #set_metadata{uuid = FileGUID, metadata =
        #metadata{type = Type, value = Value}, names = Names}}) ->
    fslogic_req_generic:set_metadata(Ctx, {uuid, fslogic_uuid:file_guid_to_uuid(FileGUID)}, Type, Value, Names);
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
handle_proxyio_request(#fslogic_ctx{session_id = SessionId}, #proxyio_request{
    parameters = Parameters = #{?PROXYIO_PARAMETER_FILE_UUID := FileGUID}, storage_id = SID, file_id = FID,
    proxyio_request = #remote_write{byte_sequence = ByteSequences}}) ->
    FileUUID = fslogic_uuid:file_guid_to_uuid(FileGUID),
    fslogic_proxyio:write(SessionId, Parameters#{?PROXYIO_PARAMETER_FILE_UUID := FileUUID}, SID, FID, ByteSequences);
handle_proxyio_request(#fslogic_ctx{session_id = SessionId}, #proxyio_request{
    parameters = Parameters = #{?PROXYIO_PARAMETER_FILE_UUID := FileGUID}, storage_id = SID, file_id = FID,
    proxyio_request = #remote_read{offset = Offset, size = Size}}) ->
    FileUUID = fslogic_uuid:file_guid_to_uuid(FileGUID),
    fslogic_proxyio:read(SessionId, Parameters#{?PROXYIO_PARAMETER_FILE_UUID := FileUUID}, SID, FID, Offset, Size);
handle_proxyio_request(_CTX, Req) ->
    ?log_bad_request(Req),
    erlang:error({invalid_request, Req}).

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Processes write events and returns a response.
%% @end
%%--------------------------------------------------------------------
-spec handle_write_events(Evts :: [event:event()], Ctx :: #{}) ->
    [ok | {error, Reason :: term()}].
handle_write_events(Evts, #{session_id := SessId} = Ctx) ->
    Results = lists:map(fun(#event{object = #write_event{
        blocks = Blocks, file_uuid = FileGUID, file_size = FileSize}}) ->
        FileUUID = fslogic_uuid:file_guid_to_uuid(FileGUID),
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
                ?error("Unable to update blocks for file ~p due to: ~p.", [FileUUID, Reason])
        end
    end, Evts),

    case Ctx of
        #{notify := NotifyFun} -> NotifyFun(#server_message{message_body = #status{code = ?OK}});
        _ -> ok
    end,

    Results.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Processes read events and returns a response.
%% @end
%%--------------------------------------------------------------------
-spec handle_read_events(Evts :: [event:event()], Ctx :: #{}) ->
    [ok | {error, Reason :: term()}].
handle_read_events(Evts, #{session_id := SessId} = _Ctx) ->
    lists:map(fun(#event{object = #read_event{file_uuid = FileGUID}}) ->
        FileUUID = fslogic_uuid:file_guid_to_uuid(FileGUID),
        {ok, #document{value = #session{identity = #user_identity{
            user_id = UserId}}}} = session:get(SessId),
        fslogic_times:update_atime({uuid, FileUUID}, UserId)
    end, Evts).

handle_file_accessed_events(Evts, #{session_id := SessId}) ->
    lists:foreach(fun(#event{object = #file_accessed_event{file_uuid = FileGUID,
        open_count = OpenCount, release_count = ReleaseCount}}) ->
        {ok, FileUUID} = file_meta:to_uuid({guid, FileGUID}),

        case OpenCount - ReleaseCount of
            Count when Count > 0 ->
                ok = open_file:register_open(FileUUID, SessId, Count);
            Count when Count < 0 ->
                ok = open_file:register_release(FileUUID, SessId, -Count);
            _ -> ok
        end
    end, Evts).

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Map given request to file-scope, provider-scope or space-scope.
%% @end
%%--------------------------------------------------------------------
-spec request_to_file_entry_or_provider(fslogic_worker:ctx(), #fuse_request{} | #provider_request{} | #proxyio_request{}) ->
    {file, file_meta:entry() | {guid, fslogic_worker:file_guid()}} | {provider, oneprovider:id()} | {space, SpaceId :: binary()}.
request_to_file_entry_or_provider(Ctx, #fuse_request{fuse_request = #get_file_attr{entry = {path, Path}}}) ->
    {ok, Tokens} = fslogic_path:verify_file_path(Path),
    case fslogic_path:get_canonical_file_entry(Ctx, Tokens) of
        {path, P} = FileEntry ->
            {ok, Tokens1} = fslogic_path:verify_file_path(P),
            case Tokens1 of
                [<<?DIRECTORY_SEPARATOR>>, SpaceId] ->
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
request_to_file_entry_or_provider(_Ctx, #fuse_request{fuse_request = #get_file_attr{entry = {guid, FileGUID}}}) ->
    case catch fslogic_uuid:space_dir_uuid_to_spaceid(fslogic_uuid:file_guid_to_uuid(FileGUID)) of
        SpaceId when is_binary(SpaceId) ->
            %% Handle root space dir locally
            {provider, oneprovider:get_provider_id()};
        _ ->
            {file, {guid, FileGUID}}
    end;
request_to_file_entry_or_provider(_Ctx, #fuse_request{fuse_request = #get_file_attr{entry = Entry}}) ->
    {file, Entry};
request_to_file_entry_or_provider(_Ctx, #fuse_request{fuse_request = #delete_file{uuid = UUID}}) ->
    {file, {guid, UUID}};
request_to_file_entry_or_provider(_Ctx, #fuse_request{fuse_request = #create_dir{parent_uuid = ParentUUID}}) ->
    {file, {guid, ParentUUID}};
request_to_file_entry_or_provider(_Ctx, #fuse_request{fuse_request = #get_file_children{uuid = UUID}}) ->
    {file, {guid, UUID}};
request_to_file_entry_or_provider(_Ctx, #fuse_request{fuse_request = #change_mode{uuid = UUID}}) ->
    {file, {guid, UUID}};
request_to_file_entry_or_provider(_Ctx, #fuse_request{fuse_request = #rename{uuid = UUID}}) ->
    {file, {guid, UUID}};
request_to_file_entry_or_provider(_Ctx, #fuse_request{fuse_request = #update_times{uuid = UUID}}) ->
    {file, {guid, UUID}};
request_to_file_entry_or_provider(_Ctx, #fuse_request{fuse_request = #get_new_file_location{parent_uuid = ParentUUID}}) ->
    {file, {guid, ParentUUID}};
request_to_file_entry_or_provider(_Ctx, #fuse_request{fuse_request = #get_file_location{uuid = UUID}}) ->
    {file, {guid, UUID}};
request_to_file_entry_or_provider(_Ctx, #fuse_request{fuse_request = #truncate{uuid = UUID}}) ->
    {file, {guid, UUID}};
request_to_file_entry_or_provider(_Ctx, #fuse_request{fuse_request = #get_helper_params{}}) ->
    {provider, oneprovider:get_provider_id()};
request_to_file_entry_or_provider(_Ctx, #fuse_request{fuse_request = #synchronize_block{uuid = UUID}}) ->
    {file, {guid, UUID}};
request_to_file_entry_or_provider(_Ctx, #fuse_request{fuse_request = #synchronize_block_and_compute_checksum{uuid = UUID}}) ->
    {file, {guid, UUID}};
request_to_file_entry_or_provider(_Ctx, #fuse_request{fuse_request = #release{uuid = UUID}}) ->
    {file, {guid, UUID}};
request_to_file_entry_or_provider(_Ctx, #fuse_request{fuse_request = #create_storage_test_file{}}) ->
    {provider, oneprovider:get_provider_id()};
request_to_file_entry_or_provider(_Ctx, #fuse_request{fuse_request = #verify_storage_test_file{}}) ->
    {provider, oneprovider:get_provider_id()};
request_to_file_entry_or_provider(_Ctx, #provider_request{provider_request = #get_parent{uuid = UUID}}) ->
    {file, {guid, UUID}};
request_to_file_entry_or_provider(_Ctx, #provider_request{provider_request = #get_xattr{uuid = UUID}}) ->
    {file, {guid, UUID}};
request_to_file_entry_or_provider(_Ctx, #provider_request{provider_request = #set_xattr{uuid = UUID}}) ->
    {file, {guid, UUID}};
request_to_file_entry_or_provider(_Ctx, #provider_request{provider_request = #remove_xattr{uuid = UUID}}) ->
    {file, {guid, UUID}};
request_to_file_entry_or_provider(_Ctx, #provider_request{provider_request = #list_xattr{uuid = UUID}}) ->
    {file, {guid, UUID}};
request_to_file_entry_or_provider(_Ctx, #provider_request{provider_request = #get_acl{uuid = UUID}}) ->
    {file, {guid, UUID}};
request_to_file_entry_or_provider(_Ctx, #provider_request{provider_request = #set_acl{uuid = UUID}}) ->
    {file, {guid, UUID}};
request_to_file_entry_or_provider(_Ctx, #provider_request{provider_request = #remove_acl{uuid = UUID}}) ->
    {file, {guid, UUID}};
request_to_file_entry_or_provider(_Ctx, #provider_request{provider_request = #get_transfer_encoding{uuid = UUID}}) ->
    {file, {guid, UUID}};
request_to_file_entry_or_provider(_Ctx, #provider_request{provider_request = #set_transfer_encoding{uuid = UUID}}) ->
    {file, {guid, UUID}};
request_to_file_entry_or_provider(_Ctx, #provider_request{provider_request = #get_cdmi_completion_status{uuid = UUID}}) ->
    {file, {guid, UUID}};
request_to_file_entry_or_provider(_Ctx, #provider_request{provider_request = #set_cdmi_completion_status{uuid = UUID}}) ->
    {file, {guid, UUID}};
request_to_file_entry_or_provider(_Ctx, #provider_request{provider_request = #get_mimetype{uuid = UUID}}) ->
    {file, {guid, UUID}};
request_to_file_entry_or_provider(_Ctx, #provider_request{provider_request = #set_mimetype{uuid = UUID}}) ->
    {file, {guid, UUID}};
request_to_file_entry_or_provider(_Ctx, #provider_request{provider_request = #get_file_path{uuid = UUID}}) ->
    {file, {guid, UUID}};
request_to_file_entry_or_provider(_Ctx, #provider_request{provider_request = #fsync{uuid = UUID}}) ->
    {file, {guid, UUID}};
request_to_file_entry_or_provider(_Ctx, #provider_request{provider_request = #get_file_distribution{uuid = UUID}}) ->
    {file, {guid, UUID}};
request_to_file_entry_or_provider(_Ctx, #provider_request{provider_request = #replicate_file{provider_id = ProviderId}}) ->
    {provider, ProviderId};
request_to_file_entry_or_provider(_Ctx, #provider_request{provider_request = #set_metadata{uuid = UUID}}) ->
    {file, {guid, UUID}};
request_to_file_entry_or_provider(_Ctx, #provider_request{provider_request = #get_metadata{uuid = UUID}}) ->
    {file, {guid, UUID}};

request_to_file_entry_or_provider(#fslogic_ctx{}, #proxyio_request{parameters = #{?PROXYIO_PARAMETER_FILE_UUID := FileGUID}}) ->
    {file, {guid, FileGUID}};

request_to_file_entry_or_provider(_Ctx, Req) ->
    ?log_bad_request(Req),
    erlang:error({invalid_request, Req}).
