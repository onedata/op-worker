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
-include("modules/events/definitions.hrl").
-include("proto/common/credentials.hrl").
-include_lib("ctool/include/global_registry/gr_spaces.hrl").
-include_lib("ctool/include/posix/errors.hrl").
-include_lib("ctool/include/logging.hrl").


-export([init/1, handle/1, cleanup/0, handle_fuse_request/2]).

%%%===================================================================
%%% Types
%%%===================================================================

-type ctx() :: #fslogic_ctx{}.
-type file() :: file_meta:entry(). %% Type alias for better code organization
-type open_flags() :: rdwr | write | read.
-type posix_permissions() :: file_meta:posix_permissions().

-export_type([ctx/0, file/0, open_flags/0, posix_permissions/0]).

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
    {ok, CounterThreshold} = application:get_env(?APP_NAME, default_write_event_counter_threshold),
    {ok, TimeThreshold} = application:get_env(?APP_NAME, default_write_event_time_threshold_miliseconds),
    {ok, SizeThreshold} = application:get_env(?APP_NAME, default_write_event_size_threshold),
    SubId = ?FSLOGIC_SUB_ID,
    Sub = #subscription{
        id = SubId,
        object = #write_subscription{
            counter_threshold = CounterThreshold,
            time_threshold = TimeThreshold,
            size_threshold = SizeThreshold
        },
        event_stream = ?WRITE_EVENT_STREAM#event_stream_definition{
            metadata = 0,
            emission_rule = fun(_) -> true end,
            init_handler = event_utils:send_subscription_handler(),
            event_handler = fun(Evts, Ctx) ->
                handle_events(Evts, Ctx)
                            end,
            terminate_handler = event_utils:send_subscription_cancellation_handler()
        }
    },

    case event:subscribe(Sub) of
        {ok, SubId} ->
            ok;
        {error, already_exists} ->
            ok
    end,
    {ok, #{sub_id => SubId}}.

%%--------------------------------------------------------------------
%% @doc
%% {@link worker_plugin_behaviour} callback handle/1.
%% @end
%%--------------------------------------------------------------------
-spec handle(Request) -> Result when
    Request :: ping | healthcheck | {fuse_request, SessId :: session:id(),
        FuseRequest :: #fuse_request{}},
    Result :: nagios_handler:healthcheck_response() | ok | {ok, Response} |
    {error, Reason} | pong,
    Response :: term(),
    Reason :: term().
handle(ping) ->
    pong;
handle(healthcheck) ->
    ok;
handle({fuse_request, SessId, FuseRequest}) ->
    run_and_catch_exceptions(fun handle_fuse_request/2, fslogic_context:new(SessId), FuseRequest, fuse_request);
handle({proxyio_request, SessId, ProxyIORequest}) ->
    run_and_catch_exceptions(fun handle_proxyio_request/2, fslogic_context:new(SessId), ProxyIORequest, proxyio_request);
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
    fuse_request | proxyio_request) -> #fuse_response{} | #proxyio_response{}.
run_and_catch_exceptions(Function, Context, Request, Type) ->
    try
        SpacesDir = fslogic_uuid:spaces_uuid(fslogic_context:get_user_id(Context)),
        {NextCTX, Providers} =
            case request_to_file_entry_or_provider(Context, Request) of
                {file, Entry} ->
                    case file_meta:to_uuid(Entry) of
                        {ok, SpacesDir} ->
                            {Context, [oneprovider:get_provider_id()]};
                        _ ->
                            #fslogic_ctx{space_id = SpaceId, session_id = SessionId} = NewCtx =
                                fslogic_context:set_space_id(Context, Entry),
                            {ok, #document{value = #session{auth = #auth{macaroon = Macaroon}}}} = session:get(SessionId),
                            {ok, ProviderIds} = gr_spaces:get_providers({user, Macaroon}, SpaceId),
                            case {ProviderIds, lists:member(oneprovider:get_provider_id(), ProviderIds)} of
                                {_, true} ->
                                    {NewCtx, oneprovider:get_provider_id()};
                                {[H | _], false} ->
                                    {NewCtx, H};
                                {[], _} ->
                                    throw(unsupported_space)
                            end
                    end;
                {provider, ProvId} ->
                    {Context, [ProvId]}
            end,

        Self = oneprovider:get_provider_id(),
        case lists:member(Self, Providers) of
            true ->
                apply(Function, [NextCTX, Request]);
            false ->
                PrePostProcessResponse =
                    try
                        case fslogic_remote:prerouting(NextCTX, Request, Providers) of
                            {ok, {reroute, Self, Request1}} ->  %% Request should be handled locally for some reason
                                {ok, apply(Function, [NextCTX, Request1])};
                            {ok, {reroute, RerouteToProvider, Request1}} ->
                                {ok, fslogic_remote:reroute(NextCTX, RerouteToProvider, Request1)};
                            {ok, {response, Response}} -> %% Do not handle this request and return custom response
                                {ok, Response};
                            {error, PreRouteError} ->
                                ?error("Cannot initialize reouting for request ~p due to error in prerouting handler: ~p", [Request, PreRouteError]),
                                throw({unable_to_reroute_message, {prerouting_error, PreRouteError}})
                        end
                    catch
                        Type:Reason0 ->
                            ?error_stacktrace("Unable to process remote fslogic request due to: ~p", [{Type, Reason0}]),
                            {error, {Type, Reason0}}
                    end,
                case fslogic_remote:postrouting(NextCTX, PrePostProcessResponse, Request) of
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
%% Returns a FUSE response with translated error description.
%% Logs an error with given log level.
%% @end
%%--------------------------------------------------------------------
-spec report_error(Request :: any(), fuse_request | proxyio_request, Error :: term(),
    LogLevel :: debug | warning | error) -> #fuse_response{} | #proxyio_response{}.
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
-spec error_response(fuse_request | proxyio_request, #status{}) -> #fuse_response{}.
error_response(fuse_request, Status) ->
    #fuse_response{status = Status};
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
handle_fuse_request(Ctx, #fuse_request{fuse_request = #get_file_attr{entry = Entry}}) ->
    fslogic_req_generic:get_file_attr(Ctx, Entry);
handle_fuse_request(Ctx, #fuse_request{fuse_request = #delete_file{uuid = UUID}}) ->
    fslogic_req_generic:delete(Ctx, {file, {uuid, UUID}});
handle_fuse_request(Ctx, #fuse_request{fuse_request = #create_dir{parent_uuid = ParentUUID, name = Name, mode = Mode}}) ->
    fslogic_req_special:mkdir(Ctx, {uuid, ParentUUID}, Name, Mode);
handle_fuse_request(Ctx, #fuse_request{fuse_request = #get_file_children{uuid = UUID, offset = Offset, size = Size}}) ->
    fslogic_req_special:read_dir(Ctx, {file, {uuid, UUID}}, Offset, Size);
handle_fuse_request(Ctx, #fuse_request{fuse_request = #get_parent{uuid = UUID}}) ->
    fslogic_req_regular:get_parent(Ctx, {file, {uuid, UUID}});
handle_fuse_request(Ctx, #fuse_request{fuse_request = #change_mode{uuid = UUID, mode = Mode}}) ->
    fslogic_req_generic:chmod(Ctx, {file, {uuid, UUID}}, Mode);
handle_fuse_request(Ctx, #fuse_request{fuse_request = #rename{uuid = UUID, target_path = TargetPath}}) ->
    {ok, Tokens} = fslogic_path:verify_file_path(TargetPath),
    CanonicalFileEntry = fslogic_path:get_canonical_file_entry(Ctx, Tokens),
    {ok, CanonicalTargetPath} = file_meta:gen_path(CanonicalFileEntry),
    fslogic_req_generic:rename(Ctx, {file, {uuid, UUID}}, CanonicalTargetPath);
handle_fuse_request(Ctx, #fuse_request{fuse_request = #update_times{uuid = UUID, atime = ATime, mtime = MTime, ctime = CTime}}) ->
    fslogic_req_generic:update_times(Ctx, {file, {uuid, UUID}}, ATime, MTime, CTime);
handle_fuse_request(Ctx, #fuse_request{fuse_request = #get_new_file_location{name = Name, parent_uuid = ParentUUID,
    flags = Flags, mode = Mode}}) ->
    NewCtx = fslogic_context:set_space_id(Ctx, {uuid, ParentUUID}),
    fslogic_req_regular:get_new_file_location(NewCtx, {uuid, ParentUUID}, Name, Mode, Flags);
handle_fuse_request(Ctx, #fuse_request{fuse_request = #get_file_location{uuid = UUID, flags = Flags}}) ->
    NewCtx = fslogic_context:set_space_id(Ctx, {file, {uuid, UUID}}),
    fslogic_req_regular:get_file_location(NewCtx, {file, {uuid, UUID}}, Flags);
handle_fuse_request(Ctx, #fuse_request{fuse_request = #truncate{uuid = UUID, size = Size}}) ->
    fslogic_req_regular:truncate(Ctx, {file, {uuid, UUID}}, Size);
handle_fuse_request(Ctx, #fuse_request{fuse_request = #get_helper_params{storage_id = SID, force_cluster_proxy = ForceCL}}) ->
    fslogic_req_regular:get_helper_params(Ctx, SID, ForceCL);
handle_fuse_request(Ctx, #fuse_request{fuse_request = #get_xattr{uuid = UUID, name = XattrName}}) ->
    fslogic_req_generic:get_xattr(Ctx, {file, {uuid, UUID}}, XattrName);
handle_fuse_request(Ctx, #fuse_request{fuse_request = #set_xattr{uuid = UUID, xattr = Xattr}}) ->
    fslogic_req_generic:set_xattr(Ctx, {file, {uuid, UUID}}, Xattr);
handle_fuse_request(Ctx, #fuse_request{fuse_request = #remove_xattr{uuid = UUID, name = XattrName}}) ->
    fslogic_req_generic:remove_xattr(Ctx, {file, {uuid, UUID}}, XattrName);
handle_fuse_request(Ctx, #fuse_request{fuse_request = #list_xattr{uuid = UUID}}) ->
    fslogic_req_generic:list_xattr(Ctx, {file, {uuid, UUID}});
handle_fuse_request(Ctx, #fuse_request{fuse_request = #get_acl{uuid = UUID}}) ->
    fslogic_req_generic:get_acl(Ctx, {file, {uuid, UUID}});
handle_fuse_request(Ctx, #fuse_request{fuse_request = #set_acl{uuid = UUID, acl = Acl}}) ->
    fslogic_req_generic:set_acl(Ctx, {file, {uuid, UUID}}, Acl);
handle_fuse_request(Ctx, #fuse_request{fuse_request = #remove_acl{uuid = UUID}}) ->
    fslogic_req_generic:remove_acl(Ctx, {file, {uuid, UUID}});
handle_fuse_request(Ctx, #fuse_request{fuse_request = #get_transfer_encoding{uuid = UUID}}) ->
    fslogic_req_generic:get_transfer_encoding(Ctx, {file, {uuid, UUID}});
handle_fuse_request(Ctx, #fuse_request{fuse_request = #set_transfer_encoding{uuid = UUID, value = Value}}) ->
    fslogic_req_generic:set_transfer_encoding(Ctx, {file, {uuid, UUID}}, Value);
handle_fuse_request(Ctx, #fuse_request{fuse_request = #get_cdmi_completion_status{uuid = UUID}}) ->
    fslogic_req_generic:get_cdmi_completion_status(Ctx, {file, {uuid, UUID}});
handle_fuse_request(Ctx, #fuse_request{fuse_request = #set_cdmi_completion_status{uuid = UUID, value = Value}}) ->
    fslogic_req_generic:set_cdmi_completion_status(Ctx, {file, {uuid, UUID}}, Value);
handle_fuse_request(Ctx, #fuse_request{fuse_request = #get_mimetype{uuid = UUID}}) ->
    fslogic_req_generic:get_mimetype(Ctx, {file, {uuid, UUID}});
handle_fuse_request(Ctx, #fuse_request{fuse_request = #set_mimetype{uuid = UUID, value = Value}}) ->
    fslogic_req_generic:set_mimetype(Ctx, {file, {uuid, UUID}}, Value);
handle_fuse_request(_Ctx, Req) ->
    ?log_bad_request(Req),
    erlang:error({invalid_request, Req}).

handle_events(Evts, #{session_id := SessId} = Ctx) ->
    Results = lists:map(fun(#event{object = #write_event{
        blocks = Blocks, file_uuid = FileUUID, file_size = FileSize
    }}) ->
        case fslogic_blocks:update(FileUUID, Blocks, FileSize) of
            {ok, size_changed} ->
                fslogic_event:emit_file_attr_update({uuid, FileUUID}, [SessId]),
                fslogic_event:emit_file_location_update({uuid, FileUUID}, [SessId]);
            {ok, size_not_changed} ->
                fslogic_event:emit_file_location_update({uuid, FileUUID}, [SessId]);
            {error, Reason} ->
                ?error("Unable to update blocks for file ~p due to: ~p.", [FileUUID, Reason])
        end
                        end, Evts),

    case Ctx of
        #{notify := Pid} -> Pid ! {handler_executed, Results};
        _ -> ok
    end,

    Results.

handle_proxyio_request(#fslogic_ctx{session_id = SessionId}, #proxyio_request{
    file_uuid = FileUuid, storage_id = SID, file_id = FID,
    proxyio_request = #remote_write{offset = Offset, data = Data}}) ->

    fslogic_proxyio:write(SessionId, FileUuid, SID, FID, Offset, Data);

handle_proxyio_request(#fslogic_ctx{session_id = SessionId}, #proxyio_request{
    file_uuid = FileUuid, storage_id = SID, file_id = FID,
    proxyio_request = #remote_read{offset = Offset, size = Size}}) ->

    fslogic_proxyio:read(SessionId, FileUuid, SID, FID, Offset, Size);

handle_proxyio_request(_CTX, Req) ->
    ?log_bad_request(Req),
    erlang:error({invalid_request, Req}).


request_to_file_entry_or_provider(Ctx, #fuse_request{fuse_request = #get_file_attr{entry = {path, Path}}}) ->
    {ok, Tokens} = fslogic_path:verify_file_path(Path),
    {file, fslogic_path:get_canonical_file_entry(Ctx, Tokens)};
request_to_file_entry_or_provider(Ctx, #fuse_request{fuse_request = #get_file_attr{entry = Entry}}) ->
    {file, Entry};
request_to_file_entry_or_provider(Ctx, #fuse_request{fuse_request = #delete_file{uuid = UUID}}) ->
    {file, {uuid, UUID}};
request_to_file_entry_or_provider(Ctx, #fuse_request{fuse_request = #create_dir{parent_uuid = ParentUUID, name = Name, mode = Mode}}) ->
    {file, {uuid, ParentUUID}};
request_to_file_entry_or_provider(Ctx, #fuse_request{fuse_request = #get_file_children{uuid = UUID, offset = Offset, size = Size}}) ->
    {file, {uuid, UUID}};
request_to_file_entry_or_provider(Ctx, #fuse_request{fuse_request = #get_parent{uuid = UUID}}) ->
    {file, {uuid, UUID}};
request_to_file_entry_or_provider(Ctx, #fuse_request{fuse_request = #change_mode{uuid = UUID, mode = Mode}}) ->
    {file, {uuid, UUID}};
request_to_file_entry_or_provider(Ctx, #fuse_request{fuse_request = #rename{uuid = UUID, target_path = TargetPath}}) ->
    {file, {uuid, UUID}};
request_to_file_entry_or_provider(Ctx, #fuse_request{fuse_request = #update_times{uuid = UUID}}) ->
    {file, {uuid, UUID}};
request_to_file_entry_or_provider(Ctx, #fuse_request{fuse_request = #get_new_file_location{parent_uuid = ParentUUID}}) ->
    {file, {uuid, ParentUUID}};
request_to_file_entry_or_provider(Ctx, #fuse_request{fuse_request = #get_file_location{uuid = UUID}}) ->
    {file, {uuid, UUID}};
request_to_file_entry_or_provider(Ctx, #fuse_request{fuse_request = #truncate{uuid = UUID}}) ->
    {file, {uuid, UUID}};
request_to_file_entry_or_provider(Ctx, #fuse_request{fuse_request = #get_helper_params{storage_id = SID, force_cluster_proxy = ForceCL}}) ->
    {provider, oneprovider:get_provider_id()};
request_to_file_entry_or_provider(Ctx, #fuse_request{fuse_request = #get_xattr{uuid = UUID}}) ->
    {file, {uuid, UUID}};
request_to_file_entry_or_provider(Ctx, #fuse_request{fuse_request = #set_xattr{uuid = UUID}}) ->
    {file, {uuid, UUID}};
request_to_file_entry_or_provider(Ctx, #fuse_request{fuse_request = #remove_xattr{uuid = UUID}}) ->
    {file, {uuid, UUID}};
request_to_file_entry_or_provider(Ctx, #fuse_request{fuse_request = #list_xattr{uuid = UUID}}) ->
    {file, {uuid, UUID}};
request_to_file_entry_or_provider(Ctx, #fuse_request{fuse_request = #get_acl{uuid = UUID}}) ->
    {file, {uuid, UUID}};
request_to_file_entry_or_provider(Ctx, #fuse_request{fuse_request = #set_acl{uuid = UUID, acl = Acl}}) ->
    {file, {uuid, UUID}};
request_to_file_entry_or_provider(Ctx, #fuse_request{fuse_request = #remove_acl{uuid = UUID}}) ->
    {file, {uuid, UUID}};
request_to_file_entry_or_provider(Ctx, #fuse_request{fuse_request = #get_transfer_encoding{uuid = UUID}}) ->
    {file, {uuid, UUID}};
request_to_file_entry_or_provider(Ctx, #fuse_request{fuse_request = #set_transfer_encoding{uuid = UUID, value = Value}}) ->
    {file, {uuid, UUID}};
request_to_file_entry_or_provider(Ctx, #fuse_request{fuse_request = #get_cdmi_completion_status{uuid = UUID}}) ->
    {file, {uuid, UUID}};
request_to_file_entry_or_provider(Ctx, #fuse_request{fuse_request = #set_cdmi_completion_status{uuid = UUID, value = Value}}) ->
    {file, {uuid, UUID}};
request_to_file_entry_or_provider(Ctx, #fuse_request{fuse_request = #get_mimetype{uuid = UUID}}) ->
    {file, {uuid, UUID}};
request_to_file_entry_or_provider(Ctx, #fuse_request{fuse_request = #set_mimetype{uuid = UUID, value = Value}}) ->
    {file, {uuid, UUID}};
request_to_file_entry_or_provider(#fslogic_ctx{}, #proxyio_request{file_uuid = UUID}) ->
    {file, {uuid, UUID}};
request_to_file_entry_or_provider(_Ctx, Req) ->
    ?log_bad_request(Req),
    erlang:error({invalid_request, Req}).