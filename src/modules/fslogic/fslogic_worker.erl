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
-include_lib("ctool/include/posix/errors.hrl").
-include_lib("ctool/include/logging.hrl").
-include_lib("ctool/include/global_definitions.hrl").
-include_lib("ctool/include/global_registry/gr_providers.hrl").


-export([init/1, handle/1, cleanup/0, handle_fuse_request/2]).

-define(RTRANSFER_PORT, 6665).
-define(RTRANSFER_NUM_ACCEPTORS, 10).

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

    start_rtransfer(),

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
    run_and_catch_exceptions(fun handle_proxyio_request/2, SessId, ProxyIORequest, proxyio_request);
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
-spec run_and_catch_exceptions(Function :: function(), ctx() | session:id(), any(),
    fuse_request | proxyio_request) -> #fuse_response{} | #proxyio_response{}.
run_and_catch_exceptions(Function, Context, Request, Type) ->
    try
        apply(Function, [Context, Request])
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
    LogLevel :: debug | warning | error) ->  #fuse_response{} | #proxyio_response{}.
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
-spec handle_fuse_request(Ctx :: fslogic_worker:ctx(), FuseRequest :: fuse_request()) ->
    FuseResponse :: #fuse_response{}.
handle_fuse_request(Ctx, #get_file_attr{entry = {path, Path}}) ->
    {ok, Tokens} = fslogic_path:verify_file_path(Path),
    CanonicalFileEntry = fslogic_path:get_canonical_file_entry(Ctx, Tokens),
    fslogic_req_generic:get_file_attr(Ctx, CanonicalFileEntry);
handle_fuse_request(Ctx, #get_file_attr{entry = Entry}) ->
    fslogic_req_generic:get_file_attr(Ctx, Entry);
handle_fuse_request(Ctx, #delete_file{uuid = UUID}) ->
    fslogic_req_generic:delete(Ctx, {uuid, UUID});
handle_fuse_request(Ctx, #create_dir{parent_uuid = ParentUUID, name = Name, mode = Mode}) ->
    fslogic_req_special:mkdir(Ctx, {uuid, ParentUUID}, Name, Mode);
handle_fuse_request(Ctx, #get_file_children{uuid = UUID, offset = Offset, size = Size}) ->
    fslogic_req_special:read_dir(Ctx, {uuid, UUID}, Offset, Size);
handle_fuse_request(Ctx, #get_parent{uuid = UUID}) ->
    fslogic_req_regular:get_parent(Ctx, {uuid, UUID});
handle_fuse_request(Ctx, #change_mode{uuid = UUID, mode = Mode}) ->
    fslogic_req_generic:chmod(Ctx, {uuid, UUID}, Mode);
handle_fuse_request(Ctx, #rename{uuid = UUID, target_path = TargetPath}) ->
    {ok, Tokens} = fslogic_path:verify_file_path(TargetPath),
    CanonicalFileEntry = fslogic_path:get_canonical_file_entry(Ctx, Tokens),
    {ok, CanonicalTargetPath} = file_meta:gen_path(CanonicalFileEntry),
    fslogic_req_generic:rename(Ctx, {uuid, UUID}, CanonicalTargetPath);
handle_fuse_request(Ctx, #update_times{uuid = UUID, atime = ATime, mtime = MTime, ctime = CTime}) ->
    fslogic_req_generic:update_times(Ctx, {uuid, UUID}, ATime, MTime, CTime);
handle_fuse_request(Ctx, #get_new_file_location{name = Name, parent_uuid = ParentUUID, flags = Flags, mode = Mode}) ->
    NewCtx = fslogic_context:set_space_id(Ctx, {uuid, ParentUUID}),
    fslogic_req_regular:get_new_file_location(NewCtx, {uuid, ParentUUID}, Name, Mode, Flags);
handle_fuse_request(Ctx, #get_file_location{uuid = UUID, flags = Flags}) ->
    NewCtx = fslogic_context:set_space_id(Ctx, {uuid, UUID}),
    fslogic_req_regular:get_file_location(NewCtx, {uuid, UUID}, Flags);
handle_fuse_request(Ctx, #truncate{uuid = UUID, size = Size}) ->
    fslogic_req_regular:truncate(Ctx, {uuid, UUID}, Size);
handle_fuse_request(Ctx, #get_helper_params{storage_id = SID, force_cluster_proxy = ForceCL}) ->
    fslogic_req_regular:get_helper_params(Ctx, SID, ForceCL);
handle_fuse_request(Ctx, #get_xattr{uuid = UUID, name = XattrName}) ->
    fslogic_req_generic:get_xattr(Ctx, {uuid, UUID}, XattrName);
handle_fuse_request(Ctx, #set_xattr{uuid = UUID, xattr = Xattr}) ->
    fslogic_req_generic:set_xattr(Ctx, {uuid, UUID}, Xattr);
handle_fuse_request(Ctx, #remove_xattr{uuid = UUID, name = XattrName}) ->
    fslogic_req_generic:remove_xattr(Ctx, {uuid, UUID}, XattrName);
handle_fuse_request(Ctx, #list_xattr{uuid = UUID}) ->
    fslogic_req_generic:list_xattr(Ctx, {uuid, UUID});
handle_fuse_request(Ctx, #get_acl{uuid = UUID}) ->
    fslogic_req_generic:get_acl(Ctx, {uuid, UUID});
handle_fuse_request(Ctx, #set_acl{uuid = UUID, acl = Acl}) ->
    fslogic_req_generic:set_acl(Ctx, {uuid, UUID}, Acl);
handle_fuse_request(Ctx, #remove_acl{uuid = UUID}) ->
    fslogic_req_generic:remove_acl(Ctx, {uuid, UUID});
handle_fuse_request(Ctx, #get_transfer_encoding{uuid = UUID}) ->
    fslogic_req_generic:get_transfer_encoding(Ctx, {uuid, UUID});
handle_fuse_request(Ctx, #set_transfer_encoding{uuid = UUID, value = Value}) ->
    fslogic_req_generic:set_transfer_encoding(Ctx, {uuid, UUID}, Value);
handle_fuse_request(Ctx, #get_cdmi_completion_status{uuid = UUID}) ->
    fslogic_req_generic:get_cdmi_completion_status(Ctx, {uuid, UUID});
handle_fuse_request(Ctx, #set_cdmi_completion_status{uuid = UUID, value = Value}) ->
    fslogic_req_generic:set_cdmi_completion_status(Ctx, {uuid, UUID}, Value);
handle_fuse_request(Ctx, #get_mimetype{uuid = UUID}) ->
    fslogic_req_generic:get_mimetype(Ctx, {uuid, UUID});
handle_fuse_request(Ctx, #set_mimetype{uuid = UUID, value = Value}) ->
    fslogic_req_generic:set_mimetype(Ctx, {uuid, UUID}, Value);
handle_fuse_request(Ctx, #synchronize_block{uuid = UUID, block = Block}) ->
    fslogic_req_regular:synchronize_block(Ctx, {uuid, UUID}, Block);
handle_fuse_request(_Ctx, Req) ->
    ?log_bad_request(Req),
    erlang:error({invalid_request, Req}).

handle_events(Evts, #{session_id := SessId} = Ctx) ->
    Results = lists:map(fun(#event{object = #write_event{
        blocks = Blocks, file_uuid = FileUUID, file_size = FileSize
    }}) ->
        case replica_updater:update(FileUUID, Blocks, FileSize, true) of
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

handle_proxyio_request(SessionId, #proxyio_request{
    file_uuid = FileUuid, storage_id = SID, file_id = FID,
    proxyio_request = #remote_write{offset = Offset, data = Data}}) ->

    fslogic_proxyio:write(SessionId, FileUuid, SID, FID, Offset, Data);

handle_proxyio_request(SessionId, #proxyio_request{
    file_uuid = FileUuid, storage_id = SID, file_id = FID,
    proxyio_request = #remote_read{offset = Offset, size = Size}}) ->

    fslogic_proxyio:read(SessionId, FileUuid, SID, FID, Offset, Size);

handle_proxyio_request(_SessionId, Req) ->
    ?log_bad_request(Req),
    erlang:error({invalid_request, Req}).

%%--------------------------------------------------------------------
%% @doc
%% Start rtransfer server
%% @end
%%--------------------------------------------------------------------
-spec start_rtransfer() -> {ok, pid()}.
start_rtransfer() ->
    Opts = [
        {get_nodes_fun,
            fun(ProviderId) ->
                {ok, #provider_details{urls = URLs}} = gr_providers:get_details(provider, ProviderId),
                lists:map(
                    fun(URL) ->
                        {ok, Ip} = inet:ip(binary_to_list(URL)),
                        {Ip, ?RTRANSFER_PORT}
                    end, URLs)
            end},
        {open_fun,
            fun(FileUUID, OpenMode) ->
                case logical_file_manager:open(?ROOT_SESS_ID, {uuid, FileUUID}, OpenMode) of
                    {ok, Handle} ->
                        {ok, term_to_binary(Handle)};
                    Error ->
                        Error
                end
            end},
        {read_fun,
            fun(Handle, Offset, Size) ->
                case logical_file_manager:read(binary_to_term(Handle), Offset, Size) of
                    {ok, Handle, Data} ->
                        {ok, term_to_binary(Handle), Data};
                    Error ->
                        Error
                end
            end},
        {write_fun,
            fun(Handle, Offset, Buffer) ->
                case logical_file_manager:write(binary_to_term(Handle), Offset, Buffer) of
                    {ok, Handle, Size} ->
                        {ok, term_to_binary(Handle), Size};
                    Error ->
                        Error
                end
            end},
        {close_fun,
            fun(_Handle) ->
                ok
            end},
        {ranch_opts,
            [
                {num_acceptors, ?RTRANSFER_NUM_ACCEPTORS},
                {transport, ranch_tcp},
                {trans_opts, [{port, ?RTRANSFER_PORT}]}
            ]
        }
    ],
    {ok, _} = rtransfer:start_link(Opts).