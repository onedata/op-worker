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

-include("errors.hrl").
-include("global_definitions.hrl").
-include("modules/fslogic/fslogic_common.hrl").
-include("proto/oneclient/fuse_messages.hrl").
-include("proto/oneclient/common_messages.hrl").
-include("modules/event_manager/events.hrl").
-include_lib("ctool/include/logging.hrl").


-export([init/1, handle/1, cleanup/0, handle_fuse_request/2]).

%%%===================================================================
%%% Types
%%%===================================================================

-type ctx() :: #fslogic_ctx{}.
-type file() :: file_meta:entry(). %% Type alias for better code organization
-type open_flags() :: 'READ_WRITE' | 'READ' | 'WRITE'.
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
    Sub = #write_event_subscription{
        producer = all,
        producer_counter_threshold = CounterThreshold,
        producer_time_threshold = TimeThreshold,
        producer_size_threshold = SizeThreshold,
        event_stream = ?WRITE_EVENT_STREAM#event_stream{
            metadata = 0,
            emission_time = 500,
            emission_rule = fun(_) -> true end,
            handlers = [fun(Evts) -> handle_events(Evts) end]
        }
    },
    SubId = binary:decode_unsigned(crypto:hash(md5, atom_to_binary(?MODULE, utf8))) rem 16#FFFFFFFFFFFF,

    case event_manager:subscribe(SubId, Sub) of
        ok ->
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
    maybe_handle_fuse_request(SessId, FuseRequest);
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
%% Handles or forwards FUSE request. In case of error translates it using
%% fslogic_errors:translate_error/1 function.
%% @end
%%--------------------------------------------------------------------
-spec maybe_handle_fuse_request(SessId :: session:id(), FuseRequest :: #fuse_request{}) ->
    FuseResponse :: #fuse_response{}.
maybe_handle_fuse_request(SessId, FuseRequest) ->
    try
        ?debug("Processing request: ~p", [FuseRequest]),
        Resp = handle_fuse_request(fslogic_context:new(SessId), FuseRequest),
        ?debug("Fuse request ~p from user ~p: ~p", [FuseRequest, SessId, Resp]),
        Resp
    catch
        Reason ->
            %% Manually thrown error, normal interrupt case.
            report_error(FuseRequest, Reason, debug);
        error:{badmatch, Reason} ->
            %% Bad Match assertion - something went wrong, but it could be expected (e.g. file not found assertion).
            report_error(FuseRequest, Reason, warning);
        error:{case_clause, Reason} ->
            %% Case Clause assertion - something went seriously wrong and we should know about it.
            report_error(FuseRequest, Reason, error);
        error:Reason ->
            %% Something went horribly wrong. This should not happen.
            report_error(FuseRequest, Reason, error)
    end.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Returns a FUSE response with translated error description.
%% Logs an error with given log level.
%% @end
%%--------------------------------------------------------------------
-spec report_error(FuseRequest :: #fuse_request{}, Error :: term(),
    LogLevel :: debug | warning | error) -> FuseResponse :: #fuse_response{}.
report_error(FuseRequest, Error, LogLevel) ->
    Status = #status{code = Code, description = Description} =
        fslogic_errors:gen_status_message(Error),
    MsgFormat = "Cannot process request ~p due to error: ~p (code: ~p)",
    case LogLevel of
        debug -> ?debug_stacktrace(MsgFormat, [FuseRequest, Description, Code]);
%%         info -> ?info(MsgFormat, [FuseRequest, Description, Code]);  %% Not used right now
        warning -> ?warning_stacktrace(MsgFormat, [FuseRequest, Description, Code]);
        error -> ?error_stacktrace(MsgFormat, [FuseRequest, Description, Code])
    end,
    #fuse_response{status = Status}.

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
    fslogic_req_generic:delete_file(Ctx, {uuid, UUID});
handle_fuse_request(Ctx, #create_dir{parent_uuid = ParentUUID, name = Name, mode = Mode}) ->
    fslogic_req_special:mkdir(Ctx, ParentUUID, Name, Mode);
handle_fuse_request(Ctx, #get_file_children{uuid = UUID, offset = Offset, size = Size}) ->
    fslogic_req_special:read_dir(Ctx, {uuid, UUID}, Offset, Size);
handle_fuse_request(Ctx, #change_mode{uuid = UUID, mode = Mode}) ->
    fslogic_req_generic:chmod(Ctx, {uuid, UUID}, Mode);
handle_fuse_request(Ctx, #rename{uuid = UUID, target_path = TargetPath}) ->
    {ok, Tokens} = fslogic_path:verify_file_path(TargetPath),
    CanonicalFileEntry = fslogic_path:get_canonical_file_entry(Ctx, Tokens),
    {ok, CanonicalTargetPath} = file_meta:gen_path(CanonicalFileEntry),
    fslogic_req_generic:rename_file(Ctx, {uuid, UUID}, CanonicalTargetPath);
handle_fuse_request(Ctx, #update_times{uuid = UUID, atime = ATime, mtime = MTime, ctime = CTime}) ->
    fslogic_req_generic:update_times(Ctx, {uuid, UUID}, ATime, MTime, CTime);
handle_fuse_request(Ctx, #get_new_file_location{name = Name, parent_uuid = ParentUUID, flags = Flags, mode = Mode}) ->
    fslogic_req_regular:get_new_file_location(Ctx, ParentUUID, Name, Mode, Flags);
handle_fuse_request(Ctx, #get_file_location{uuid = UUID, flags = Flags}) ->
    fslogic_req_regular:get_file_location(Ctx, {uuid, UUID}, Flags);
handle_fuse_request(Ctx, #truncate{uuid = UUID, size = Size}) ->
    fslogic_req_regular:truncate(Ctx, {uuid, UUID}, Size);
handle_fuse_request(Ctx, #get_helper_params{storage_id = SID, force_cluster_proxy = ForceCL}) ->
    fslogic_req_regular:get_helper_params(Ctx, SID, ForceCL);
handle_fuse_request(Ctx, #unlink{uuid = UUID}) ->
    fslogic_req_generic:delete_file(Ctx, {uuid, UUID});
handle_fuse_request(_Ctx, Req) ->
    ?log_bad_request(Req),
    erlang:error({invalid_request, Req}).

handle_events([]) ->
    [];
handle_events([Event | T]) ->
    [handle_events(Event) | handle_events(T)];
handle_events(#write_event{blocks = Blocks, file_uuid = FileUUID, file_size = FileSize, source = Source} = T) ->
    ?debug("fslogic handle_events: ~p", [T]),
    ExcludedSessions =
        case Source of
            {session, SessionId} ->
                [SessionId]
        end,

    case fslogic_blocks:update(FileUUID, Blocks, FileSize) of
        {ok, size_changed} ->
            fslogic_notify:attributes({uuid, FileUUID}, ExcludedSessions),
            fslogic_notify:blocks({uuid, FileUUID}, Blocks, ExcludedSessions);
        {ok, size_not_changed} ->
            fslogic_notify:blocks({uuid, FileUUID}, Blocks, ExcludedSessions);
        {error, Reason} ->
            {error, Reason}
    end.