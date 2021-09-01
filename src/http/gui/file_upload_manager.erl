%%%-------------------------------------------------------------------
%%% @author Bartosz Walkowicz
%%% @copyright (C) 2019-2021 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% Monitors activity of file uploads which is measured by existence of processes
%%% uploading chunks of file (it is assumed that each chunk is handled by separate
%%% process which terminates right after uploading entire chunk).
%%% If no activity (writes) happens for longer than allowed inactivity period
%%% it is assumed that GUI lost connection to backend and no more file chunks
%%% will be uploaded. Such partially uploaded files will be deleted.
%%% @end
%%%-------------------------------------------------------------------
-module(file_upload_manager).
-author("Bartosz Walkowicz").

-behaviour(gen_server).

-include("modules/fslogic/fslogic_common.hrl").
-include("modules/logical_file_manager/lfm.hrl").
-include("timeouts.hrl").
-include_lib("ctool/include/errors.hrl").
-include_lib("ctool/include/logging.hrl").

%% API
-export([start_service/0, start_internal/0]).
-export([register_upload/2, authorize_chunk_upload/2, deregister_upload/2]).

%% gen_server callbacks
-export([
    init/1,
    handle_call/3, handle_cast/2, handle_info/2,
    terminate/2, code_change/3
]).


-record(upload_ctx, {
    user_id :: od_user:id(),
    monitors :: ordsets:ordset(reference()),
    latest_activity_timestamp :: time:seconds()
}).
-type upload_ctx() :: #upload_ctx{}.
-type uploads() :: #{file_id:file_guid() => upload_ctx()}.

-record(state, {
    uploads = #{} :: uploads(),
    monitor_to_file_mapping = #{} :: #{reference() => file_id:file_guid()},

    checkup_timer = undefined :: undefined | reference()
}).
-type state() :: #state{}.

-type error() :: {error, Reason :: term()}.


-define(SERVER, {global, ?MODULE}).

-define(CHECK_UPLOADS_REQ, check_uploads).

-define(REGISTER_UPLOAD_REQ(UserId, FileGuid), {register, UserId, FileGuid}).
-define(AUTHORIZE_CHUNK_UPLOAD(UserId, FileGuid), {authorize_chunk, UserId, FileGuid}).
-define(DEREGISTER_UPLOAD_REQ(UserId, FileGuid), {deregister, UserId, FileGuid}).

-define(NOW(), global_clock:timestamp_seconds()).
-define(INACTIVITY_PERIOD_SEC(), op_worker:get_env(upload_inactivity_period_sec, 60)).
-define(UPLOADS_CHECKUP_INTERVAL, timer:minutes(1)).


%%%===================================================================
%%% API
%%%===================================================================


-spec start_service() -> ok | aborted.
start_service() ->
    internal_services_manager:start_service(?MODULE, <<?MODULE_STRING>>, #{
        start_function => start_internal
    }).


-spec start_internal() -> ok | abort.
start_internal() ->
    case gen_server2:start(?SERVER, ?MODULE, [], []) of
        {ok, _} -> ok;
        _ -> abort
    end.


-spec register_upload(od_user:id(), file_id:file_guid()) -> ok | error().
register_upload(UserId, FileGuid) ->
    call_server(?REGISTER_UPLOAD_REQ(UserId, FileGuid)).


-spec authorize_chunk_upload(od_user:id(), file_id:file_guid()) -> boolean().
authorize_chunk_upload(UserId, FileGuid) ->
    case call_server(?AUTHORIZE_CHUNK_UPLOAD(UserId, FileGuid)) of
        true -> true;
        _ -> false
    end.


-spec deregister_upload(od_user:id(), file_id:file_guid()) -> ok.
deregister_upload(UserId, FileGuid) ->
    gen_server2:cast(?SERVER, ?DEREGISTER_UPLOAD_REQ(UserId, FileGuid)).


%%%===================================================================
%%% gen_server callbacks
%%%===================================================================


%%--------------------------------------------------------------------
%% @private
%% @doc
%% Initializes the server.
%% @end
%%--------------------------------------------------------------------
-spec init(Args :: term()) -> {ok, state(), hibernate}.
init(_) ->
    {ok, #state{}, hibernate}.


%%--------------------------------------------------------------------
%% @private
%% @doc
%% Handles call messages
%% @end
%%--------------------------------------------------------------------
-spec handle_call(Request :: term(), From :: {pid(), Tag :: term()}, state()) ->
    {reply, Reply :: term(), NewState :: state()} |
    {reply, Reply :: term(), NewState :: state(), hibernate}.
handle_call(?REGISTER_UPLOAD_REQ(UserId, FileGuid), _, #state{uploads = Uploads} = State) ->
    UploadCtx = #upload_ctx{
        user_id = UserId,
        monitors = ordsets:new(),
        latest_activity_timestamp = ?NOW()
    },  %% TODO check if not registered ?
    reply(ok, State#state{uploads = Uploads#{FileGuid => UploadCtx}});

handle_call(?AUTHORIZE_CHUNK_UPLOAD(UserId, FileGuid), {ClientPid, _}, State = #state{
    uploads = Uploads,
    monitor_to_file_mapping = MonitorToFileMapping
}) ->
    case maps:find(FileGuid, Uploads) of
        {ok, #upload_ctx{user_id = UserId, monitors = Monitors} = UploadCtx} ->
            Monitor = erlang:monitor(process, ClientPid),
            NewUploadCtx = UploadCtx#upload_ctx{monitors = ordsets:add_element(
                Monitor, Monitors
            )},
            reply(true, State#state{
                uploads = Uploads#{FileGuid => NewUploadCtx},
                monitor_to_file_mapping = MonitorToFileMapping#{Monitor => FileGuid}
            });
        _ ->
            reply(false, State)
    end;

handle_call(Request, _From, State) ->
    ?log_bad_request(Request),
    reply({error, wrong_request}, State).


%%--------------------------------------------------------------------
%% @private
%% @doc
%% Handles cast messages
%% @end
%%--------------------------------------------------------------------
-spec handle_cast(Request :: term(), state()) ->
    {noreply, NewState :: state()} |
    {noreply, NewState :: state(), hibernate}.
handle_cast(?DEREGISTER_UPLOAD_REQ(UserId, FileGuid), State = #state{
    uploads = Uploads,
    monitor_to_file_mapping = MonitorToFileMapping
}) ->
    NewState = case maps:take(FileGuid, Uploads) of
        {#upload_ctx{user_id = UserId, monitors = Monitors}, ActiveUploads} ->
            MonitorsList = ordsets:to_list(Monitors),
            lists:foreach(fun(Ref) -> erlang:demonitor(Ref, [flush]) end, MonitorsList),

            State#state{
                uploads = ActiveUploads,
                monitor_to_file_mapping = maps:without(MonitorsList, MonitorToFileMapping)
            };
        _ ->
            State
    end,
    noreply(NewState);

handle_cast(Request, State) ->
    ?log_bad_request(Request),
    noreply(State).


%%--------------------------------------------------------------------
%% @private
%% @doc
%% Handles all non call/cast messages
%% @end
%%--------------------------------------------------------------------
-spec handle_info(Info :: term(), state()) ->
    {noreply, NewState :: state()} |
    {noreply, NewState :: state(), hibernate}.
handle_info({'DOWN', Monitor, process, _, _}, State = #state{
    uploads = Uploads,
    monitor_to_file_mapping = MonitorToFileMapping
}) ->
    NewState = case maps:take(Monitor, MonitorToFileMapping) of
        error ->
            State;
        {FileGuid, NewMonitorToFileMapping} ->
            UploadCtx = #upload_ctx{monitors = Monitors} = maps:get(FileGuid, Uploads),
            NewUploadCtx = UploadCtx#upload_ctx{
                monitors = ordsets:del_element(Monitor, Monitors),
                latest_activity_timestamp = ?NOW()
            },
            State#state{
                uploads = Uploads#{FileGuid => NewUploadCtx},
                monitor_to_file_mapping = NewMonitorToFileMapping
            }
    end,
    noreply(NewState);

handle_info(?CHECK_UPLOADS_REQ, #state{uploads = Uploads} = State) ->
    noreply(State#state{
        uploads = remove_stale_uploads(Uploads),
        checkup_timer = undefined
    });

handle_info(Info, State) ->
    ?log_bad_request(Info),
    noreply(State).


%%--------------------------------------------------------------------
%% @private
%% @doc
%% This function is called by a gen_server when it is about to
%% terminate. It should be the opposite of Module:init/1 and do any
%% necessary cleaning up. When it returns, the gen_server terminates
%% with Reason. The return value is ignored.
%% @end
%%--------------------------------------------------------------------
-spec terminate(Reason :: (normal | shutdown | {shutdown, term()} | term()),
    state()) -> term().
terminate(_Reason, _State) ->
    ok.


%%--------------------------------------------------------------------
%% @private
%% @doc
%% Converts process state when code is changed
%% @end
%%--------------------------------------------------------------------
-spec code_change(OldVsn :: term() | {down, term()}, state(), Extra :: term()) ->
    {ok, NewState :: state()} | error().
code_change(_OldVsn, State, _Extra) ->
    {ok, State}.


%%%===================================================================
%%% Internal functions
%%%===================================================================


%% @private
-spec reply(Response :: term(), state()) ->
    {reply, Reply :: term(), NewState :: state()} |
    {reply, Reply :: term(), NewState :: state(), hibernate}.
reply(Response, #state{uploads = Uploads} = State) ->
    case maps:size(Uploads) of
        0 -> {reply, Response, cancel_uploads_checkup(State), hibernate};
        _ -> {reply, Response, schedule_uploads_checkup(State)}
    end.


%% @private
-spec noreply(state()) ->
    {noreply, NewState :: state()} |
    {noreply, NewState :: state(), hibernate}.
noreply(#state{uploads = Uploads} = State) ->
    case maps:size(Uploads) of
        0 -> {noreply, cancel_uploads_checkup(State), hibernate};
        _ -> {noreply, schedule_uploads_checkup(State)}
    end.


%% @private
-spec remove_stale_uploads(uploads()) -> uploads().
remove_stale_uploads(Uploads) ->
    Now = ?NOW(),
    InactivityPeriod = ?INACTIVITY_PERIOD_SEC(),

    maps:fold(fun
        (FileGuid, UploadCtx = #upload_ctx{latest_activity_timestamp = Timestamp}, Acc) when
            Now < Timestamp - InactivityPeriod
        ->
            % backward time warp must have occurred - timestamp must be adjusted to new point in time
            Acc#{FileGuid => UploadCtx#upload_ctx{latest_activity_timestamp = Now}};
        (FileGuid, UploadCtx, Acc) ->
            Monitors = UploadCtx#upload_ctx.monitors,
            Timestamp = UploadCtx#upload_ctx.latest_activity_timestamp,

            case ordsets:is_empty(Monitors) andalso Timestamp + InactivityPeriod < Now of
                true ->
                    lfm:unlink(?ROOT_SESS_ID, ?FILE_REF(FileGuid), false),
                    Acc;
                false ->
                    Acc#{FileGuid => UploadCtx}
            end
    end, #{}, Uploads).


%% @private
-spec call_server(term()) -> ok | boolean() | error().
call_server(Request) ->
    try
        gen_server2:call(?SERVER, Request, ?DEFAULT_REQUEST_TIMEOUT)
    catch
        exit:{noproc, _} ->
            ?debug("Process '~p' does not exist", [?MODULE]),
            ?ERROR_NOT_FOUND;
        exit:{normal, _} ->
            ?debug("Exit of '~p' process", [?MODULE]),
            ?ERROR_NOT_FOUND;
        exit:{timeout, _} ->
            ?debug("Timeout of '~p' process", [?MODULE]),
            ?ERROR_TIMEOUT;
        Type:Reason ->
            ?error("Cannot call '~p' due to ~p:~p", [?MODULE, Type, Reason]),
            {error, Reason}
    end.


%% @private
-spec schedule_uploads_checkup(state()) -> state().
schedule_uploads_checkup(#state{checkup_timer = undefined} = State) ->
    State#state{checkup_timer = erlang:send_after(
        ?UPLOADS_CHECKUP_INTERVAL, self(), ?CHECK_UPLOADS_REQ
    )};
schedule_uploads_checkup(State) ->
    State.


%% @private
-spec cancel_uploads_checkup(state()) -> state().
cancel_uploads_checkup(#state{checkup_timer = undefined} = State) ->
    State;
cancel_uploads_checkup(#state{checkup_timer = TimerRef} = State) ->
    erlang:cancel_timer(TimerRef, [{async, true}, {info, false}]),
    State#state{checkup_timer = undefined}.
