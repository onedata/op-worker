%%%-------------------------------------------------------------------
%%% @author Bartosz Walkowicz
%%% @copyright (C) 2019-2021 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% Monitors activity of file uploads. If no activity (writes) happens
%%% for longer than allowed ?INACTIVITY_PERIOD it is assumed that GUI
%%% lost connection to backend and no more file chunks will be uploaded.
%%% Such damaged files will be deleted.
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
-export([register_upload/2, is_upload_registered/2, deregister_upload/2]).

%% gen_server callbacks
-export([
    init/1,
    handle_call/3, handle_cast/2, handle_info/2,
    terminate/2, code_change/3
]).


-type uploads() :: #{file_id:file_guid() => {od_user:id(), non_neg_integer()}}.

-record(state, {
    uploads = #{} :: uploads(),
    checkup_timer = undefined :: undefined | reference()
}).
-type state() :: #state{}.

-type error() :: {error, Reason :: term()}.


-define(SERVER, {global, ?MODULE}).

-define(CHECK_UPLOADS_REQ, check_uploads).

-define(REGISTER_UPLOAD_REQ(UserId, FileGuid), {register, UserId, FileGuid}).
-define(IS_UPLOAD_REGISTERED(UserId, FileGuid), {is_registered, UserId, FileGuid}).
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


-spec is_upload_registered(od_user:id(), file_id:file_guid()) -> boolean().
is_upload_registered(UserId, FileGuid) ->
    case call_server(?IS_UPLOAD_REGISTERED(UserId, FileGuid)) of
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
    {reply, Reply :: term(), NewState :: state(), timeout() | hibernate}.
handle_call(?REGISTER_UPLOAD_REQ(UserId, FileGuid), _, #state{uploads = Uploads} = State) ->
    {reply, ok, maybe_schedule_uploads_checkup(State#state{
        uploads = Uploads#{FileGuid => {UserId, ?NOW() + ?INACTIVITY_PERIOD_SEC()}}
    })};

handle_call(?IS_UPLOAD_REGISTERED(UserId, FileGuid), _, #state{uploads = Uploads} = State) ->
    IsRegistered = case maps:find(FileGuid, Uploads) of
        {ok, {UserId, _}} ->
            true;
        _ ->
            false
    end,
    {reply, IsRegistered, State};

handle_call(Request, _From, State) ->
    ?log_bad_request(Request),
    {reply, {error, wrong_request}, State}.


%%--------------------------------------------------------------------
%% @private
%% @doc
%% Handles cast messages
%% @end
%%--------------------------------------------------------------------
-spec handle_cast(Request :: term(), state()) ->
    {noreply, NewState :: state()} |
    {noreply, NewState :: state(), hibernate}.
handle_cast(?DEREGISTER_UPLOAD_REQ(UserId, FileGuid), #state{uploads = Uploads} = State) ->
    NewState = case maps:take(FileGuid, Uploads) of
        {{UserId, _}, ActiveUploads} ->
            State#state{uploads = ActiveUploads};
        _ ->
            State
    end,
    case maps:size(NewState#state.uploads) of
        0 -> {noreply, cancel_uploads_checkup(NewState), hibernate};
        _ -> {noreply, NewState}
    end;

handle_cast(Request, State) ->
    ?log_bad_request(Request),
    {noreply, State}.


%%--------------------------------------------------------------------
%% @private
%% @doc
%% Handles all non call/cast messages
%% @end
%%--------------------------------------------------------------------
-spec handle_info(Info :: term(), state()) ->
    {noreply, NewState :: state()} |
    {noreply, NewState :: state(), hibernate}.
handle_info(?CHECK_UPLOADS_REQ, #state{uploads = Uploads} = State) ->
    NewState = State#state{
        uploads = ActiveUploads = remove_stale_uploads(Uploads),
        checkup_timer = undefined
    },
    case maps:size(ActiveUploads) of
        0 -> {noreply, NewState, hibernate};
        _ -> {noreply, maybe_schedule_uploads_checkup(NewState)}
    end;

handle_info(Info, State) ->
    ?log_bad_request(Info),
    {noreply, State}.


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


%%--------------------------------------------------------------------
%% @private
%% @doc
%% Checks time since last modification of file and if it's beyond
%% inactivity period deletes file because of interrupted and
%% unfinished upload.
%% @end
%%--------------------------------------------------------------------
-spec remove_stale_uploads(uploads()) -> uploads().
remove_stale_uploads(Uploads) ->
    Now = ?NOW(),
    InactivityPeriod = ?INACTIVITY_PERIOD_SEC(),

    maps:fold(fun
        (FileGuid, {UserId, CheckupTime}, Acc) when CheckupTime < Now ->
            FileRef = ?FILE_REF(FileGuid),

            case lfm:stat(?ROOT_SESS_ID, FileRef) of
                {ok, #file_attr{mtime = MTime}} when MTime + InactivityPeriod > Now ->
                    Acc#{FileGuid => {UserId, MTime + InactivityPeriod}};
                {ok, _} ->
                    lfm:unlink(?ROOT_SESS_ID, FileRef, false),
                    Acc;
                {error, _} ->
                    Acc
            end;
        (FileGuid, FileCheckup, Acc) ->
            Acc#{FileGuid => FileCheckup}
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
-spec maybe_schedule_uploads_checkup(state()) -> state().
maybe_schedule_uploads_checkup(#state{checkup_timer = undefined} = State) ->
    State#state{checkup_timer = erlang:send_after(
        ?UPLOADS_CHECKUP_INTERVAL, self(), ?CHECK_UPLOADS_REQ
    )};
maybe_schedule_uploads_checkup(State) ->
    State.


%% @private
-spec cancel_uploads_checkup(state()) -> state().
cancel_uploads_checkup(#state{checkup_timer = undefined} = State) ->
    State;
cancel_uploads_checkup(#state{checkup_timer = TimerRef} = State) ->
    erlang:cancel_timer(TimerRef, [{async, true}, {info, false}]),
    State#state{checkup_timer = undefined}.
