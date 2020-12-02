%%%-------------------------------------------------------------------
%%% @author Bartosz Walkowicz
%%% @copyright (C) 2019 ACK CYFRONET AGH
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

-include("timeouts.hrl").
-include("modules/fslogic/fslogic_common.hrl").
-include_lib("ctool/include/logging.hrl").
-include_lib("ctool/include/errors.hrl").
-include_lib("ctool/include/posix/file_attr.hrl").

%% API
-export([start_link/0, spec/0]).
-export([register_upload/2, is_upload_registered/2, deregister_upload/2]).

%% gen_server callbacks
-export([
    init/1,
    handle_call/3, handle_cast/2, handle_info/2,
    terminate/2, code_change/3
]).

-record(state, {
    uploads = #{} :: uploads(),
    checkup_timer = undefined :: undefined | reference()
}).

-type uploads() :: #{file_id:file_guid() => {od_user:id(), non_neg_integer()}}.
-type state() :: #state{}.
-type error() :: {error, Reason :: term()}.

-define(NOW(), global_clock:timestamp_seconds()).
-define(INACTIVITY_PERIOD, 60).
-define(UPLOADS_CHECKUP_INTERVAL, ?INACTIVITY_PERIOD * 1000).


%%%===================================================================
%%% API
%%%===================================================================


%%--------------------------------------------------------------------
%% @doc
%% Starts the file_upload_manager server.
%% @end
%%--------------------------------------------------------------------
-spec start_link() -> {ok, pid()} | ignore | error().
start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).


%%--------------------------------------------------------------------
%% @doc
%% Registers upload monitoring for specified file.
%% @end
%%--------------------------------------------------------------------
-spec register_upload(od_user:id(), file_id:file_guid()) -> ok | error().
register_upload(UserId, FileGuid) ->
    call({register, UserId, FileGuid}).


%%--------------------------------------------------------------------
%% @doc
%% Checks if upload for specified session and file is registered.
%% @end
%%--------------------------------------------------------------------
-spec is_upload_registered(od_user:id(), file_id:file_guid()) -> boolean().
is_upload_registered(UserId, FileGuid) ->
    case call({is_registered, UserId, FileGuid}) of
        true -> true;
        _ -> false
    end.


%%--------------------------------------------------------------------
%% @doc
%% Deregisters upload monitoring for specified file.
%% @end
%%--------------------------------------------------------------------
-spec deregister_upload(od_user:id(), file_id:file_guid()) -> ok.
deregister_upload(UserId, FileGuid) ->
    gen_server2:cast(?MODULE, {deregister, UserId, FileGuid}).


%%-------------------------------------------------------------------
%% @doc
%% Returns child spec for ?MODULE to attach it to supervision.
%% @end
%%-------------------------------------------------------------------
-spec spec() -> supervisor:child_spec().
spec() -> #{
    id => ?MODULE,
    start => {?MODULE, start_link, []},
    restart => permanent,
    shutdown => timer:seconds(10),
    type => worker,
    modules => [?MODULE]
}.


%%%===================================================================
%%% gen_server callbacks
%%%===================================================================


%%--------------------------------------------------------------------
%% @private
%% @doc
%% Initializes the server.
%% @end
%%--------------------------------------------------------------------
-spec init(Args :: term()) ->
    {ok, state()} | {ok, state(), timeout() | hibernate} |
    {stop, Reason :: term()} | ignore.
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
    {reply, Reply :: term(), NewState :: state(), timeout() | hibernate} |
    {noreply, NewState :: state()} |
    {noreply, NewState :: state(), timeout() | hibernate} |
    {stop, Reason :: term(), Reply :: term(), NewState :: state()} |
    {stop, Reason :: term(), NewState :: state()}.
handle_call({register, UserId, FileGuid}, _, #state{uploads = Uploads} = State) ->
    {reply, ok, maybe_schedule_uploads_checkup(State#state{
        uploads = Uploads#{FileGuid => {UserId, ?NOW() + ?INACTIVITY_PERIOD}}
    })};
handle_call({is_registered, UserId, FileGuid}, _, State) ->
    IsRegistered = case maps:find(FileGuid, State#state.uploads) of
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
    {noreply, NewState :: state(), timeout() | hibernate} |
    {stop, Reason :: term(), NewState :: state()}.
handle_cast({deregister, UserId, FileGuid}, #state{uploads = Uploads} = State) ->
    case maps:take(FileGuid, Uploads) of
        {{UserId, _}, ActiveUploads} ->
            NewState = State#state{uploads = ActiveUploads},
            case maps:size(ActiveUploads) of
                0 ->
                    {noreply, cancel_uploads_checkup(NewState), hibernate};
                _ ->
                    {noreply, NewState}
            end;
        _ ->
            {noreply, State}
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
-spec handle_info(Info :: timeout() | term(), state()) ->
    {noreply, NewState :: state()} |
    {noreply, NewState :: state(), timeout() | hibernate} |
    {stop, Reason :: term(), NewState :: state()}.
handle_info(check_uploads, #state{uploads = Uploads} = State) ->
    NewState = State#state{
        uploads = ActiveUploads = remove_stale_uploads(Uploads),
        checkup_timer = undefined
    },
    case maps:size(ActiveUploads) of
        0 ->
            {noreply, NewState, hibernate};
        _ ->
            {noreply, maybe_schedule_uploads_checkup(NewState)}
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
%% ?INACTIVITY_PERIOD deletes file because of interrupted and
%% unfinished upload.
%% @end
%%--------------------------------------------------------------------
-spec remove_stale_uploads(uploads()) -> uploads().
remove_stale_uploads(Uploads) ->
    Now = ?NOW(),
    maps:fold(fun
        (FileGuid, {UserId, CheckupTime}, Acc) when CheckupTime < Now ->
            case lfm:stat(?ROOT_SESS_ID, {guid, FileGuid}) of
                {ok, #file_attr{mtime = MTime}} when MTime + ?INACTIVITY_PERIOD > Now ->
                    Acc#{FileGuid => {UserId, MTime + ?INACTIVITY_PERIOD}};
                {ok, _} ->
                    lfm:unlink(?ROOT_SESS_ID, {guid, FileGuid}, false),
                    Acc;
                {error, _} ->
                    Acc
            end;
        (FileGuid, FileCheckup, Acc) ->
            Acc#{FileGuid => FileCheckup}
    end, #{}, Uploads).


%% @private
-spec call(term()) -> ok | boolean() | error().
call(Msg) ->
    try
        gen_server2:call(?MODULE, Msg, ?DEFAULT_REQUEST_TIMEOUT)
    catch
        exit:{noproc, _} ->
            ?debug("File upload manager process does not exist"),
            {error, no_file_upload_manager};
        exit:{normal, _} ->
            ?debug("Exit of file upload manager process"),
            {error, no_file_upload_manager};
        exit:{timeout, _} ->
            ?debug("Timeout of file upload manager process"),
            ?ERROR_TIMEOUT;
        Type:Reason ->
            ?error("Cannot call file upload manager due to ~p:~p", [
                Type, Reason
            ]),
            {error, Reason}
    end.


%% @private
-spec maybe_schedule_uploads_checkup(state()) -> state().
maybe_schedule_uploads_checkup(#state{checkup_timer = undefined} = State) ->
    State#state{checkup_timer = erlang:send_after(
        ?UPLOADS_CHECKUP_INTERVAL, self(), check_uploads
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
