%%%-------------------------------------------------------------------
%%% @author Bartosz Walkowicz
%%% @copyright (C) 2019 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% Monitors activity of file uploads. If no activity (writes) happens
%%% for longer than allowed ?INACTIVITY _PERIOD it is assumed that GUI
%%% lost connection to backend and no more file chunks will be uploaded.
%%% Such damaged files will be deleted.
%%% @end
%%%-------------------------------------------------------------------
-module(file_upload_manager).
-author("Bartosz Walkowicz").

-behaviour(gen_server).

-include_lib("ctool/include/logging.hrl").
-include_lib("ctool/include/posix/file_attr.hrl").

%% API
-export([start_link/0, spec/0]).
-export([register_upload/2, deregister_upload/2]).

%% gen_server callbacks
-export([
    init/1,
    handle_call/3, handle_cast/2, handle_info/2,
    terminate/2, code_change/3
]).

-record(state, {
    uploads = #{} :: #{file_id:file_guid() => pid()},
    checkup_timer = undefined :: undefined | reference()
}).

-type state() :: #state{}.
-type error() :: {error, Reason :: term()}.

-define(NOW, time_utils:system_time_millis()).
-define(INACTIVITY_PERIOD, 60000).  % timer:minutes(1)


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
-spec register_upload(session:id(), file_id:file_guid()) -> ok.
register_upload(SessionId, FileGuid) ->
    gen_server2:cast(?MODULE, {register, SessionId, FileGuid}).


%%--------------------------------------------------------------------
%% @doc
%% Deregisters upload monitoring for specified file.
%% @end
%%--------------------------------------------------------------------
-spec deregister_upload(session:id(), file_id:file_guid()) -> ok.
deregister_upload(SessionId, FileGuid) ->
    gen_server2:cast(?MODULE, {deregister, SessionId, FileGuid}).


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
handle_cast({register, SessionId, FileGuid}, #state{uploads = Uploads} = State) ->
    {noreply, maybe_schedule_uploads_checkup(State#state{
        uploads = Uploads#{{SessionId, FileGuid} => ?NOW + ?INACTIVITY_PERIOD}
    })};
handle_cast({deregister, SessionId, FileGuid}, #state{uploads = Uploads} = State) ->
    ActiveUploads = maps:remove({SessionId, FileGuid}, Uploads),
    NewState = State#state{uploads = ActiveUploads},
    case maps:size(ActiveUploads) of
        0 ->
            {noreply, cancel_uploads_checkup(NewState), hibernate};
        _ ->
            {noreply, NewState}
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
remove_stale_uploads(Uploads) ->
    Now = ?NOW,
    maps:fold(fun
        ({SessionId, FileGuid}, FileCheckup, Acc) when FileCheckup < Now ->
            case lfm:stat(SessionId, {guid, FileGuid}) of
                {ok, #file_attr{mtime = MTime}} when MTime + ?INACTIVITY_PERIOD > Now ->
                    Acc#{{SessionId, FileGuid} => MTime + ?INACTIVITY_PERIOD};
                {ok, _} ->
                    lfm:unlink(SessionId, {guid, FileGuid}, false),
                    Acc;
                {error, _} ->
                    Acc
            end;
        (FileGuid, FileCheckup, Acc) ->
            Acc#{FileGuid => FileCheckup}
    end, #{}, Uploads).


%% @private
-spec maybe_schedule_uploads_checkup(state()) -> state().
maybe_schedule_uploads_checkup(#state{checkup_timer = undefined} = State) ->
    State#state{checkup_timer = erlang:send_after(
        ?INACTIVITY_PERIOD, self(), check_uploads
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
