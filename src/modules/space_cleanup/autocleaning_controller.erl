%%%-------------------------------------------------------------------
%%% @author Jakub Kudzia
%%% @copyright (C) 2017 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%%--------------------------------------------------------------------
%%% @doc
%%% This module is responsible for performing space cleanup with given
%%% configuration. It deletes files returned from file_popularity_view,
%%% with given constraints, until configured target level of storage
%%% occupancy is reached.
%%% @end
%%%-------------------------------------------------------------------
-module(autocleaning_controller).
-author("Jakub Kudzia").

-behaviour(gen_server).

-include("modules/datastore/datastore_models.hrl").
-include("proto/oneprovider/provider_messages.hrl").
-include_lib("ctool/include/logging.hrl").

%% API
-export([start_link/0, maybe_start/2, stop/0]).

%% gen_server callbacks
-export([init/1,
    handle_call/3,
    handle_cast/2,
    handle_info/2,
    terminate/2,
    code_change/3]).

-define(SERVER, ?MODULE).

-record(state, {
    autocleaning_id :: autocleaning:id(),
    space_id :: od_space:id()
}).

-define(START_CLEANUP, start_autocleaning).
-define(STOP_CLEAN, finish_autocleaning).

%%%===================================================================
%%% API
%%%===================================================================

%%-------------------------------------------------------------------
%% @doc
%% This function starts autocleaning if given AutocleaningId is set
%% in space_storage model as being in progress.
%% @end
%%-------------------------------------------------------------------
-spec maybe_start(autocleaning:id(), autocleaning:autocleaning()) -> ok.
maybe_start(AutocleaningId, AC = #autocleaning{space_id = SpaceId}) ->
    case space_storage:get_cleanup_in_progress(SpaceId) of
        AutocleaningId ->
            start(AutocleaningId, AC);
        _ ->
            autocleaning:remove_skipped(AutocleaningId, SpaceId)
    end.

%%--------------------------------------------------------------------
%% @doc
%% Starts the server
%% @end
%%--------------------------------------------------------------------
-spec(start_link() ->
    {ok, Pid :: pid()} | ignore | {error, Reason :: term()}).
start_link() ->
    gen_server:start_link({local, ?SERVER}, ?MODULE, [], []).


%%-------------------------------------------------------------------
%% @doc
%% Stops transfer_controller process and marks transfer as completed.
%% @end
%%-------------------------------------------------------------------
-spec stop() -> ok.
stop() ->
    gen_server2:cast(self(), ?STOP_CLEAN).

%%%===================================================================
%%% gen_server callbacks
%%%===================================================================

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Initializes the server
%%--------------------------------------------------------------------
-spec(init(Args :: term()) ->
    {ok, State :: #state{}} | {ok, State :: #state{}, timeout() | hibernate} |
    {stop, Reason :: term()} | ignore).
init([AutocleaningId, SpaceId, Config]) ->
    ok = gen_server2:cast(self(), {?START_CLEANUP, Config}),
    ?info("Autocleaning: ~p started in space ~p", [AutocleaningId, SpaceId]),
    {ok, _} = autocleaning:mark_active(AutocleaningId),
    {ok, #state{
        autocleaning_id = AutocleaningId,
        space_id = SpaceId
    }}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Handling call messages
%% @end
%%--------------------------------------------------------------------
-spec(handle_call(Request :: term(), From :: {pid(), Tag :: term()},
    State :: #state{}) ->
    {reply, Reply :: term(), NewState :: #state{}} |
    {reply, Reply :: term(), NewState :: #state{}, timeout() | hibernate} |
    {noreply, NewState :: #state{}} |
    {noreply, NewState :: #state{}, timeout() | hibernate} |
    {stop, Reason :: term(), Reply :: term(), NewState :: #state{}} |
    {stop, Reason :: term(), NewState :: #state{}}).
handle_call(_Request, _From, State) ->
    ?log_bad_request(_Request),
    {reply, wrong_request, State}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Handling cast messages
%% @end
%%--------------------------------------------------------------------
-spec(handle_cast(Request :: term(), State :: #state{}) ->
    {noreply, NewState :: #state{}} |
    {noreply, NewState :: #state{}, timeout() | hibernate} |
    {stop, Reason :: term(), NewState :: #state{}}).
handle_cast({?START_CLEANUP, #autocleaning_config{
    lower_file_size_limit = LowerFileLimit,
    upper_file_size_limit = UpperFileLimit,
    max_file_not_opened_hours = MaxNotOpenedHours,
    target = Target
}}, State = #state{
    autocleaning_id = AutocleaningId,
    space_id = SpaceId
}) ->
    try
        cleanup_space(SpaceId, AutocleaningId, LowerFileLimit, UpperFileLimit,
            MaxNotOpenedHours, Target),
        {noreply, State}
    catch
        _:Reason ->
            autocleaning:mark_failed(AutocleaningId),
            ?error_stacktrace("Could not autoclean space ~p due to ~p", [SpaceId, Reason]),
            {stop, Reason, State}
    end;
handle_cast(?STOP_CLEAN, State = #state{
    autocleaning_id = AutocleaningId,
    space_id = SpaceId
}) ->
    autocleaning:mark_completed(AutocleaningId),
    ?info("Autocleaning ~p of space ~p finished", [AutocleaningId, SpaceId]),
    {stop, normal, State};
handle_cast(_Request, State) ->
    ?log_bad_request(_Request),
    {noreply, State}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Handling all non call/cast messages
%% @end
%%--------------------------------------------------------------------
-spec(handle_info(Info :: timeout() | term(), State :: #state{}) ->
    {noreply, NewState :: #state{}} |
    {noreply, NewState :: #state{}, timeout() | hibernate} |
    {stop, Reason :: term(), NewState :: #state{}}).
handle_info(_Info, State) ->
    ?log_bad_request(_Info),
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
-spec(terminate(Reason :: (normal | shutdown | {shutdown, term()} | term()),
    State :: #state{}) -> term()).
terminate(_Reason, _State) ->
    ok.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Convert process state when code is changed
%% @end
%%--------------------------------------------------------------------
-spec(code_change(OldVsn :: term() | {down, term()}, State :: #state{},
    Extra :: term()) ->
    {ok, NewState :: #state{}} | {error, Reason :: term()}).
code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%%%===================================================================
%%% Internal functions
%%%===================================================================

%%-------------------------------------------------------------------
%% @private
%% @doc
%% Starts process responsible for performing autocleaning
%% @end
%%-------------------------------------------------------------------
-spec start(autocleaning:id(), autocleaning:autocleaning()) -> ok.
start(AutocleaningId, AC = #autocleaning{space_id = SpaceId}) ->
    Config = #autocleaning_config{} = autocleaning:get_config(AC),
    {ok, _Pid} = gen_server2:start(autocleaning_controller, [AutocleaningId,
        SpaceId, Config], []),
    ok.

%%--------------------------------------------------------------------
%% @doc
%% Cleanups unpopular files from space
%% @end
%%--------------------------------------------------------------------
-spec cleanup_space(od_space:id(), autocleaning:id(), non_neg_integer(),
    non_neg_integer(), non_neg_integer(), non_neg_integer()) -> ok.
cleanup_space(SpaceId, AutocleaningId, SizeLowerLimit, SizeUpperLimit, MaxNotOpenedHours, Target) ->
    FilesToClean = file_popularity_view:get_unpopular_files(
        SpaceId, SizeLowerLimit, SizeUpperLimit, MaxNotOpenedHours, null,
        null, null, null
    ),
    ConditionFun = fun() ->
        CurrentSize = space_quota:current_size(SpaceId),
        CurrentSize =< Target
    end,
    ForeachFun = fun(FileCtx) -> cleanup_replica(FileCtx, AutocleaningId) end,
    foreach_until(ForeachFun, ConditionFun, FilesToClean),
    stop().

%%-------------------------------------------------------------------
%% @private
%% @doc
%% Executes Fun for each element of the List until Condition is satisfied.
%% @end
%%-------------------------------------------------------------------
-spec foreach_until(fun((FileCtx :: file_ctx:ctx()) -> ok),
    fun(() -> boolean()), [file_ctx:ctx()]) -> ok.
foreach_until(Fun, Condition, List) ->
    case Condition() of
        true -> ok;
        false ->
            lists:foldl(fun
                (_Arg, true) ->
                    true;
                (Arg, false) ->
                    Fun(Arg),
                    Condition()
            end, false, List)
    end,
    ok.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Invalidates local replica of given file
%% @end
%%--------------------------------------------------------------------
-spec cleanup_replica(file_ctx:ctx(), autocleaning:id()) -> ok.
cleanup_replica(FileCtx, AutocleaningId) ->
    RootUserCtx = user_ctx:new(session:root_session_id()),
    try
        #provider_response{status = #status{code = ?OK}} =
            sync_req:invalidate_file_replica(RootUserCtx, FileCtx,
                undefined, undefined, AutocleaningId),
        ok
    catch
        _Error:Reason ->
            ?error_stacktrace("Error of autocleaning procedure ~p due to ~p",
                [AutocleaningId, Reason]
            )
    end.