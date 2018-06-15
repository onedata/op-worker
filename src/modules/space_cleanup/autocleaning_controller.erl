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
-export([maybe_start/2, stop/1, process_eviction_result/3]).

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
    space_id :: od_space:id(),
    autocleaning_config :: autocleaning_config:config()
}).

-define(START_CLEANUP, start_autocleaning).
-define(STOP_CLEAN, finish_autocleaning).
-define(CHECK_QUOTA, check_quota).


-define(RESPONSE_ARRIVED, response_arrived).
-define(CHECK_QUOTA_INTERVAL, 20).

-define(NAME(SpaceId), {globa, {?SERVER, SpaceId}}).

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
            autocleaning:delete(AutocleaningId, SpaceId)
    end.

%%-------------------------------------------------------------------
%% @doc
%% Stops autocleaning_controller process and marks transfer as completed.
%% @end
%%-------------------------------------------------------------------
-spec stop(od_space:id()) -> ok.
stop(SpaceId) ->
    gen_server2:cast({global, {?SERVER, SpaceId}}, ?STOP_CLEAN).

%%-------------------------------------------------------------------
%% @doc
%% Posthook executed by replica_eviction_worker after evicting file.
%% @end
%%-------------------------------------------------------------------
-spec process_eviction_result(replica_eviction:result(), file_meta:uuid(),
    autocleaning:id()) -> ok.
process_eviction_result({ok, ReleasedBytes}, FileUuid, AutocleaningId) ->
    ?debug("Autocleaning of file ~p in procedure ~p released ~p bytes.",
        [FileUuid, AutocleaningId, ReleasedBytes]),
    {ok, _} = autocleaning:mark_released_file(AutocleaningId, ReleasedBytes),
    ok;
process_eviction_result(Error, FileUuid, AutocleaningId) ->
    ?error("Error ~p occured during autocleanig of file ~p in procedure ~p",
        [Error, FileUuid, AutocleaningId]),
    {ok, _} = autocleaning:mark_processed_file(AutocleaningId),
    ok.

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
    {ok, #state{
        autocleaning_id = AutocleaningId,
        space_id = SpaceId,
        autocleaning_config = Config
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
    max_file_not_opened_hours = MaxNotOpenedHours
}}, State = #state{
    autocleaning_id = AutocleaningId,
    space_id = SpaceId
}) ->
    try
        FilesToClean = file_popularity_view:get_unpopular_files(
            SpaceId, LowerFileLimit, UpperFileLimit, MaxNotOpenedHours, null,
            null, null, null
        ),
        case FilesToClean of
            [] ->
                stop(SpaceId);
            _ ->
                evict_files(FilesToClean, AutocleaningId, SpaceId)
        end,
        {noreply, State}
    catch
        _:Reason ->
            autocleaning:mark_failed(AutocleaningId),
            ?error_stacktrace("Could not autoclean space ~p due to ~p", [SpaceId, Reason]),
            {stop, Reason, State}
    end;
handle_cast(?CHECK_QUOTA, State = #state{
    space_id = SpaceId,
    autocleaning_id = AutocleaningId,
    autocleaning_config = #autocleaning_config{target = Target}
}) ->
    case should_stop(SpaceId, Target) of
        true ->
            replica_evictor:cancel(AutocleaningId, SpaceId);
        false ->
            ok
    end,
    {noreply, State};
handle_cast(?STOP_CLEAN, State = #state{
    autocleaning_id = AutocleaningId,
    space_id = SpaceId
}) ->
    autocleaning:mark_completed(AutocleaningId),
    replica_evictor:cancelling_finished(AutocleaningId, SpaceId),
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
%% Adds all FilesToClean to replica_evictor queue.
%% @end
%%-------------------------------------------------------------------
-spec evict_files([file_ctx:ctx()], autocleaning:id(), od_space:id()) -> ok.
evict_files(FilesToClean, AutocleaningId, SpaceId) ->

    EvictionSettingsList = lists:filtermap(fun(FileCtx) ->
        case replica_evictor:get_setting_for_eviction_task(FileCtx) of
            undefined ->
                false;
            EvictionSettings ->
                {true, EvictionSettings}
        end
    end, FilesToClean),

    FilesToCleanNum = length(EvictionSettingsList),
    {ok, _} = autocleaning:mark_active(AutocleaningId, FilesToCleanNum),

    lists:foreach(fun({N, {FileUuid, Provider, Blocks, VV}}) ->
        case N rem ?CHECK_QUOTA_INTERVAL of
            0 ->
                % every ?CHECK_QUOTA_INTERVAL task, add notification task
                % it will trigger checking guota
                Self = self(),
                replica_evictor:notify(fun() -> check_quota(Self) end, AutocleaningId, SpaceId),
                schedule_eviction_task(FileUuid, Provider, Blocks, VV, AutocleaningId, SpaceId);
            _ ->
                schedule_eviction_task(FileUuid, Provider, Blocks, VV, AutocleaningId, SpaceId)
        end
    end, lists:zip(lists:seq(1, FilesToCleanNum), EvictionSettingsList)).


-spec schedule_eviction_task(file_meta:uuid(), od_provider:id(), fslogic_blocks:blocks(),
    version_vector:version_vector(), autocleaning:id(), od_space:id()) -> ok.
schedule_eviction_task(FileUuid, Provider, Blocks, VV, AutoCleaningId, SpaceId) ->
    replica_evictor:evict(FileUuid, Provider, Blocks, VV, AutoCleaningId, autocleaning, SpaceId).

%%-------------------------------------------------------------------
%% @private
%% @doc
%% Casts task for checking quota
%% @end
%%-------------------------------------------------------------------
-spec check_quota(pid()) -> ok.
check_quota(Pid) ->
    gen_server2:cast(Pid, ?CHECK_QUOTA).

%%-------------------------------------------------------------------
%% @private
%% @doc
%% This function checks whether current storage occupancy has been
%% lowered beneath configure Target
%% @end
%%-------------------------------------------------------------------
-spec should_stop(od_space:id(), non_neg_integer()) -> boolean().
should_stop(SpaceId, Target) ->
    space_quota:current_size(SpaceId) =< Target.

%%-------------------------------------------------------------------
%% @private
%% @doc
%% Starts process responsible for performing autocleaning
%% @end
%%-------------------------------------------------------------------
-spec start(autocleaning:id(), autocleaning:autocleaning()) -> ok.
start(AutocleaningId, AC = #autocleaning{space_id = SpaceId}) ->
    Config = #autocleaning_config{} = autocleaning:get_config(AC),
    {ok, _Pid} = gen_server2:start({global, {?SERVER, SpaceId}}, ?MODULE,
        [AutocleaningId, SpaceId, Config], []),
    ok.
