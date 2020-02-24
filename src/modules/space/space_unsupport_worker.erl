%%%--------------------------------------------------------------------
%%% @author Michal Stanisz
%%% @copyright (C) 2020 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%--------------------------------------------------------------------
%%% @doc
%%% fixme
%%% @end
%%%--------------------------------------------------------------------
-module(space_unsupport_worker).
-author("Michal Stanisz").

-behaviour(gen_server).

-include("modules/datastore/qos.hrl").
-include("modules/fslogic/fslogic_common.hrl").
-include_lib("ctool/include/logging.hrl").

-export([start_link/0]).
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2]).
-export([start/2, cancel/2]).
-export([next_stage/2]).

-export([debug/0]).

-define(SERVER, ?MODULE).

-define(START(SpaceId, StorageId), {start, SpaceId, StorageId}).
-define(EXECUTE_STAGE(SpaceId, StorageId), {handle_stage, SpaceId, StorageId}).
-define(NEXT_STAGE(SpaceId, StorageId), {next_stage, SpaceId, StorageId}).

% fixme
-record(state_int, {
    current_stage :: space_unsupport:stage()
}).

% API

start(SpaceId, StorageId) ->
    gen_server2:cast(?SERVER, ?START(SpaceId, StorageId)).

cancel(_SpaceId, _StorageId) -> ok.

next_stage(SpaceId, StorageId) ->
    gen_server2:cast(?SERVER, ?NEXT_STAGE(SpaceId, StorageId)).

debug() ->
    gen_server2:cast(?SERVER, debug).

%%%===================================================================
%%% Spawning and gen_server implementation
%%%===================================================================

start_link() ->
    gen_server:start_link({local, ?SERVER}, ?MODULE, [], []).

init([]) ->
    {ok, #{}}.

handle_call(_, _, State) -> 
    % fixme log
    {reply, ok, State}.


handle_cast(debug, State) ->
    ?critical("~p", [State]),
    {noreply, State};
    
handle_cast(?START(SpaceId, StorageId), State) ->
    gen_server2:cast(?SERVER, ?EXECUTE_STAGE(SpaceId, StorageId)),   
    {noreply, State#{{SpaceId, StorageId} => #state_int{current_stage = qos}}};

handle_cast(?EXECUTE_STAGE(SpaceId, StorageId), State) ->
    #state_int{current_stage = CurrentStage} = maps:get({SpaceId, StorageId}, State),
    ?notice("execute stage: ~p", [CurrentStage]),
    execute_stage(CurrentStage, SpaceId, StorageId),
    {noreply, State};

handle_cast(?NEXT_STAGE(SpaceId, StorageId), State) ->
    #state_int{current_stage = CurrentStage} = maps:get({SpaceId, StorageId}, State),
    ?notice("next stage: ~p", [CurrentStage]),
    case get_next_stage(CurrentStage) of
        finished ->
            {noreply, maps:remove({SpaceId, StorageId}, State)};
        NextStage ->
            gen_server2:cast(?SERVER, ?EXECUTE_STAGE(SpaceId, StorageId)),
            {noreply, State#{{SpaceId, StorageId} => #state_int{current_stage = NextStage}}}
    end.
    
handle_info(_Info, State) ->
    % fixme log
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.

%%%===================================================================
%%% Internal functions
%%%===================================================================

get_next_stage(qos) -> clear_storage;
get_next_stage(clear_storage) -> wait_for_dbsync;
get_next_stage(wait_for_dbsync) -> clean_docs;
get_next_stage(clean_docs) -> finished.

execute_stage(qos, SpaceId, StorageId) ->
    SpaceGuid = fslogic_uuid:spaceid_to_space_dir_guid(SpaceId),
    % fixme if preferred provider replace AllStorages with provider Id (not really if other providers still allowed)
    Expression = <<?ALL_STORAGES/binary, " - storage_id = ", StorageId/binary>>,
    {ok, QosEntryId} = lfm:add_qos_entry(?ROOT_SESS_ID, {guid, SpaceGuid}, Expression, 1),
    ?notice("QosEntry: ~p", [QosEntryId]),
    % fixme use qos status sub
    spawn(fun() -> wait_for_qos(QosEntryId, SpaceId, StorageId) end),
    ok;
execute_stage(clear_storage, SpaceId, StorageId) ->
    % fixme remove file_location ()
    {ok, TaskId} = unsupport_traverse:start(SpaceId, StorageId),
    ?notice("TaskId: ~p", [TaskId]),
    % fixme delete space root dir
    ok;
execute_stage(wait_for_dbsync, SpaceId, StorageId) ->
    % fixme
%%    timer:sleep(timer:minutes(1)),
    next_stage(SpaceId, StorageId);
execute_stage(clean_docs, SpaceId, StorageId) ->
    % fixme remove all synced using changes stream
    % fixme do it first (inform zone, that space is being unsupported)
    storage_logic:revoke_space_support(StorageId, SpaceId),
    % fixme find which local documents are to be deleted and to it here (storage:space_unsupported?)
    storage:on_space_unsupported(SpaceId, StorageId),
    next_stage(SpaceId, StorageId).


wait_for_qos(QosEntryId, SpaceId, StorageId) ->
    case lfm:check_qos_fulfilled(?ROOT_SESS_ID, QosEntryId) of
        {ok, true} ->
            lfm:remove_qos_entry(?ROOT_SESS_ID, QosEntryId),
            next_stage(SpaceId, StorageId);
        {ok, false} ->
            timer:sleep(timer:seconds(1)),
            wait_for_qos(QosEntryId, SpaceId, StorageId);
        {error, _} = Error ->
            ?error("wait for qos error: ~p", [Error]),
            timer:sleep(timer:seconds(1)),
            wait_for_qos(QosEntryId, SpaceId, StorageId)
    end.

