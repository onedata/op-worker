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
% fixme name
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

% fixme
-export([debug/0]).
-export([xd/2]).

-define(SERVER, ?MODULE).

-define(START(SpaceId, StorageId), {start, SpaceId, StorageId}).
-define(EXECUTE_STAGE(SpaceId, StorageId), {handle_stage, SpaceId, StorageId}).
-define(NEXT_STAGE(SpaceId, StorageId), {next_stage, SpaceId, StorageId}).

% fixme
-record(state, {
    current_stage :: space_unsupport:stage()
    % fixme is canceling
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
    % fixme na ilu nodach wystartować?
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
    {noreply, State#{{SpaceId, StorageId} => #state{current_stage = init}}};

handle_cast(?EXECUTE_STAGE(SpaceId, StorageId), State) ->
    #state{current_stage = CurrentStage} = maps:get({SpaceId, StorageId}, State),
    ?notice("execute stage: ~p", [CurrentStage]),
    execute_stage(CurrentStage, SpaceId, StorageId),
    {noreply, State};

handle_cast(?NEXT_STAGE(SpaceId, StorageId), State) ->
    #state{current_stage = CurrentStage} = maps:get({SpaceId, StorageId}, State),
    ?notice("stage finished: ~p", [CurrentStage]),
    case get_next_stage(CurrentStage) of
        finished ->
            {noreply, maps:remove({SpaceId, StorageId}, State)};
        NextStage ->
            gen_server2:cast(?SERVER, ?EXECUTE_STAGE(SpaceId, StorageId)),
            {noreply, State#{{SpaceId, StorageId} => #state{current_stage = NextStage}}}
    end.
    
handle_info(_Info, State) ->
    % fixme log
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.

%%%===================================================================
%%% Internal functions
%%%===================================================================

get_next_stage(init) -> qos;
get_next_stage(qos) -> clear_storage;
get_next_stage(clear_storage) -> wait_for_dbsync;
get_next_stage(wait_for_dbsync) -> clean_database;
get_next_stage(clean_database) -> finished.

execute_stage(init, SpaceId, StorageId) ->
    % fixme inform zone space unsupported
    % fixme start blockades
    % fixme stop all incoming transfers
    % fixme kill handles
    % fixme mozna zrownoleglić (+ qos)
    next_stage(SpaceId, StorageId);
execute_stage(qos, SpaceId, StorageId) ->
    SpaceGuid = fslogic_uuid:spaceid_to_space_dir_guid(SpaceId),
    % fixme if preferred provider replace AllStorages with provider Id (not really if other providers still allowed)
    Expression = <<?ALL_STORAGES/binary, " - storage_id = ", StorageId/binary>>,
    % fixme internal qos (pid to inform? (what if dead - callback))
    {ok, QosEntryId} = lfm:add_qos_entry(?ROOT_SESS_ID, {guid, SpaceGuid}, Expression, 1),
    ?notice("QosEntry: ~p", [QosEntryId]),
    % fixme use qos status sub
    spawn(fun() -> wait_for_qos(QosEntryId, SpaceId, StorageId) end),
    ok;
execute_stage(clear_storage, SpaceId, StorageId) ->
    % fixme remove file_location ()
    % fixme do not clean storage if import is on
    {ok, TaskId} = unsupport_traverse:start(SpaceId, StorageId),
    ?notice("TaskId: ~p", [TaskId]),
    % fixme delete space root dir
    ok;
execute_stage(wait_for_dbsync, SpaceId, StorageId) ->
    % fixme
    % fixme wait for all docs to be saved on disc
    timer:sleep(timer:seconds(15)),
    % fixme stop dbsync
    next_stage(SpaceId, StorageId);
execute_stage(clean_database, SpaceId, StorageId) ->
    % fixme do it first (inform zone, that space is being unsupported)
    storage_logic:revoke_space_support(StorageId, SpaceId),
    xd(SpaceId, StorageId),
    % fixme find which local documents are to be deleted and to it here (storage:space_unsupported?), space_quota
    % fixme file_handles, dbsync_state
    % fixme do not call storage (move it here?)
    storage:on_space_unsupported(SpaceId, StorageId),
    space_quota:delete(SpaceId),
    unsupport_traverse:delete_ended(SpaceId, StorageId).


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

% fixme
-define(EXPIRY, 3).

xd(SpaceId, StorageId) ->
    Until = dbsync_state:get_seq(SpaceId, oneprovider:get_id()),
    Callback = 
        fun ({ok, end_of_stream}) -> next_stage(SpaceId, StorageId);
            ({ok, Batch}) ->
                lists:foreach(fun(#document{value = Value} = Doc) ->
                    case Value of
                        #links_forest{model = Model, key = Key} ->
                            ?warning("links_forest"),
                            links_save(Model, Key, Doc);
                        #links_node{model = Model, key = Key} ->
                            ?warning("links_node"),
                            links_save(Model, Key, Doc);
                        _ ->
                            Model = element(1, Value),
                            ?critical("xd: ~p", [Value]),
                            Ctx = datastore_model_default:get_ctx(Model),
                            datastore_model:save(Ctx#{hooks_disabled => true, expiry => ?EXPIRY}, Doc)
                    end
                end, Batch);
            (Dupa) ->
                ?critical("~p", [Dupa])
        end,
    couchbase_changes_stream:start_link(
        couchbase_changes:design(), SpaceId, Callback,
        [{since, 0}, {until, Until}], [self()]
    ).

% fixme almost the same as in dbsync_changes
links_save(Model, RoutingKey, Doc = #document{key = Key}) ->
    Ctx = datastore_model_default:get_ctx(Model),
    Ctx2 = Ctx#{
        local_links_tree_id => oneprovider:get_id(),
        routing_key => RoutingKey
    },
    Ctx3 = datastore_multiplier:extend_name(RoutingKey, Ctx2),
    case datastore_router:route(save, [Ctx3#{expiry => ?EXPIRY}, Key, Doc]) of
        {ok, Doc2} ->
            Doc2;
        {error, ignored} ->
            undefined
    end.
