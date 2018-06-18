%%%-------------------------------------------------------------------
%%% @author Jakub Kudzia
%%% @copyright (C) 2018 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%%--------------------------------------------------------------------
%%% @doc
%%% gen_server started used replica_eviction_workers_pool
%%% This processes are used to evict file_replicas. 
%%% @end
%%%-------------------------------------------------------------------
-module(replica_eviction_worker).
-author("Jakub Kudzia").

-behaviour(gen_server).

-include("global_definitions.hrl").
-include("proto/oneprovider/provider_messages.hrl").
-include("modules/datastore/transfer.hrl").
-include_lib("ctool/include/logging.hrl").

%% API
-export([start_link/0]).

-export([cast/7]).

%% gen_server callbacks
-export([init/1,
    handle_call/3,
    handle_cast/2,
    handle_info/2,
    terminate/2,
    code_change/3]).

-define(SERVER, ?MODULE).
-define(EVICT_REPLICA, evict_replica).

-record(state, {}).

%%%===================================================================
%%% API
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Starts the server
%% @end
%%--------------------------------------------------------------------
-spec(start_link() ->
    {ok, Pid :: pid()} | ignore | {error, Reason :: term()}).
start_link() ->
    gen_server2:start_link({local, ?SERVER}, ?MODULE, [], []).

%%-------------------------------------------------------------------
%% @doc
%% Casts task to worker pool called ?REPLICA_EVICTION_WORKERS_POOL
%% @end
%%-------------------------------------------------------------------
-spec cast(file_meta:uuid(), od_space:id(), fslogic_blocks:blocks(),
    version_vector:version_vector(), replica_evicion:id(),replica_eviction:type(),
    replica_eviction:report_id()) -> ok.
cast(FileUuid, SpaceId, Blocks, VV, REId, Type, Id) ->
    Task = {?EVICT_REPLICA, FileUuid, SpaceId, Blocks, VV, REId, Type, Id},
    worker_pool:cast(?REPLICA_EVICTION_WORKERS_POOL, Task).

%%%===================================================================
%%% gen_server callbacks
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Initializes the server
%% @doc
%%--------------------------------------------------------------------
-spec(init(Args :: term()) ->
    {ok, State :: #state{}} | {ok, State :: #state{}, timeout() | hibernate} |
    {stop, Reason :: term()} | ignore).
init([]) ->
    {ok, #state{}}.

%%--------------------------------------------------------------------
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
%% @doc
%% Handling cast messages
%% @end
%%--------------------------------------------------------------------
-spec(handle_cast(Request :: term(), State :: #state{}) ->
    {noreply, NewState :: #state{}} |
    {noreply, NewState :: #state{}, timeout() | hibernate} |
    {stop, Reason :: term(), NewState :: #state{}}).
handle_cast({?EVICT_REPLICA, FileUuid, SpaceId, Blocks, VV, REId, Type, Id}, State) ->
    FileGuid = fslogic_uuid:uuid_to_guid(FileUuid, SpaceId),
    FileCtx = file_ctx:new_by_guid(FileGuid),
    Result = case replica_eviction_lock:acquire_write_lock(FileUuid) of
        ok ->
            FileGuid = fslogic_uuid:uuid_to_guid(FileUuid, SpaceId),
            FileCtx = file_ctx:new_by_guid(FileGuid),
            EvictionResult = case replica_eviction_req:evict_blocks(FileCtx, Blocks, VV) of
                ok ->
                    {ok, fslogic_blocks:size(Blocks)};
                Error ->
                    Error
            end,
            replica_eviction_lock:release_write_lock(FileUuid),
            EvictionResult;
        Error ->
            Error
    end,
    replica_eviction:release_supporting_lock(REId),
    replica_evictor:process_result(Type, FileUuid, Result, Id),
    {noreply, State};
handle_cast(Request, State) ->
    ?log_bad_request(Request),
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
%%
%% @spec code_change(OldVsn, State, Extra) -> {ok, NewState}
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
