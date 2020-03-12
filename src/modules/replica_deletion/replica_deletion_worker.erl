%%%-------------------------------------------------------------------
%%% @author Jakub Kudzia
%%% @copyright (C) 2018 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%%--------------------------------------------------------------------
%%% @doc
%%% gen_server started as a worker of replica_deletion_workers_pool
%%% This processes are used to delete file_replicas from storage after
%%% request for deleting file replica has been granted by a remote
%%% provider.
%%% @end
%%%-------------------------------------------------------------------
-module(replica_deletion_worker).
-author("Jakub Kudzia").

-behaviour(gen_server).

-include("global_definitions.hrl").
-include("proto/oneprovider/provider_messages.hrl").
-include("modules/datastore/transfer.hrl").
-include("modules/replica_deletion/replica_deletion.hrl").
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

% exported for tests
-export([custom_predicate/3]).

-define(SERVER, ?MODULE).
-define(DELETE_REPLICA(FileUuid, SpaceId, Blocks, VV, RDId, JobType, JobId),
    {delete_replica, FileUuid, SpaceId, Blocks, VV, RDId, JobType, JobId}).

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
%% Casts task to worker pool called ?REPLICA_DELETION_WORKERS_POOL
%% @end
%%-------------------------------------------------------------------
-spec cast(file_meta:uuid(), od_space:id(), fslogic_blocks:blocks(),
    version_vector:version_vector(), replica_deletion:id(), replica_deletion:job_type(),
    replica_deletion:job_id()) -> ok.
cast(FileUuid, SpaceId, Blocks, VV, RDId, JobType, JobId) ->
    worker_pool:cast(?REPLICA_DELETION_WORKERS_POOL,
        ?DELETE_REPLICA(FileUuid, SpaceId, Blocks, VV, RDId, JobType, JobId)).

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
handle_cast(?DELETE_REPLICA(FileUuid, SpaceId, Blocks, VV, RDId, JobType, JobId), State) ->
    case replica_deletion_worker:custom_predicate(SpaceId, JobType, JobId) of
        true ->
            delete_if_not_opened(FileUuid, SpaceId, Blocks, VV, RDId, JobType, JobId);
        false ->
            replica_deletion_master:process_result(SpaceId, FileUuid, {error, precondition_not_satisfied}, JobId, JobType)
    end,
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

%%-------------------------------------------------------------------
%% @doc
%% This function allows each replica_deletion:job_type() to define
%% its custom, additional precondition that must be satisfied
%% to delete the file replica.
%% @end
%%-------------------------------------------------------------------
-spec custom_predicate(od_space:id(), replica_deletion:job_type(), replica_deletion:job_id()) -> true | false.
custom_predicate(SpaceId, JobType, JobId) ->
    Module = replica_deletion_master:job_type_to_module(JobType),
    case erlang:function_exported(Module, replica_deletion_predicate, 2) of
        true -> Module:replica_deletion_predicate(SpaceId, JobId);
        false -> true
    end.

-spec delete_if_not_opened(file_meta:uuid(), od_space:id(), [fslogic_blocks:blocks()], version_vector:version_vector(),
    replica_deletion:id(), replica_deletion:job_type(), replica_deletion:job_id()) -> any().
delete_if_not_opened(FileUuid, SpaceId, Blocks, VV, RDId, JobType, JobId) ->
    FileGuid = file_id:pack_guid(FileUuid, SpaceId),
    FileCtx = file_ctx:new_by_guid(FileGuid),
    ?alert("DUPA1"),
    case file_handles:exists(FileUuid) of
        false ->
            ?alert("DUPA2"),
            % file is not opened, we can delete it
            Result = case replica_deletion_lock:acquire_write_lock(FileUuid) of
                ok ->
                    ?alert("DUPA3"),
                    DeletionResult = case replica_deletion_req:delete_blocks(FileCtx, Blocks, VV) of
                        ok ->
                            ?alert("DUPA4"),
                            {ok, fslogic_blocks:size(Blocks)};
                        Error ->
                            ?alert("DUPA5"),
                            Error
                    end,
                    ?alert("DUPA6"),
                    replica_deletion_lock:release_write_lock(FileUuid),
                    ?alert("DUPA7"),
                    DeletionResult;
                Error ->
                    ?alert("DUPA8"),
                    Error
            end,
            ?alert("DUPA9"),
            replica_deletion:release_supporting_lock(RDId),
            ?alert("DUPA10"),
            replica_deletion_master:process_result(SpaceId, FileUuid, Result, JobId, JobType);
        true ->
            ?alert("DUPA11"),
            replica_deletion_master:process_result(SpaceId, FileUuid, {error, file_opened}, JobId, JobType)
    end.