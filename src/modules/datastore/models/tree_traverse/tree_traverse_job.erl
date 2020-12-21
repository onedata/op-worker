%%%-------------------------------------------------------------------
%%% @author Michal Wrzeszcz
%%% @copyright (C) 2019 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc Model for holding traverse jobs (see tree_traverse.erl). Main jobs for each task
%%% are synchronized between providers, other are local for provider executing task.
%%% @end
%%%-------------------------------------------------------------------
-module(tree_traverse_job).
-author("Michal Wrzeszcz").

-include("tree_traverse.hrl").
-include("modules/fslogic/fslogic_common.hrl").
-include("modules/datastore/datastore_models.hrl").
-include("modules/datastore/datastore_runner.hrl").

%% API
-export([save_master_job/5, delete_master_job/4, get_master_job/1]).

%% datastore_model callbacks
-export([get_ctx/0, get_record_struct/1, upgrade_record/2, get_record_version/0]).

-type key() :: datastore:key().
-type record() :: #tree_traverse_job{}.
-type doc() :: datastore_doc:doc(record()).

-export_type([doc/0]).

-define(CTX, #{
    model => ?MODULE
}).
-define(SYNC_CTX, #{
    model => ?MODULE,
    sync_enabled => true,
    mutator => oneprovider:get_id_or_undefined()
}).

-define(MAIN_JOB_PREFIX, "main_job").

%%%===================================================================
%%% API
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Saves information about job. See save/3 for more information.
%% @end
%%--------------------------------------------------------------------
-spec save_master_job(datastore:key() | main_job, tree_traverse:master_job(), traverse:pool(), traverse:id(),
    traverse:callback_module()) -> {ok, key()} | {error, term()}.
save_master_job(Key, Job = #tree_traverse{
    file_ctx = FileCtx,
    user_ctx = UserCtx,
    token = Token,
    last_name = LastName,
    last_tree = LastTree,
    execute_slave_on_dir = OnDir,
    children_master_jobs_mode = ChildrenMasterJobsMode,
    track_subtree_status = TrackSubtreeStatus,
    batch_size = BatchSize,
    traverse_info = TraverseInfo
}, Pool, TaskID, CallbackModule) ->
    Uuid = file_ctx:get_uuid_const(FileCtx),
    Scope = file_ctx:get_space_id_const(FileCtx),
    Record = #tree_traverse_job{
        pool = Pool,
        callback_module = CallbackModule,
        task_id = TaskID,
        doc_id = Uuid,
        session_id = user_ctx:get_session_id(UserCtx),
        use_token = Token =/= undefined,
        last_name = LastName,
        last_tree = LastTree,
        execute_slave_on_dir = OnDir,
        children_master_jobs_mode = ChildrenMasterJobsMode,
        track_subtree_status = TrackSubtreeStatus,
        batch_size = BatchSize,
        traverse_info = term_to_binary(TraverseInfo)
    },
    Ctx = get_extended_ctx(Job, CallbackModule),
    save(Key, Scope, Record, Ctx).

-spec delete_master_job(datastore:key(), tree_traverse:master_job(), datastore_doc:scope(), traverse:callback_module()) ->
    ok | {error, term()}.
delete_master_job(<<?MAIN_JOB_PREFIX, _/binary>> = Key, Job, Scope, CallbackModule) ->
    Ctx = get_extended_ctx(Job, CallbackModule),
    datastore_model:delete(Ctx#{scope => Scope}, Key);
delete_master_job(Key, Job, _, CallbackModule) ->
    Ctx = get_extended_ctx(Job, CallbackModule),
    datastore_model:delete(Ctx, Key).

-spec get_master_job(key() | doc()) ->
    {ok, tree_traverse:master_job(), traverse:pool(), traverse:id()}  | {error, term()}.
get_master_job(#document{value = #tree_traverse_job{
    pool = Pool, task_id = TaskID,
    doc_id = DocID,
    session_id = SessionId,
    use_token = UseToken,
    last_name = LastName,
    last_tree = LastTree,
    execute_slave_on_dir = OnDir,
    children_master_jobs_mode = ChildrenMasterJobsMode,
    track_subtree_status = TrackSubtreeStatus,
    batch_size = BatchSize,
    traverse_info = TraverseInfo
}}) ->
    {ok, Doc = #document{scope = SpaceId}} = file_meta:get_including_deleted(DocID),
    FileCtx = file_ctx:new_by_doc(Doc, SpaceId),
    Job = #tree_traverse{
        file_ctx = FileCtx,
        user_ctx = user_ctx:new(SessionId),
        token = case UseToken of
            true -> #link_token{};
            false -> undefined
        end,
        last_name = LastName,
        last_tree = LastTree,
        execute_slave_on_dir = OnDir,
        children_master_jobs_mode = ChildrenMasterJobsMode,
        track_subtree_status = TrackSubtreeStatus,
        batch_size = BatchSize,
        traverse_info = binary_to_term(TraverseInfo)
    },
    {ok, Job, Pool, TaskID};
get_master_job(Key) ->
    case datastore_model:get(?CTX#{include_deleted => true}, Key) of
        {ok, Doc} ->
            get_master_job(Doc);
        Other ->
            Other
    end.

%%%===================================================================
%%% datastore_model callbacks
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Returns model's context used by datastore and dbsync.
%% @end
%%--------------------------------------------------------------------
-spec get_ctx() -> datastore:ctx().
get_ctx() ->
    ?SYNC_CTX.


%%--------------------------------------------------------------------
%% @doc
%% Returns model's record version.
%% @end
%%--------------------------------------------------------------------
-spec get_record_version() -> datastore_model:record_version().
get_record_version() ->
    2.


-spec get_record_struct(datastore_model:record_version()) ->
    datastore_model:record_struct().
get_record_struct(1) ->
    {record, [
        {pool, string},
        {callback_module, atom},
        {task_id, string},
        {doc_id, string},
        {last_name, string},
        {last_tree, string},
        {execute_slave_on_dir, boolean},
        {batch_size, integer},
        {traverse_info, binary}
    ]};
get_record_struct(2) ->
    {record, [
        {pool, string},
        {callback_module, atom},
        {task_id, string},
        {doc_id, string},
        % session_id has been added in this version
        {session_id, string},
        % use_token field has been added in this version
        {use_token, boolean},
        {last_name, string},
        {last_tree, string},
        {execute_slave_on_dir, boolean},
        % children_master_jobs_mode has been added in this version
        {children_master_jobs_mode, atom},
        % track_subtree_status has been added in this version
        {track_subtree_status, boolean},
        {batch_size, integer},
        {traverse_info, binary}
    ]}.


%%--------------------------------------------------------------------
%% @doc
%% Upgrades model's record from provided version to the next one.
%% @end
%%--------------------------------------------------------------------
-spec upgrade_record(datastore_model:record_version(), datastore_model:record()) ->
    {datastore_model:record_version(), datastore_model:record()}.
upgrade_record(1, {?MODULE, Pool, CallbackModule, TaskId, DocId, LastName, LastTree, ExecuteSlaveOnDir,
    BatchSize, TraverseInfo}
) ->
    {2, {?MODULE,
        Pool,
        CallbackModule,
        TaskId,
        DocId,
        % session_id has been added in this version
        ?ROOT_SESS_ID,
        % use_token field has been added in this version
        true,
        LastName,
        LastTree,
        ExecuteSlaveOnDir,
        % children_master_jobs_mode has been added in this version
        sync,
        % track_subtree_status has been added in this version
        false,
        BatchSize,
        TraverseInfo
    }}.

%%%===================================================================
%%% Internal functions
%%%===================================================================

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Saves information about job. Generates special key for main jobs (see tree_traverse.erl) to treat the differently
%% (main jobs are synchronized between providers - other not).
%% @end
%%--------------------------------------------------------------------
-spec save(datastore:key() | main_job, datastore_doc:scope(), record(), datastore:ctx()) -> {ok, key()} | {error, term()}.
save(main_job, Scope, Value, Ctx) ->
    RandomPart = datastore_key:new(),
    GenKey = <<?MAIN_JOB_PREFIX, RandomPart/binary>>,
    ?extract_key(datastore_model:save(Ctx#{generated_key => true},
        #document{key = GenKey, scope = Scope, value = Value}));
save(<<?MAIN_JOB_PREFIX, _/binary>> = Key, Scope, Value, Ctx) ->
    ?extract_key(datastore_model:save(Ctx, #document{key = Key, scope = Scope, value = Value}));
save(Key, _, Value, Ctx) ->
    ?extract_key(datastore_model:save(Ctx, #document{key = Key, value = Value})).


-spec get_extended_ctx(tree_traverse:master_job(), traverse:callback_module()) -> datastore:ctx().
get_extended_ctx(Job, CallbackModule) ->
    {ok, ExtendedCtx} = case erlang:function_exported(CallbackModule, get_sync_info, 1) of
        true ->
            CallbackModule:get_sync_info(Job);
        _ ->
            {ok, #{}}
    end,
    maps:merge(?CTX, ExtendedCtx).