%%%--------------------------------------------------------------------
%%% @author Michal Cwiertnia
%%% @copyright (C) 2019 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%--------------------------------------------------------------------
%%% @doc
%%% This module contains functions responsible for traversing tree and
%%% performing actions related to QoS management.
%%% Traverse is started for each storage that given QoS requires files
%%% to be. Traverse is run on provider, that given storage belongs to.
%%% @end
%%%--------------------------------------------------------------------
-module(qos_traverse).
-author("Michal Cwiertnia").

-include("global_definitions.hrl").
-include("proto/oneclient/fuse_messages.hrl").
-include("modules/fslogic/fslogic_common.hrl").
-include("modules/datastore/qos.hrl").
-include_lib("ctool/include/logging.hrl").

%% API
-export([restore_qos/3, start_initial_traverse/4, init_pool/0]).

%% Traverse behaviour callbacks
-export([do_master_job/2, do_slave_job/2,
    task_finished/1, get_job/1, get_sync_info/1, update_job_progress/5]).

% For test purpose
-export([synchronize_file/2]).

-record(add_qos_traverse_args, {
    qos_id :: qos_entry:id(),
    file_path_tokens = [] :: [binary()],
    target_storage = undefined :: storage:id() | undefined
}).

-type task_type() :: traverse | restore.

-define(POOL_NAME, atom_to_binary(?MODULE, utf8)).
-define(TRAVERSE_BATCH_SIZE, application:get_env(?APP_NAME, qos_traverse_batch_size, 40)).


%%%===================================================================
%%% API
%%%===================================================================

-spec restore_qos(file_ctx:ctx(), qos_entry:id(), storage:id()) -> ok.
restore_qos(FileCtx, QosId, TargetStorage) ->
    {ok, QosOriginFileGuid} = qos_entry:get_file_guid(QosId),
    {FilePathTokens, _FileCtx} = file_ctx:get_canonical_path_tokens(file_ctx:new_by_guid(QosOriginFileGuid)),
    SpaceId = file_ctx:get_space_id_const(FileCtx),
    TaskId = datastore_utils:gen_key(),
    RelativePath = qos_status:get_relative_path(FilePathTokens, file_ctx:get_guid_const(FileCtx)),
    Options = #{
        task_id => TaskId,
        batch_size => ?TRAVERSE_BATCH_SIZE,
        traverse_info => #add_qos_traverse_args{
            qos_id = QosId,
            file_path_tokens = FilePathTokens,
            target_storage = TargetStorage
        },
        % fixme create model
        additional_data => #{
            <<"qos_id">> => QosId,
            <<"space_id">> => SpaceId,
            <<"relative_path">> => RelativePath,
            <<"target_storage">> => TargetStorage,
            <<"task_type">> => <<"restore">>
        }
    },
    ok = qos_status:add_link(QosId, SpaceId, RelativePath, TaskId, TargetStorage),
    {ok, _} = tree_traverse:run(?POOL_NAME, FileCtx, Options),
    ok.

%%--------------------------------------------------------------------
%% @doc
%% Creates traverse task to fulfill qos.
%% @end
%%--------------------------------------------------------------------
-spec start_initial_traverse(file_ctx:ctx(), qos_entry:id(), storage:id(), traverse:id()) -> ok.
start_initial_traverse(FileCtx, QosId, TargetStorage, TaskId) ->
    Options = #{
        task_id => TaskId,
        batch_size => ?TRAVERSE_BATCH_SIZE,
        traverse_info => #add_qos_traverse_args{
            qos_id = QosId,
            target_storage = TargetStorage
        },
        additional_data => #{
            <<"qos_id">> => QosId,
            <<"task_type">> => <<"traverse">>
        }
    },
    {ok, _} = tree_traverse:run(?POOL_NAME, FileCtx, Options),
    ok.

%%--------------------------------------------------------------------
%% @doc
%% Initializes pool for traverse tasks concerning qos management.
%% @end
%%--------------------------------------------------------------------
-spec init_pool() -> ok.
init_pool() ->
    % Get pool limits from app.config
    MasterJobsLimit = application:get_env(?APP_NAME, qos_traverse_master_jobs_limit, 10),
    SlaveJobsLimit = application:get_env(?APP_NAME, qos_traverse_slave_jobs_limit, 20),
    ParallelismLimit = application:get_env(?APP_NAME, qos_traverse_parallelism_limit, 20),

    tree_traverse:init(?MODULE, MasterJobsLimit, SlaveJobsLimit, ParallelismLimit).

%%%===================================================================
%%% Traverse callbacks
%%%===================================================================

-spec get_job(traverse:job_id() | tree_traverse_job:doc()) ->
    {ok, tree_traverse:master_job(), traverse:pool(), traverse:id()}  | {error, term()}.
get_job(DocOrID) ->
    tree_traverse:get_job(DocOrID).

-spec get_sync_info(tree_traverse:master_job()) -> {ok, traverse:sync_info()}.
get_sync_info(Job) ->
    tree_traverse:get_sync_info(Job).

-spec task_finished(traverse:id()) -> ok.
task_finished(TaskId) ->
    {ok, #{
        <<"qos_id">> := QosId,
        <<"task_type">> := TaskType
    } = AdditionalData} = traverse_task:get_additional_data(?POOL_NAME, TaskId),
    case TaskType of
        <<"traverse">> ->
            {ok, _} = qos_entry:mark_traverse_finished(QosId, TaskId),
            ok;
        <<"restore">> ->
            #{
                <<"space_id">> := SpaceId,
                <<"relative_path">> := RelativePath,
                <<"target_storage">> := TargetStorage
            } = AdditionalData,
            ok = qos_status:delete_link(QosId, SpaceId, RelativePath, TaskId, TargetStorage)
    end.

-spec update_job_progress(undefined | main_job | traverse:job_id(),
    tree_traverse:master_job(), traverse:pool(), traverse:id(),
    traverse:job_status()) -> {ok, traverse:job_id()}  | {error, term()}.
update_job_progress(Id, Job, Pool, TaskId, Status) ->
    tree_traverse:update_job_progress(Id, Job, Pool, TaskId, Status, ?MODULE).

-spec do_master_job(tree_traverse:master_job(), traverse:id()) -> {ok, traverse:master_job_map()}.
do_master_job(Job, TaskId) ->
    tree_traverse:do_master_job(Job, TaskId).


%%--------------------------------------------------------------------
%% @doc
%% Performs slave job for traverse task responsible for scheduling replications
%% to fulfill QoS requirements.
%% @end
%%--------------------------------------------------------------------
-spec do_slave_job(traverse:job(), traverse:id()) -> ok.
do_slave_job({#document{key = FileUuid, scope = SpaceId},
    #add_qos_traverse_args{target_storage = _TargetStorage}}, _TaskId) ->
    FileGuid = file_id:pack_guid(FileUuid, SpaceId),
    FileCtx = file_ctx:new_by_guid(FileGuid),
    UserCtx = user_ctx:new(?ROOT_SESS_ID),
    % TODO: add space check and optionally choose other storage

    % call using ?MODULE macro for mocking in tests
    ok = ?MODULE:synchronize_file(UserCtx, FileCtx).


%%%===================================================================
%%% Internal functions
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Synchronizes file to given storage.
%% @end
%%--------------------------------------------------------------------
-spec synchronize_file(user_ctx:ctx(), file_ctx:ctx()) -> ok.
synchronize_file(UserCtx, FileCtx) ->
    {Size, FileCtx2} = file_ctx:get_file_size(FileCtx),
    FileBlock = #file_block{offset = 0, size = Size},
    case replica_synchronizer:synchronize(UserCtx, FileCtx2, FileBlock, false, undefined, 1) of
        {ok, _} ->
            ok;
        {error, Reason} ->
            % TODO: VFS-5737 handle failures properly
            ?error("Error during file synchronization: ~p", [Reason])
    end,
    ok.
