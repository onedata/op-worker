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

% Time in seconds for main job document to expire after delete (one year).
% After expiration of main job document, information about traverse cancellation
% cannot be propagated to other providers.
-define(MAIN_JOB_EXPIRY, 31536000).

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
    user_id = UserId,
    tune_for_large_continuous_listing = TuneForLargeContinuousListing,
    pagination_token = ListingPaginationToken,
    child_dirs_job_generation_policy = ChildDirsJobGenerationPolicy,
    children_master_jobs_mode = ChildrenMasterJobsMode,
    track_subtree_status = TrackSubtreeStatus,
    batch_size = BatchSize,
    traverse_info = TraverseInfo,
    resolved_root_uuids = ResolvedRootUuids,
    symlink_resolution_policy = SymlinkResolutionPolicy,
    relative_path = RelativePath,
    encountered_files = EncounteredFilesMap
}, Pool, TaskId, CallbackModule) ->
    Uuid = file_ctx:get_logical_uuid_const(FileCtx),
    Scope = file_ctx:get_space_id_const(FileCtx),
    Record = #tree_traverse_job{
        pool = Pool,
        callback_module = CallbackModule,
        task_id = TaskId,
        doc_id = Uuid,
        user_id = UserId,
        tune_for_large_continuous_listing = TuneForLargeContinuousListing,
        pagination_token = ListingPaginationToken,
        child_dirs_job_generation_policy = ChildDirsJobGenerationPolicy,
        children_master_jobs_mode = ChildrenMasterJobsMode,
        track_subtree_status = TrackSubtreeStatus,
        batch_size = BatchSize,
        traverse_info = term_to_binary(TraverseInfo),
        symlink_resolution_policy = SymlinkResolutionPolicy,
        resolved_root_uuids = ResolvedRootUuids,
        relative_path = RelativePath,
        encountered_files = EncounteredFilesMap
    },
    Ctx = get_extended_ctx(Job, CallbackModule),
    save(Key, Scope, Record, Ctx).

-spec delete_master_job(datastore:key(), tree_traverse:master_job(), datastore_doc:scope(), traverse:callback_module()) ->
    ok | {error, term()}.
delete_master_job(<<?MAIN_JOB_PREFIX, _/binary>> = Key, Job, Scope, CallbackModule) ->
    Ctx = get_extended_ctx(Job, CallbackModule),
    Ctx2 = case maps:get(sync_enabled, Ctx, false) of
        true -> Ctx#{scope => Scope};
        false -> Ctx
    end,
    Ctx3 = datastore_model:set_expiry(Ctx2, ?MAIN_JOB_EXPIRY),
    datastore_model:delete(Ctx3, Key);
delete_master_job(Key, _Job, _, _CallbackModule) ->
    datastore_model:delete(?CTX, Key).

-spec get_master_job(key() | doc()) ->
    {ok, tree_traverse:master_job(), traverse:pool(), traverse:id()}  | {error, term()}.
get_master_job(#document{value = #tree_traverse_job{
    pool = Pool,
    task_id = TaskId,
    doc_id = DocId,
    user_id = UserId,
    tune_for_large_continuous_listing = TuneForLargeContinuousListing,
    pagination_token = PaginationToken,
    child_dirs_job_generation_policy = ChildDirsJobGenerationPolicy,
    children_master_jobs_mode = ChildrenMasterJobsMode,
    track_subtree_status = TrackSubtreeStatus,
    batch_size = BatchSize,
    traverse_info = TraverseInfo,
    symlink_resolution_policy = SymlinkResolutionPolicy,
    resolved_root_uuids = ResolvedRootUuids,
    relative_path = RelativePath,
    encountered_files = EncounteredFilesMap
}}) ->
    case file_meta:get_including_deleted(DocId) of
        {ok, Doc = #document{scope = SpaceId}} ->
            FileCtx = file_ctx:new_by_doc(Doc, SpaceId),
            Job = #tree_traverse{
                file_ctx = FileCtx,
                user_id = UserId,
                tune_for_large_continuous_listing = TuneForLargeContinuousListing,
                pagination_token = PaginationToken,
                child_dirs_job_generation_policy = ChildDirsJobGenerationPolicy,
                children_master_jobs_mode = ChildrenMasterJobsMode,
                track_subtree_status = TrackSubtreeStatus,
                batch_size = BatchSize,
                traverse_info = binary_to_term(TraverseInfo),
                symlink_resolution_policy = SymlinkResolutionPolicy,
                resolved_root_uuids = ResolvedRootUuids,
                relative_path = RelativePath,
                encountered_files = EncounteredFilesMap
            },
            {ok, Job, Pool, TaskId};
        {error, _} = Error ->
            Error
    end;
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
    %%% !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
    %%% WARNING: this is a synced model and MUST NOT be changed outside of a new major release!!!
    %%% !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
    6.


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
        % user_id has been added in this version
        {user_id, string},
        % use_listing_token field has been added in this version
        {use_listing_token, boolean},
        {last_name, string},
        {last_tree, string},
        {execute_slave_on_dir, boolean},
        % children_master_jobs_mode has been added in this version
        {children_master_jobs_mode, atom},
        % track_subtree_status has been added in this version
        {track_subtree_status, boolean},
        {batch_size, integer},
        {traverse_info, binary}
    ]};
get_record_struct(3) ->
    {record, [
        {pool, string},
        {callback_module, atom},
        {task_id, string},
        {doc_id, string},
        {user_id, string},
        {use_listing_token, boolean},
        {last_name, string},
        {last_tree, string},
        % execute_slave_on_dir has been changed to child_dirs_job_generation_policy in this version
        {child_dirs_job_generation_policy, atom},
        {children_master_jobs_mode, atom},
        {track_subtree_status, boolean},
        {batch_size, integer},
        {traverse_info, binary}
    ]};
get_record_struct(4) ->
    {record, [
        {pool, string},
        {callback_module, atom},
        {task_id, string},
        {doc_id, string},
        {user_id, string},
        {use_listing_token, boolean},
        {last_name, string},
        {last_tree, string},
        {child_dirs_job_generation_policy, atom},
        {children_master_jobs_mode, atom},
        {track_subtree_status, boolean},
        {batch_size, integer},
        {traverse_info, binary},
        % new fields - follow_symlinks, relative_path and encountered_files
        {follow_symlinks, boolean},
        {relative_path, binary},
        {encountered_files, #{string => boolean}}
    ]};
get_record_struct(5) ->
    {record, [
        {pool, string},
        {callback_module, atom},
        {task_id, string},
        {doc_id, string},
        {user_id, string},
        {use_listing_token, boolean},
        {last_name, string},
        {last_tree, string},
        {child_dirs_job_generation_policy, atom},
        {children_master_jobs_mode, atom},
        {track_subtree_status, boolean},
        {batch_size, integer},
        {traverse_info, binary},
        {symlink_resolution_policy, atom}, % modified field
        {resolved_root_uuids, [string]}, % new field
        {relative_path, binary},
        {encountered_files, #{string => boolean}}
    ]};
get_record_struct(6) ->
    %%% !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
    %%% WARNING: this is a synced model and MUST NOT be changed outside of a new major release!!!
    %%% !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
    {record, [
        {pool, string},
        {callback_module, atom},
        {task_id, string},
        {doc_id, string},
        {user_id, string},
        {tune_for_large_continuous_listing, boolean}, % modified field (renamed from use_listing_token)
        {pagination_token, {custom, string, {file_listing, encode_pagination_token, decode_pagination_token}}}, % new field
        % removed fields last_name and last_tree
        {child_dirs_job_generation_policy, atom},
        {children_master_jobs_mode, atom},
        {track_subtree_status, boolean},
        {batch_size, integer},
        {traverse_info, binary},
        {symlink_resolution_policy, atom},
        {resolved_root_uuids, [string]},
        {relative_path, binary},
        {encountered_files, #{string => boolean}}
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
        % user_id has been added in this version
        ?ROOT_USER_ID,
        % use_listing_token field has been added in this version
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
    }};
upgrade_record(2, Record) ->
    {
        ?MODULE,
        Pool,
        CallbackModule,
        TaskId,
        DocId,
        UserId,
        UseListingToken,
        LastName,
        LastTree,
        ExecuteSlaveOnDir,
        ChildrenMasterJobsMode,
        TrackSubtreeStatus,
        BatchSize,
        TraverseInfo
    } = Record,
    
    ChildDirsJobGenerationPolicy = case ExecuteSlaveOnDir of
        false -> generate_master_jobs;
        true -> generate_slave_and_master_jobs
    end,
    
    {3, {?MODULE,
        Pool,
        CallbackModule,
        TaskId,
        DocId,
        UserId,
        UseListingToken,
        LastName,
        LastTree,
        ChildDirsJobGenerationPolicy,
        ChildrenMasterJobsMode,
        TrackSubtreeStatus,
        BatchSize,
        TraverseInfo
    }};
upgrade_record(3, Record) ->
    {
        ?MODULE,
        Pool,
        CallbackModule,
        TaskId,
        DocId,
        UserId,
        UseListingToken,
        LastName,
        LastTree,
        ChildDirsJobGenerationPolicy,
        ChildrenMasterJobsMode,
        TrackSubtreeStatus,
        BatchSize,
        TraverseInfo
    } = Record,
    
    {4, {?MODULE,
        Pool,
        CallbackModule,
        TaskId,
        DocId,
        UserId,
        UseListingToken,
        LastName,
        LastTree,
        ChildDirsJobGenerationPolicy,
        ChildrenMasterJobsMode,
        TrackSubtreeStatus,
        BatchSize,
        TraverseInfo,
        % new fields follow_symlinks, relative path and encountered files
        false,
        <<>>,
        #{}
    }};
upgrade_record(4, Record) ->
    {
        ?MODULE,
        Pool,
        CallbackModule,
        TaskId,
        DocId,
        UserId,
        UseListingToken,
        LastName,
        LastTree,
        ChildDirsJobGenerationPolicy,
        ChildrenMasterJobsMode,
        TrackSubtreeStatus,
        BatchSize,
        TraverseInfo,
        FollowSymlinks,
        RelativePath,
        EncounteredFiles
    } = Record,
    
    SymlinkResolutionPolicy = case FollowSymlinks of
        true -> follow_external;
        false -> preserve
    end,
    
    {5, {?MODULE,
        Pool,
        CallbackModule,
        TaskId,
        DocId,
        UserId,
        UseListingToken,
        LastName,
        LastTree,
        ChildDirsJobGenerationPolicy,
        ChildrenMasterJobsMode,
        TrackSubtreeStatus,
        BatchSize,
        TraverseInfo,
        SymlinkResolutionPolicy, % modified field
        [], % new field resolved_root_uuids
        RelativePath,
        EncounteredFiles
    }};
upgrade_record(5, Record) ->
    {
        ?MODULE,
        Pool,
        CallbackModule,
        TaskId,
        DocId,
        UserId,
        UseListingToken,
        LastName,
        LastTree,
        ChildDirsJobGenerationPolicy,
        ChildrenMasterJobsMode,
        TrackSubtreeStatus,
        BatchSize,
        TraverseInfo,
        SymlinkResolutionPolicy,
        RootUuids,
        RelativePath,
        EncounteredFiles
    } = Record,
    
    TuneForLargeContinuousListing = UseListingToken,
    Index = file_listing:build_index(LastName, LastTree),
    % listing with limit 0 does not list anything, but returns a pagination_token that can be used 
    % to continue listing from this point
    {ok, [], ListingPaginationToken} = file_listing:list(<<"dummy_uuid">>, #{
        index => Index, 
        tune_for_large_continuous_listing => TuneForLargeContinuousListing,
        limit => 0
    }),

    {6, {?MODULE,
        Pool,
        CallbackModule,
        TaskId,
        DocId,
        UserId,
        TuneForLargeContinuousListing,
        ListingPaginationToken,
        ChildDirsJobGenerationPolicy,
        ChildrenMasterJobsMode,
        TrackSubtreeStatus,
        BatchSize,
        TraverseInfo,
        SymlinkResolutionPolicy,
        RootUuids,
        RelativePath,
        EncounteredFiles
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
save(main_job, Scope, Value, ExtendedCtx) ->
    RandomPart = datastore_key:new(),
    GenKey = <<?MAIN_JOB_PREFIX, RandomPart/binary>>,
    Doc = #document{key = GenKey, value = Value},
    Doc2 = set_scope_if_sync_enabled(Doc, Scope, ExtendedCtx),
    ?extract_key(datastore_model:save(ExtendedCtx#{generated_key => true}, Doc2));
save(<<?MAIN_JOB_PREFIX, _/binary>> = Key, Scope, Value, ExtendedCtx) ->
    Doc = #document{key = Key, value = Value},
    Doc2 = set_scope_if_sync_enabled(Doc, Scope, ExtendedCtx),
    ?extract_key(datastore_model:save(ExtendedCtx, Doc2));
save(Key, _, Value, _ExtendedCtx) ->
    ?extract_key(datastore_model:save(?CTX, #document{key = Key, value = Value})).


-spec set_scope_if_sync_enabled(doc(), datastore_doc:scope(), datastore:ctx()) -> doc().
set_scope_if_sync_enabled(Doc, Scope, #{sync_enabled := true}) ->
    Doc#document{scope = Scope};
set_scope_if_sync_enabled(Doc, _Scope, _Ctx) ->
    Doc.


-spec get_extended_ctx(tree_traverse:master_job(), traverse:callback_module()) -> datastore:ctx().
get_extended_ctx(Job, CallbackModule) ->
    {ok, ExtendedCtx} = case erlang:function_exported(CallbackModule, get_sync_info, 1) of
        true ->
            CallbackModule:get_sync_info(Job);
        _ ->
            {ok, #{}}
    end,
    maps:merge(?CTX, ExtendedCtx).