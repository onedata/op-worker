%%%-------------------------------------------------------------------
%%% @author Michal Wrzeszcz
%%% @copyright (C) 2019 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% This module provides base functionality for directory tree traversing.
%%% @end
%%%-------------------------------------------------------------------
-module(tree_traverse).
-author("Michal Wrzeszcz").

% This module is base for traver jobs
% (slave jobs and task finish callbacks has to be defined)
%-behaviour(job_behaviour).

-include("proto/oneclient/fuse_messages.hrl").
-include("modules/datastore/datastore_models.hrl").
-include_lib("cluster_worker/include/modules/datastore/datastore_links.hrl").

%% API
-export([init/4, run/5, run/6, run/7, run/8, get_traverse_info/1, get_doc/1]).
%% Behaviour callbacks
-export([do_master_job/1, save_job/3, update_job/4, get_job/1, get_sync_info/0]).

-record(tree_travserse, {
    doc :: file_meta:doc(),
    token :: datastore_links_iter:token() | undefined,
    last_name = <<>> :: file_meta:name(),
    last_tree = <<>> :: od_provider:id(),
    execute_slave_on_dir :: boolean(),
    batch_size :: non_neg_integer(),
    traverse_info :: term()
}).

-type master_job() :: #tree_travserse{}.
-type slave_job() :: file_meta:doc().

-define(DEFAULT_GROUP, <<"main_group">>).

%%%===================================================================
%%% API
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Initializes pool.
%% @end
%%--------------------------------------------------------------------
-spec init(traverse:task_module(), non_neg_integer(), non_neg_integer(), non_neg_integer()) -> ok.
init(CallbackModule, MasterJobsNum, SlaveJobsNum, ParallelOrdersLimit) ->
    traverse:init_pool(CallbackModule, MasterJobsNum, SlaveJobsNum, ParallelOrdersLimit, oneprovider:get_id_or_undefined()).

run(TaskModule, Doc, TaskID, BatchSize, TraverseInfo) ->
    run(TaskModule, Doc, TaskID, false, BatchSize, TraverseInfo).

run(TaskModule, Doc, TaskID, ExecuteActionOnDir, BatchSize, TraverseInfo) ->
    run(TaskModule, Doc, TaskID, ?DEFAULT_GROUP, ExecuteActionOnDir, BatchSize, TraverseInfo).

run(TaskModule, #document{} = Doc, TaskID, TaskGroup, ExecuteActionOnDir, BatchSize, TraverseInfo) ->
    traverse:run(TaskModule, TaskID, TaskGroup, #tree_travserse{
        doc = Doc,
        execute_slave_on_dir = ExecuteActionOnDir,
        batch_size = BatchSize,
        traverse_info = TraverseInfo
    }, oneprovider:get_id_or_undefined());
run(TaskModule, FileCtx, TaskID, TaskGroup, ExecuteActionOnDir, BatchSize, TraverseInfo) ->
    {Doc, _} = file_ctx:get_file_doc(FileCtx),
    run(TaskModule, Doc, TaskID, TaskGroup, ExecuteActionOnDir, BatchSize, TraverseInfo).

run(TaskModule, #document{} = Doc, TaskID, TaskGroup, ExecuteActionOnDir, BatchSize, TraverseInfo, ProviderID) ->
    traverse:run(TaskModule, TaskID, TaskGroup, #tree_travserse{
        doc = Doc,
        execute_slave_on_dir = ExecuteActionOnDir,
        batch_size = BatchSize,
        traverse_info = TraverseInfo
    }, oneprovider:get_id_or_undefined(), ProviderID);
run(TaskModule, FileCtx, TaskID, TaskGroup, ExecuteActionOnDir, BatchSize, TraverseInfo, ProviderID) ->
    {Doc, _} = file_ctx:get_file_doc(FileCtx),
    run(TaskModule, Doc, TaskID, TaskGroup, ExecuteActionOnDir, BatchSize, TraverseInfo, ProviderID).

get_traverse_info(#tree_travserse{traverse_info = TraverseInfo}) ->
    TraverseInfo.

get_doc(#tree_travserse{doc = Doc}) ->
    Doc.

%%%===================================================================
%%% Behaviour callbacks
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Does master job that traverse directory tree.
%% @end
%%--------------------------------------------------------------------
-spec do_master_job(master_job()) -> {ok, [slave_job()], [master_job()]}.
do_master_job(#tree_travserse{
    doc = #document{value = #file_meta{}} = Doc,
    token = Token,
    last_name = LN,
    last_tree = LT,
    execute_slave_on_dir = OnDir,
    batch_size = BatchSize,
    traverse_info = TraverseInfo
}) ->
    {ok, Children, ExtendedInfo} = case {Token, LN} of
        {undefined, <<>>} -> file_meta:list_children(Doc, BatchSize);
        {undefined, _} -> file_meta:list_children_by_key(Doc, LN, LT, BatchSize);
        _ -> file_meta:list_children_by_key(Doc, LN, LT, BatchSize, Token)
    end,

    #{token := Token2, last_name := LN2, last_tree := LT2} = ExtendedInfo,

    {SlaveJobs, MasterJobs} = lists:foldl(fun(#child_link_uuid{
        uuid = UUID}, {Slaves, Masters} = Acc) ->
        case {file_meta:get({uuid, UUID}), OnDir} of
            {{ok, #document{value = #file_meta{type = ?DIRECTORY_TYPE}} = ChildDoc}, true} ->
                {[{ChildDoc, TraverseInfo} | Slaves], [#tree_travserse{doc = ChildDoc,
                    execute_slave_on_dir = OnDir, batch_size = BatchSize,
                    traverse_info = TraverseInfo} | Masters]};
            {{ok, #document{value = #file_meta{type = ?DIRECTORY_TYPE}} = ChildDoc}, _} ->
                {Slaves, [#tree_travserse{doc = ChildDoc,
                    execute_slave_on_dir = OnDir, batch_size = BatchSize,
                    traverse_info = TraverseInfo} | Masters]};
            {{ok, ChildDoc}, _} ->
                {[{ChildDoc, TraverseInfo} | Slaves], Masters};
            {{error, not_found}, _} ->
                Acc
        end
    end, {[], []}, Children),

    case Token2#link_token.is_last of
        true -> {ok, lists:reverse(SlaveJobs), lists:reverse(MasterJobs)};
        false -> {ok, lists:reverse(SlaveJobs), [#tree_travserse{
            doc = Doc,
            token = Token2,
            last_name = LN2,
            last_tree = LT2,
            execute_slave_on_dir = OnDir,
            batch_size = BatchSize,
            traverse_info = TraverseInfo
        } | lists:reverse(MasterJobs)]}
    end.

save_job(#tree_travserse{
    doc = #document{key = DocID, scope = Scope},
    last_name = LN,
    last_tree = LT,
    execute_slave_on_dir = OnDir,
    batch_size = BatchSize,
    traverse_info = TraverseInfo
}, TaskID, _Status) ->
    {ok, #document{key = ID}} = tree_travserse_job:save(undefined, TaskID, DocID, Scope, LN, LT, OnDir, BatchSize, TraverseInfo),
    {ok, ID}.

update_job(ID, #tree_travserse{
    doc = #document{key = DocID, scope = Scope},
    last_name = LN,
    last_tree = LT,
    execute_slave_on_dir = OnDir,
    batch_size = BatchSize,
    traverse_info = TraverseInfo
}, TaskID, _Status) ->
    {ok, _} = tree_travserse_job:save(ID, TaskID, DocID, Scope, LN, LT, OnDir, BatchSize, TraverseInfo),
    ok.

get_job(ID) ->
    case tree_travserse_job:get(ID) of
        {ok, TaskID, DocID, LN, LT, OnDir, BatchSize, TraverseInfo} ->
            {ok, Doc} = file_meta:get(DocID),
            Job = #tree_travserse{
                doc = Doc,
                last_name = LN,
                last_tree = LT,
                execute_slave_on_dir = OnDir,
                batch_size = BatchSize,
                traverse_info = TraverseInfo
            },
            {ok, Job, TaskID};
        Other ->
            Other
    end.

get_sync_info() ->
    #{
        sync_enabled => true,
        remote_driver => datastore_remote_driver,
        mutator => oneprovider:get_id_or_undefined(),
        local_links_tree_id => oneprovider:get_id_or_undefined(),
        scope => <<"space1">>
    }.