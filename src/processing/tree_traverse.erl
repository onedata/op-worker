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
% (other callbacks especially for slave jobs have to be defined)
%-behaviour(job_behaviour).

-include("proto/oneclient/fuse_messages.hrl").
-include("modules/datastore/datastore_models.hrl").
-include_lib("cluster_worker/include/modules/datastore/datastore_links.hrl").

%% Main API
-export([init/4, init/5, run/2, run/3]).
% Getters API
-export([get_traverse_info/1, get_doc/1, get_task/2]).
%% Behaviour callbacks
-export([do_master_job/1, update_job_progress/6, get_job/1, get_sync_info/1, get_timestamp/0]).

-record(tree_travserse, {
    doc :: file_meta:doc(),
    token :: datastore_links_iter:token() | undefined,
    last_name = <<>> :: file_meta:name(),
    last_tree = <<>> :: od_provider:id(),
    execute_slave_on_dir :: execute_slave_on_dir(),
    batch_size :: batch_size(),
    traverse_info :: traverse_info()
}).

-type master_job() :: #tree_travserse{}.
-type slave_job() :: file_meta:doc().
-type execute_slave_on_dir() :: boolean().
-type batch_size() :: non_neg_integer().
-type traverse_info() :: term().
-type run_options() :: #{
    task_id => traverse:id(),
    execute_slave_on_dir => execute_slave_on_dir(),
    batch_size => batch_size(),
    traverse_info => traverse_info(),
    target_provider_id => oneprovider:id(),
    callback_module => traverse:callback_module(),
    group_id => traverse:group()
}.

-export_type([slave_job/0, execute_slave_on_dir/0, batch_size/0, traverse_info/0]).

%%%===================================================================
%%% Main API
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Initializes pool.
%% @end
%%--------------------------------------------------------------------
-spec init(traverse:pool() | atom(), non_neg_integer(), non_neg_integer(), non_neg_integer()) -> ok.
init(Pool, MasterJobsNum, SlaveJobsNum, ParallelOrdersLimit) when is_atom(Pool) ->
    init(atom_to_binary(Pool, utf8), MasterJobsNum, SlaveJobsNum, ParallelOrdersLimit);
init(Pool, MasterJobsNum, SlaveJobsNum, ParallelOrdersLimit) ->
    traverse:init_pool(Pool, MasterJobsNum, SlaveJobsNum, ParallelOrdersLimit,
        #{executor => oneprovider:get_id_or_undefined()}).

%%--------------------------------------------------------------------
%% @doc
%% Initializes pool.
%% @end
%%--------------------------------------------------------------------
-spec init(traverse:pool() | atom(), non_neg_integer(), non_neg_integer(), non_neg_integer(),
    [traverse:callback_module()]) -> ok.
init(Pool, MasterJobsNum, SlaveJobsNum, ParallelOrdersLimit, CallbackModules) when is_atom(Pool) ->
    init(atom_to_binary(Pool, utf8), MasterJobsNum, SlaveJobsNum, ParallelOrdersLimit, CallbackModules);
init(Pool, MasterJobsNum, SlaveJobsNum, ParallelOrdersLimit, CallbackModules) ->
    traverse:init_pool(Pool, MasterJobsNum, SlaveJobsNum, ParallelOrdersLimit,
        #{executor => oneprovider:get_id_or_undefined(), callback_modules => CallbackModules}).

%%--------------------------------------------------------------------
%% @doc
%% @equiv run(Pool, Doc, #{}).
%% @end
%%--------------------------------------------------------------------
-spec run(traverse:pool() | atom(), file_meta:doc() | file_ctx:ctx()) -> {ok, traverse:id()}.
run(Pool, DocOrCtx) ->
    run(Pool, DocOrCtx, #{}).

%%--------------------------------------------------------------------
%% @doc
%% Runs job on pool.
%% @end
%%--------------------------------------------------------------------
-spec run(traverse:pool() | atom(), file_meta:doc() | file_ctx:ctx(), run_options()) -> {ok, traverse:id()}.
run(Pool, DocOrCtx, Opts) when is_atom(Pool) ->
    run(atom_to_binary(Pool, utf8), DocOrCtx, Opts);
run(Pool, #document{} = Doc, Opts) ->
    TaskID = case maps:get(task_id, Opts, undefined) of
                 undefined -> datastore_utils:gen_key();
                 ID -> ID
             end,
    ExecuteActionOnDir = maps:get(execute_slave_on_dir, Opts, false),
    BatchSize = maps:get(batch_size, Opts, 100),
    TraverseInfo = maps:get(traverse_info, Opts, undefined),

    RunOpts = case maps:get(target_provider_id, Opts, undefined) of
                  undefined -> #{executor => oneprovider:get_id_or_undefined()};
                  TargetID -> #{creator => oneprovider:get_id_or_undefined(), executor => TargetID}
              end,
    RunOpts2 = case maps:get(callback_module, Opts, undefined) of
                   undefined -> RunOpts;
                   CM -> RunOpts#{callback_module => CM}
               end,
    RunOpts3 = case maps:get(group_id, Opts, undefined) of
                   undefined -> RunOpts2;
                   Group -> RunOpts2#{group_id => Group}
               end,

    ok = traverse:run(Pool, TaskID, #tree_travserse{
        doc = Doc,
        execute_slave_on_dir = ExecuteActionOnDir,
        batch_size = BatchSize,
        traverse_info = TraverseInfo
    }, RunOpts3),
    {ok, TaskID};
run(Pool, FileCtx, Opts) ->
    {Doc, _} = file_ctx:get_file_doc(FileCtx),
    run(Pool, Doc, Opts).

%%%===================================================================
%%% Getters API
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Provides traverse info from master job record.
%% @end
%%--------------------------------------------------------------------
-spec get_traverse_info(master_job()) -> traverse_info().
get_traverse_info(#tree_travserse{traverse_info = TraverseInfo}) ->
    TraverseInfo.

%%--------------------------------------------------------------------
%% @doc
%% Provides doc info from master job record.
%% @end
%%--------------------------------------------------------------------
-spec get_doc(master_job()) -> file_meta:job().
get_doc(#tree_travserse{doc = Doc}) ->
    Doc.

%%--------------------------------------------------------------------
%% @doc
%% Returns task.
%% @end
%%--------------------------------------------------------------------
-spec get_task(traverse:pool() | atom(), traverse:id()) -> {ok, traverse_task:doc()} | {error, term()}.
get_task(Pool, ID) when is_atom(Pool) ->
    get_task(atom_to_binary(Pool, utf8), ID);
get_task(Pool, ID) ->
    traverse_task:get(Pool, ID).

%%%===================================================================
%%% Behaviour callbacks
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Does master job that traverse directory tree.
%% @end
%%--------------------------------------------------------------------
-spec do_master_job(master_job()) -> {ok, traverse:master_job_map()}.
do_master_job(#tree_travserse{
    doc = #document{value = #file_meta{}} = Doc,
    token = Token,
    last_name = LN,
    last_tree = LT,
    execute_slave_on_dir = OnDir,
    batch_size = BatchSize,
    traverse_info = TraverseInfo
} = TT) ->
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
                {[{ChildDoc, TraverseInfo} | Slaves], [get_child_job(TT, ChildDoc) | Masters]};
            {{ok, #document{value = #file_meta{type = ?DIRECTORY_TYPE}} = ChildDoc}, _} ->
                {Slaves, [get_child_job(TT, ChildDoc) | Masters]};
            {{ok, ChildDoc}, _} ->
                {[{ChildDoc, TraverseInfo} | Slaves], Masters};
            {{error, not_found}, _} ->
                Acc
        end
    end, {[], []}, Children),

    % TODO - czy job na nastepny batch nie powinien isc na koniec listy?
    FinalMasterJobs = case Token2#link_token.is_last of
                          true -> lists:reverse(MasterJobs);
                          false -> [TT#tree_travserse{
                              token = Token2,
                              last_name = LN2,
                              last_tree = LT2
                          } | lists:reverse(MasterJobs)]
                      end,
    {ok, #{slave_jobs => lists:reverse(SlaveJobs), master_jobs => FinalMasterJobs}}.

%%--------------------------------------------------------------------
%% @doc
%% Updates information about master jobs.
%% @end
%%--------------------------------------------------------------------
-spec update_job_progress(undefined | main_job | traverse:job_id(),
    master_job(), traverse:pool(), traverse:id(), traverse:job_status(), traverse:callback_module()) ->
    {ok, traverse:job_id()}  | {error, term()}.
update_job_progress(ID, #tree_travserse{
    doc = #document{key = DocID, scope = Scope},
    last_name = LN,
    last_tree = LT,
    execute_slave_on_dir = OnDir,
    batch_size = BatchSize,
    traverse_info = TraverseInfo
}, Pool, TaskID, Status, CallbackModule) when Status =:= waiting ; Status =:= on_pool ->
    tree_travserse_job:save(ID, Scope, Pool, CallbackModule, TaskID, DocID, LN, LT,
        OnDir, BatchSize, TraverseInfo);
update_job_progress(ID, #tree_travserse{doc = #document{scope = Scope}}, _, _, _, _) ->
    ok = tree_travserse_job:delete(ID, Scope),
    {ok, ID}.

%%--------------------------------------------------------------------
%% @doc
%% Gets master job.
%% @end
%%--------------------------------------------------------------------
-spec get_job(traverse:job_id() | tree_travserse_job:doc()) ->
    {ok, master_job(), traverse:pool(), traverse:id()}  | {error, term()}.
get_job(DocOrID) ->
    case tree_travserse_job:get(DocOrID) of
        {ok, Pool, _CallbackModule, TaskID, DocID, LN, LT, OnDir, BatchSize, TraverseInfo} ->
            {ok, Doc} = file_meta:get(DocID),
            Job = #tree_travserse{
                doc = Doc,
                last_name = LN,
                last_tree = LT,
                execute_slave_on_dir = OnDir,
                batch_size = BatchSize,
                traverse_info = TraverseInfo
            },
            {ok, Job, Pool, TaskID};
        Other ->
            Other
    end.

%%--------------------------------------------------------------------
%% @doc
%% Provides information needed for doc synchronization.
%% @end
%%--------------------------------------------------------------------
-spec get_sync_info(master_job()) -> {ok, traverse:ctx_sync_info()}.
get_sync_info(#tree_travserse{
    doc = #document{scope = Scope}
}) ->
    {ok, #{
        sync_enabled => true,
        remote_driver => datastore_remote_driver,
        mutator => oneprovider:get_id_or_undefined(),
        local_links_tree_id => oneprovider:get_id_or_undefined(),
        scope => Scope
    }}.

%%--------------------------------------------------------------------
%% @doc
%% Provides timestamp used for tasks listing.
%% @end
%%--------------------------------------------------------------------
-spec get_timestamp() -> {ok, traverse:timestamp()}.
get_timestamp() ->
    {ok, provider_logic:zone_time_seconds()}.

%%%===================================================================
%%% Internal functions
%%%===================================================================

-spec get_child_job(master_job(), file_meta:doc()) -> master_job().
get_child_job(#tree_travserse{
    execute_slave_on_dir = OnDir,
    batch_size = BatchSize,
    traverse_info = TraverseInfo
}, ChildDoc) ->
    #tree_travserse{
        doc = ChildDoc,
        execute_slave_on_dir = OnDir,
        batch_size = BatchSize,
        traverse_info = TraverseInfo
    }.