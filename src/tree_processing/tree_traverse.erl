%%%-------------------------------------------------------------------
%%% @author Michal Wrzeszcz
%%% @copyright (C) 2019 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% This module provides base functionality for directory tree traversing.
%%% It bases on traverse framework (see traverse.erl in cluster_worker).
%%% The module provides functions for pool and tasks start and implementation of some callbacks.
%%% To use tree traverse, new callback module has to be defined (see traverse_behaviour.erl from cluster_worker) that
%%% uses callbacks defined in this module and additionally provides do_slave_job function implementation. Next,
%%% pool and tasks are started using init and run functions from this module.
%%% The traverse jobs (see traverse.erl for jobs definition) are persisted using tree_traverse_job datastore model
%%% which stores jobs locally and synchronizes main job for each task between providers (to allow tasks execution
%%% on other provider resources).
%%% @end
%%%-------------------------------------------------------------------
-module(tree_traverse).
-author("Michal Wrzeszcz").

% This module is a base for traverse callback
% (other callbacks especially for slave jobs have to be defined)
%-behaviour(traverse_behaviour).

-include("proto/oneclient/fuse_messages.hrl").
-include("modules/datastore/datastore_models.hrl").
-include_lib("cluster_worker/include/modules/datastore/datastore_links.hrl").

%% Main API
-export([init/4, init/5, run/2, run/3]).
% Getters API
-export([get_traverse_info/1, get_doc/1, get_task/2]).
%% Behaviour callbacks
-export([do_master_job/1, update_job_progress/6, get_job/1, get_sync_info/1, get_timestamp/0]).

% Record that defines master job
-record(tree_travserse, {
    % File or directory processed by job
    doc :: file_meta:doc(),
    % Fields used for directory listing
    token :: datastore_links_iter:token() | undefined,
    last_name = <<>> :: file_meta:name(),
    last_tree = <<>> :: od_provider:id(),
    batch_size :: batch_size(),
    % Traverse config
    execute_slave_on_dir :: execute_slave_on_dir(), % generate slave jobs also for directories
    traverse_info :: traverse_info() % info passed to every slave job
}).

-type master_job() :: #tree_travserse{}.
-type slave_job() :: file_meta:doc().
-type execute_slave_on_dir() :: boolean().
-type batch_size() :: non_neg_integer().
-type traverse_info() :: term().
-type run_options() :: #{
    % Options of traverse framework
    task_id => traverse:id(),
    callback_module => traverse:callback_module(),
    group_id => traverse:group(),
    % Options used to create jobs
    execute_slave_on_dir => execute_slave_on_dir(),
    batch_size => batch_size(),
    traverse_info => traverse_info(),
    % Provider which should execute task
    target_provider_id => oneprovider:id()
}.

-export_type([slave_job/0, execute_slave_on_dir/0, batch_size/0, traverse_info/0]).

%%%===================================================================
%%% Main API
%%%===================================================================

-spec init(traverse:pool() | atom(), non_neg_integer(), non_neg_integer(), non_neg_integer()) -> ok.
init(Pool, MasterJobsNum, SlaveJobsNum, ParallelOrdersLimit) when is_atom(Pool) ->
    init(atom_to_binary(Pool, utf8), MasterJobsNum, SlaveJobsNum, ParallelOrdersLimit);
init(Pool, MasterJobsNum, SlaveJobsNum, ParallelOrdersLimit) ->
    traverse:init_pool(Pool, MasterJobsNum, SlaveJobsNum, ParallelOrdersLimit,
        #{executor => oneprovider:get_id_or_undefined()}).

-spec init(traverse:pool() | atom(), non_neg_integer(), non_neg_integer(), non_neg_integer(),
    [traverse:callback_module()]) -> ok.
init(Pool, MasterJobsNum, SlaveJobsNum, ParallelOrdersLimit, CallbackModules) when is_atom(Pool) ->
    init(atom_to_binary(Pool, utf8), MasterJobsNum, SlaveJobsNum, ParallelOrdersLimit, CallbackModules);
init(Pool, MasterJobsNum, SlaveJobsNum, ParallelOrdersLimit, CallbackModules) ->
    traverse:init_pool(Pool, MasterJobsNum, SlaveJobsNum, ParallelOrdersLimit,
        #{executor => oneprovider:get_id_or_undefined(), callback_modules => CallbackModules}).

-spec run(traverse:pool() | atom(), file_meta:doc() | file_ctx:ctx()) -> {ok, traverse:id()}.
run(Pool, DocOrCtx) ->
    run(Pool, DocOrCtx, #{}).

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
%% Provides file_meta document from master job record.
%% @end
%%--------------------------------------------------------------------
-spec get_doc(master_job()) -> file_meta:job().
get_doc(#tree_travserse{doc = Doc}) ->
    Doc.

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
%% Does master job that traverse directory tree. The job lists directory (number of listed children is limited) and
%% returns jobs for listed children and next batch if needed.
%% @end
%%--------------------------------------------------------------------
-spec do_master_job(master_job()) -> {ok, traverse:master_job_map()}.
% TODO - moze dac mozliwosc zmiany traverse info?
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
%% Updates information about master jobs saving it in datastore or deleting if it is finished or canceled.
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
    tree_traverse_job:save(ID, Scope, Pool, CallbackModule, TaskID, DocID, LN, LT,
        OnDir, BatchSize, TraverseInfo);
update_job_progress(ID, #tree_travserse{doc = #document{scope = Scope}}, _, _, _, _) ->
    ok = tree_traverse_job:delete(ID, Scope),
    {ok, ID}.

-spec get_job(traverse:job_id() | tree_traverse_job:doc()) ->
    {ok, master_job(), traverse:pool(), traverse:id()}  | {error, term()}.
get_job(DocOrID) ->
    case tree_traverse_job:get(DocOrID) of
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
%% Provides information needed for task document synchronization basing on file_meta scope.
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