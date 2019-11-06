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

-include("tree_traverse.hrl").
-include("proto/oneclient/fuse_messages.hrl").
-include("modules/datastore/datastore_models.hrl").
-include_lib("cluster_worker/include/modules/datastore/datastore_links.hrl").

%% Main API
-export([init/4, init/5, run/2, run/3, cancel/2]).
% Getters API
-export([get_traverse_info/1, set_traverse_info/2, get_doc/1, get_task/2, get_sync_info/0]).
%% Behaviour callbacks
-export([do_master_job/2, update_job_progress/6, get_job/1, get_sync_info/1, get_timestamp/0]).

-type master_job() :: #tree_traverse{}.
-type slave_job() :: file_meta:doc().
-type execute_slave_on_dir() :: boolean().
-type batch_size() :: non_neg_integer().
-type traverse_info() :: term().
-type run_options() :: #{
    % Options of traverse framework
    task_id => traverse:id(),
    callback_module => traverse:callback_module(),
    group_id => traverse:group(),
    additional_data => traverse:additional_data(),
    % Options used to create jobs
    execute_slave_on_dir => execute_slave_on_dir(),
    batch_size => batch_size(),
    traverse_info => traverse_info(),
    % Provider which should execute task
    target_provider_id => oneprovider:id(),
    additional_data => traverse:additional_data()
}.

-export_type([master_job/0, slave_job/0, execute_slave_on_dir/0, batch_size/0, traverse_info/0]).

%%%===================================================================
%%% Main API
%%%===================================================================

-spec init(traverse:pool() | atom(), non_neg_integer(), non_neg_integer(), non_neg_integer()) -> ok | no_return().
init(Pool, MasterJobsNum, SlaveJobsNum, ParallelOrdersLimit) when is_atom(Pool) ->
    init(atom_to_binary(Pool, utf8), MasterJobsNum, SlaveJobsNum, ParallelOrdersLimit);
init(Pool, MasterJobsNum, SlaveJobsNum, ParallelOrdersLimit) ->
    traverse:init_pool(Pool, MasterJobsNum, SlaveJobsNum, ParallelOrdersLimit,
        #{executor => oneprovider:get_id_or_undefined()}).

-spec init(traverse:pool() | atom(), non_neg_integer(), non_neg_integer(), non_neg_integer(),
    [traverse:callback_module()]) -> ok  | no_return().
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
    RunOpts4 = case maps:get(additional_data, Opts, undefined) of
        undefined -> RunOpts3;
        AdditionalData -> RunOpts3#{additional_data => AdditionalData}
    end,

    ok = traverse:run(Pool, TaskID, #tree_traverse{
        doc = Doc,
        execute_slave_on_dir = ExecuteActionOnDir,
        batch_size = BatchSize,
        traverse_info = TraverseInfo
    }, RunOpts4),
    {ok, TaskID};
run(Pool, FileCtx, Opts) ->
    {Doc, _} = file_ctx:get_file_doc(FileCtx),
    run(Pool, Doc, Opts).

-spec cancel(traverse:pool() | atom(), traverse:id()) -> ok | {error, term()}.
cancel(Pool, TaskID) when is_atom(Pool) ->
    cancel(atom_to_binary(Pool, utf8), TaskID);
cancel(Pool, TaskID) ->
    traverse:cancel(Pool, TaskID, oneprovider:get_id_or_undefined()).

%%%===================================================================
%%% Getters/Setters API
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Provides traverse info from master job record.
%% @end
%%--------------------------------------------------------------------
-spec get_traverse_info(master_job()) -> traverse_info().
get_traverse_info(#tree_traverse{traverse_info = TraverseInfo}) ->
    TraverseInfo.

%%--------------------------------------------------------------------
%% @doc
%% Sets traverse info in master job record.
%% @end
%%--------------------------------------------------------------------
-spec set_traverse_info(master_job(), traverse_info()) -> master_job().
set_traverse_info(TraverseJob, TraverseInfo) ->
    TraverseJob#tree_traverse{traverse_info = TraverseInfo}.

%%--------------------------------------------------------------------
%% @doc
%% Provides file_meta document from master job record.
%% @end
%%--------------------------------------------------------------------
-spec get_doc(master_job()) -> file_meta:doc().
get_doc(#tree_traverse{doc = Doc}) ->
    Doc.

-spec get_task(traverse:pool() | atom(), traverse:id()) -> {ok, traverse_task:doc()} | {error, term()}.
get_task(Pool, ID) when is_atom(Pool) ->
    get_task(atom_to_binary(Pool, utf8), ID);
get_task(Pool, ID) ->
    traverse_task:get(Pool, ID).

%%--------------------------------------------------------------------
%% @doc
%% Provides information needed for document synchronization.
%% Warning: if any traverse callback module uses other sync info than one provided by tree_traverse,
%% dbsync_changes:get_ctx function has to be extended to parse #document and get callback module.
%% @end
%%--------------------------------------------------------------------
-spec get_sync_info() -> traverse:sync_info().
get_sync_info() ->
    Provider = oneprovider:get_id_or_undefined(),
    #{
        sync_enabled => true,
        remote_driver => datastore_remote_driver,
        mutator => Provider,
        local_links_tree_id => Provider
    }.

%%%===================================================================
%%% Behaviour callbacks
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Does master job that traverse directory tree. The job lists directory (number of listed children is limited) and
%% returns jobs for listed children and next batch if needed.
%% @end
%%--------------------------------------------------------------------
-spec do_master_job(master_job(), traverse:id()) -> {ok, traverse:master_job_map()}.
do_master_job(#tree_traverse{
    doc = #document{value = #file_meta{type = ?DIRECTORY_TYPE}} = Doc,
    token = Token,
    last_name = LN,
    last_tree = LT,
    execute_slave_on_dir = OnDir,
    batch_size = BatchSize,
    traverse_info = TraverseInfo
} = TT, _TaskID) ->
    {ok, Children, ExtendedInfo} = case {Token, LN} of
        {undefined, <<>>} ->
            file_meta:list_children(Doc, BatchSize);
        _ ->
            file_meta:list_children(Doc, 0, BatchSize, Token, LN, LT)
    end,
    #{token := Token2, last_name := LN2, last_tree := LT2} = maps:merge(#{token => undefined}, ExtendedInfo),

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

    FinalMasterJobs = case (Token2 =/= undefined andalso Token2#link_token.is_last) or (Children =:= []) of
        true -> lists:reverse(MasterJobs);
        false -> [TT#tree_traverse{
            token = Token2,
            last_name = LN2,
            last_tree = LT2
        } | lists:reverse(MasterJobs)]
    end,
    {ok, #{slave_jobs => lists:reverse(SlaveJobs), master_jobs => FinalMasterJobs}};
do_master_job(#tree_traverse{
    doc = Doc,
    traverse_info = TraverseInfo
}, _TaskID) ->
    {ok, #{slave_jobs => [{Doc, TraverseInfo}], master_jobs => []}}.

%%--------------------------------------------------------------------
%% @doc
%% Updates information about master jobs saving it in datastore or deleting if it is finished or canceled.
%% @end
%%--------------------------------------------------------------------
-spec update_job_progress(undefined | main_job | traverse:job_id(),
    master_job(), traverse:pool(), traverse:id(), traverse:job_status(), traverse:callback_module()) ->
    {ok, traverse:job_id()}  | {error, term()}.
update_job_progress(ID, Job, Pool, TaskID, Status, CallbackModule) when Status =:= waiting ; Status =:= on_pool ->
    tree_traverse_job:save_master_job(ID, Job, Pool, TaskID, CallbackModule);
update_job_progress(ID, #tree_traverse{doc = #document{scope = Scope}}, _, _, _, _) ->
    ok = tree_traverse_job:delete_master_job(ID, Scope),
    {ok, ID}.

-spec get_job(traverse:job_id() | tree_traverse_job:doc()) ->
    {ok, master_job(), traverse:pool(), traverse:id()}  | {error, term()}.
get_job(DocOrID) ->
    tree_traverse_job:get_master_job(DocOrID).

%%--------------------------------------------------------------------
%% @doc
%% Provides information needed for task document synchronization basing on file_meta scope.
%% @end
%%--------------------------------------------------------------------
-spec get_sync_info(master_job()) -> {ok, traverse:sync_info()}.
get_sync_info(#tree_traverse{
    doc = #document{scope = Scope}
}) ->
    Info = get_sync_info(),
    {ok, Info#{scope => Scope}}.

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
get_child_job(#tree_traverse{
    execute_slave_on_dir = OnDir,
    batch_size = BatchSize,
    traverse_info = TraverseInfo
}, ChildDoc) ->
    #tree_traverse{
        doc = ChildDoc,
        execute_slave_on_dir = OnDir,
        batch_size = BatchSize,
        traverse_info = TraverseInfo
    }.