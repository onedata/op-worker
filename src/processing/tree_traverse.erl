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
-export([run/7]).
%% Behaviour callbacks
-export([do_master_job/1, save_job/2]).

-record(tree_travserse_job, {
    doc :: file_meta:doc(),
    token = #link_token{} :: datastore_links_iter:token(),
    last_name = 0 :: non_neg_integer(),
    execute_slave_on_dir :: boolean(),
    batch_size :: non_neg_integer()
}).

-type master_job() :: #tree_travserse_job{}.
-type slave_job() :: file_meta:doc().

%%%===================================================================
%%% API
%%%===================================================================

run(#document{} = Doc, Pool, TaskID, TaskGroup, TaskModule, ExecuteActionOnDir, BatchSize) ->
    traverse:run(Pool, TaskID, TaskGroup, TaskModule, #tree_travserse_job{
        doc = Doc,
        execute_slave_on_dir = ExecuteActionOnDir,
        batch_size = BatchSize
    });
run(FileCtx, Pool, TaskID, TaskGroup, TaskModule, ExecuteActionOnDir, BatchSize) ->
    {Doc, _} = file_ctx:get_file_doc(FileCtx),
    run(Doc, Pool, TaskID, TaskGroup, TaskModule, ExecuteActionOnDir, BatchSize).

%%%===================================================================
%%% Behaviour callbacks
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Does master job that traverse directory tree.
%% @end
%%--------------------------------------------------------------------
-spec do_master_job(master_job()) -> {ok, [slave_job()], [master_job()]}.
do_master_job(#tree_travserse_job{
    doc = #document{value = #file_meta{type = ?DIRECTORY_TYPE}} = Doc,
    token = Token,
    last_name = LN,
    execute_slave_on_dir = OnDir,
    batch_size = BatchSize
}) ->
    % TODO - list by key - not token
    {ok, Children, NewToken} = file_meta:list_children(Doc, LN, BatchSize, Token),

    {SlaveJobs, MasterJobs, LN2} = lists:foldl(fun(#child_link_uuid{
        uuid = UUID,
        name = Name}, {Slaves, Masters, _} = Acc) ->
        case {file_meta:get({uuid, UUID}), OnDir} of
            {{ok, #document{value = #file_meta{type = ?DIRECTORY_TYPE}} = ChildDoc}, true} ->
                {[ChildDoc | Slaves], [#tree_travserse_job{doc = ChildDoc,
                    execute_slave_on_dir = OnDir, batch_size = BatchSize} | Masters], Name};
            {{ok, #document{value = #file_meta{type = ?DIRECTORY_TYPE}} = ChildDoc}, _} ->
                {Slaves, [#tree_travserse_job{doc = ChildDoc,
                    execute_slave_on_dir = OnDir, batch_size = BatchSize} | Masters], Name};
            {{ok, ChildDoc}, _} ->
                {[ChildDoc | Slaves], Masters, Name};
            {{error, not_found}, _} ->
                Acc
        end
    end, {[], [], undefined}, Children),

    case NewToken#link_token.is_last of
        true -> {ok, lists:reverse(SlaveJobs), lists:reverse(MasterJobs)};
        false -> {ok, lists:reverse(SlaveJobs), [#tree_travserse_job{
            doc = Doc,
            token = NewToken,
            last_name = LN2,
            execute_slave_on_dir = OnDir,
            batch_size = BatchSize
        } | lists:reverse(MasterJobs)]}
    end;
do_master_job(#tree_travserse_job{
    doc = Doc
}) ->
    {ok, [Doc], []}.

%%--------------------------------------------------------------------
%% @doc
%% Saves information about job progress.
%% @end
%%--------------------------------------------------------------------
-spec save_job(master_job(), traverse:job_status()) -> ok  | {error, term()}.
save_job(_, _) ->
    ok.
