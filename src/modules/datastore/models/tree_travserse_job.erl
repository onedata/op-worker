%%%-------------------------------------------------------------------
%%% @author Michal Wrzeszcz
%%% @copyright (C) 2019 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc Model for holding traverse tasks.
%%% @end
%%%-------------------------------------------------------------------
-module(tree_travserse_job).
-author("Michal Wrzeszcz").

-include("modules/datastore/datastore_models.hrl").

%% API
-export([save/9, get/1]).

%% datastore_model callbacks
-export([get_ctx/0, get_record_struct/1]).

-type key() :: datastore:key().
-type record() :: #tree_travserse_job{}.
-type doc() :: datastore_doc:doc(record()).

% TODO - trzymamy drzewo ongoing jobow
% TODO - synchronizujemy tylko main taski
-define(CTX, #{
    model => ?MODULE,
    sync_enabled => true,
    remote_driver => datastore_remote_driver,
    mutator => oneprovider:get_id_or_undefined(),
    local_links_tree_id => oneprovider:get_id_or_undefined()
}).

%%%===================================================================
%%% API
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%%
%% @end
%%--------------------------------------------------------------------
%%-spec save(doc()) -> {ok, doc()} | {error, term()}.
save(Key, TaskID, DocID, Scope, LastName, LastTree, OnDir, BatchSize, TraverseInfo) ->
    Doc = #document{key = Key, scope = Scope,
        value = #tree_travserse_job{task_id = TaskID, doc_id = DocID, last_name = LastName, last_tree = LastTree,
        execute_slave_on_dir = OnDir, batch_size = BatchSize, traverse_info = term_to_binary(TraverseInfo)}},
    datastore_model:save(?CTX, Doc).

%%--------------------------------------------------------------------
%% @doc
%%
%% @end
%%--------------------------------------------------------------------
%%-spec get(key()) -> {ok, doc()} | {error, term()}.
get(Key) ->
    case datastore_model:get(?CTX, Key) of
        {ok, #document{value = #tree_travserse_job{task_id = TaskID, doc_id = DocID, last_name = LastName,
            last_tree = LastTree, execute_slave_on_dir = OnDir, batch_size = BatchSize,
            traverse_info = TraverseInfo}}} ->
            {ok, TaskID, DocID, LastName, LastTree, OnDir, BatchSize, binary_to_term(TraverseInfo)};
        Other ->
            Other
    end.

%%%===================================================================
%%% datastore_model callbacks
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Returns model's context.
%% @end
%%--------------------------------------------------------------------
-spec get_ctx() -> datastore:ctx().
get_ctx() ->
    ?CTX.

%%--------------------------------------------------------------------
%% @doc
%% Returns model's record structure in provided version.
%% @end
%%--------------------------------------------------------------------
-spec get_record_struct(datastore_model:record_version()) ->
    datastore_model:record_struct().
get_record_struct(1) ->
    {record, [
        {task_id, string},
        {doc_id, string},
        {last_name, string},
        {last_tree, string},
        {execute_slave_on_dir, boolean},
        {batch_size, integer},
        {traverse_info, binary}
    ]}.