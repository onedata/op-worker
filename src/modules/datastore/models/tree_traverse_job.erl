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

-include("modules/datastore/datastore_models.hrl").
-include("modules/datastore/datastore_runner.hrl").

%% API
-export([save/11, delete/2, get/1]).

%% datastore_model callbacks
-export([get_ctx/0, get_record_struct/1]).

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
-spec save(datastore:key() | main_job, datastore_doc:scope(), traverse:pool(), traverse:callback_module(),
    traverse:id(), file_meta:uuid(), file_meta:name(), od_provider:id(), tree_traverse:execute_slave_on_dir(),
    tree_traverse:batch_size(), tree_traverse:traverse_info()) -> {ok, key()} | {error, term()}.
save(Key, Scope, Pool, CallbackModule, TaskID, DocID, LastName, LastTree, OnDir, BatchSize, TraverseInfo) ->
    Value = get_record(Pool, CallbackModule, TaskID, DocID, LastName, LastTree, OnDir, BatchSize, TraverseInfo),
    save(Key, Scope, Value).

-spec delete(datastore:key(), datastore_doc:scope()) -> ok | {error, term()}.
delete(<<?MAIN_JOB_PREFIX, _>> = Key, Scope) ->
    datastore_model:delete(?SYNC_CTX#{scope => Scope}, Key);
delete(Key, _) ->
    datastore_model:delete(?CTX, Key).

-spec get(key() | doc()) -> {ok, traverse:pool(), traverse:callback_module(), traverse:id(), file_meta:uuid(),
    file_meta:name(), od_provider:id(), tree_traverse:execute_slave_on_dir(), tree_traverse:batch_size(),
    tree_traverse:traverse_info()} | {error, term()}.
get(#document{value = #tree_traverse_job{pool = Pool, callback_module = CallbackModule, task_id = TaskID,
    doc_id = DocID, last_name = LastName, last_tree = LastTree, execute_slave_on_dir = OnDir,
    batch_size = BatchSize, traverse_info = TraverseInfo}}) ->
    {ok, Pool, CallbackModule, TaskID, DocID, LastName, LastTree, OnDir, BatchSize, binary_to_term(TraverseInfo)};
get(Key) ->
    case datastore_model:get(?CTX, Key) of
        {ok, Doc} ->
            ?MODULE:get(Doc);
        Other ->
            Other
    end.

%%%===================================================================
%%% datastore_model callbacks
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Returns model's context for documents that are not synced.
%% @end
%%--------------------------------------------------------------------
-spec get_ctx() -> datastore:ctx().
get_ctx() ->
    ?CTX.

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
    ]}.

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
-spec save(datastore:key() | main_job, datastore_doc:scope(), record()) -> {ok, key()} | {error, term()}.
save(main_job, Scope, Value) ->
    RandomPart = datastore_utils:gen_key(),
    GenKey = <<?MAIN_JOB_PREFIX, RandomPart/binary>>,
    ?extract_key(datastore_model:save(?SYNC_CTX#{generated_key => true},
        #document{key = GenKey, scope = Scope, value = Value}));
save(<<?MAIN_JOB_PREFIX, _>> = Key, Scope, Value) ->
    ?extract_key(datastore_model:save(?SYNC_CTX, #document{key = Key, scope = Scope, value = Value}));
save(Key, _, Value) ->
    ?extract_key(datastore_model:save(?CTX, #document{key = Key, value = Value})).

-spec get_record(traverse:pool(), traverse:callback_module(), traverse:id(), file_meta:uuid(), file_meta:name(),
    od_provider:id(), tree_traverse:execute_slave_on_dir(), tree_traverse:batch_size(),
    tree_traverse:traverse_info()) -> record().
get_record(Pool, CallbackModule, TaskID, DocID, LastName, LastTree, OnDir, BatchSize, TraverseInfo) ->
    #tree_traverse_job{pool = Pool, callback_module = CallbackModule, task_id = TaskID, doc_id = DocID,
            last_name = LastName, last_tree = LastTree, execute_slave_on_dir = OnDir, batch_size = BatchSize,
            traverse_info = term_to_binary(TraverseInfo)}.