%%%-------------------------------------------------------------------
%%% @author Rafal Slota
%%% @copyright (C) 2016 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% Persistent state of DBSync worker.
%%% @end
%%%-------------------------------------------------------------------
-module(dbsync_state).
-author("Rafal Slota").
-behaviour(model_behaviour).

-include("modules/datastore/datastore_specific_models_def.hrl").
-include_lib("cluster_worker/include/modules/datastore/datastore_model.hrl").

%% model_behaviour callbacks
-export([save/1, get/1, list/0, exists/1, delete/1, update/2, create/1,
    model_init/0, 'after'/5, before/4]).
-export([record_struct/1]).

%%--------------------------------------------------------------------
%% @doc
%% Returns structure of the record in specified version.
%% @end
%%--------------------------------------------------------------------
-spec record_struct(datastore_json:record_version()) -> datastore_json:record_struct().
record_struct(1) ->
    {record, [
        {entry, term}
    ]}.

%%%===================================================================
%%% model_behaviour callbacks
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% {@link model_behaviour} callback save/1.
%% @end
%%--------------------------------------------------------------------
-spec save(datastore:document()) -> {ok, datastore:ext_key()} | datastore:generic_error().
save(#document{} = Document) ->
    datastore:save(?STORE_LEVEL, Document).

%%--------------------------------------------------------------------
%% @doc
%% {@link model_behaviour} callback update/2.
%% @end
%%--------------------------------------------------------------------
-spec update(datastore:ext_key(), Diff :: datastore:document_diff()) ->
    {ok, datastore:ext_key()} | datastore:update_error().
update(Key, Diff) ->
    datastore:update(?STORE_LEVEL, ?MODULE, Key, Diff).

%%--------------------------------------------------------------------
%% @doc
%% {@link model_behaviour} callback create/1.
%% @end
%%--------------------------------------------------------------------
-spec create(datastore:document()) -> {ok, datastore:ext_key()} | datastore:create_error().
create(#document{} = Document) ->
    datastore:create(?STORE_LEVEL, Document).

%%--------------------------------------------------------------------
%% @doc
%% {@link model_behaviour} callback get/1.
%% Sets access time to current time for user session and returns old value.
%% @end
%%--------------------------------------------------------------------
-spec get(datastore:ext_key()) -> {ok, datastore:document()} | datastore:get_error().
get(Key) ->
    datastore:get(?STORE_LEVEL, ?MODULE, Key).

%%--------------------------------------------------------------------
%% @doc
%% Returns list of all records.
%% @end
%%--------------------------------------------------------------------
-spec list() -> {ok, [datastore:document()]} | datastore:generic_error() | no_return().
list() ->
    datastore:list(?STORE_LEVEL, ?MODEL_NAME, ?GET_ALL, []).

%%--------------------------------------------------------------------
%% @doc
%% {@link model_behaviour} callback delete/1.
%% @end
%%--------------------------------------------------------------------
-spec delete(datastore:ext_key()) -> ok | datastore:generic_error().
delete(Key) ->
    datastore:delete(?STORE_LEVEL, ?MODULE, Key).

%%--------------------------------------------------------------------
%% @doc
%% {@link model_behaviour} callback exists/1.
%% @end
%%--------------------------------------------------------------------
-spec exists(datastore:ext_key()) -> datastore:exists_return().
exists(Key) ->
    ?RESPONSE(datastore:exists(?STORE_LEVEL, ?MODULE, Key)).

%%--------------------------------------------------------------------
%% @doc
%% {@link model_behaviour} callback model_init/0.
%% @end
%%--------------------------------------------------------------------
-spec model_init() -> model_behaviour:model_config().
model_init() ->
    ?MODEL_CONFIG(dbsync_bucket, [{file_meta, delete}, {file_meta, delete_links}, {times, delete},
        {custom_metadata, delete}, {file_location, delete}, {change_propagation_controller, delete},
        {change_propagation_controller, delete_links}], ?LOCALLY_CACHED_LEVEL).

%%--------------------------------------------------------------------
%% @doc
%% {@link model_behaviour} callback 'after'/5.
%% @end
%%--------------------------------------------------------------------
-spec 'after'(ModelName :: model_behaviour:model_type(), Method :: model_behaviour:model_action(),
    Level :: datastore:store_level(), Context :: term(),
    ReturnValue :: term()) -> ok.
'after'(change_propagation_controller = ModelName, delete, _Level, [Key, _Pred], _ReturnValue) ->
    verify_and_del_key(Key, ModelName);
'after'(_ModelName, delete, _Level, [Key, _Pred], _ReturnValue) ->
    verify_and_del_key(Key, file_meta);
'after'(ModelName, delete_links, _Level, [Key, _Links], _ReturnValue) ->
    verify_and_del_key(Key, ModelName);
'after'(_ModelName, _Method, _Level, _Context, _ReturnValue) ->
    ok.

%%--------------------------------------------------------------------
%% @doc
%% {@link model_behaviour} callback before/4.
%% @end
%%--------------------------------------------------------------------
-spec before(ModelName :: model_behaviour:model_type(), Method :: model_behaviour:model_action(),
    Level :: datastore:store_level(), Context :: term()) -> ok | datastore:generic_error().
before(file_meta = ModelName, delete, ?DISK_ONLY_LEVEL, [Key, _Pred]) ->
    save_space_id(ModelName, Key);
before(change_propagation_controller = ModelName, delete, ?DISK_ONLY_LEVEL, [Key, _Pred]) ->
    save_space_id(ModelName, Key);
before(_ModelName, _Method, _Level, _Context) ->
    ok.

%%%===================================================================
%%% Internal functions
%%%===================================================================

verify_and_del_key(Key, change_propagation_controller = ModelName) ->
    Checks = [{change_propagation_controller, exists_link_doc}],
    verify_and_del_key(Key, ModelName, Checks);
verify_and_del_key(Key, ModelName) ->
    Checks = [{file_meta, exists_link_doc}, {times, exists},
        {custom_metadata, exists}, {file_location, exists}],
    verify_and_del_key(Key, ModelName, Checks).

verify_and_del_key(Key, ModelName, Checks) ->
    VerAns = lists:foldl(fun
        ({ModelName, Op}, ok) ->
            case erlang:apply(datastore:driver_to_module(datastore:level_to_driver(disk_only)),
                Op, [ModelName:model_init(), Key]) of
                {ok, false} ->
                    ok;
                _ ->
                    cannot_clear
            end;
        (_, Acc) ->
            Acc
    end, ok, Checks),

    case VerAns of
        ok ->
            delete({sid, ModelName, Key});
        _ ->
            ok
    end.

save_space_id(ModelName, Key) ->
    case erlang:apply(datastore:driver_to_module(datastore:level_to_driver(disk_only)),
        get, [ModelName:model_init(), Key]) of
        {ok, Doc} ->
            {ok, SID} = dbsync_worker:get_space_id(Doc),
            {ok, _} = save(#document{key = {sid, ModelName, Key}, value = #dbsync_state{entry = SID}}),
            ok;
        {error, {not_found, _}} ->
            ok;
        Other ->
            {prehook_error, Other}
    end.