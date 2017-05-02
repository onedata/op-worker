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
-include_lib("cluster_worker/include/modules/datastore/datastore_common_internal.hrl").
-include_lib("cluster_worker/include/modules/datastore/datastore_engine.hrl").

%% model_behaviour callbacks
-export([save/1, get/1, list/0, exists/1, delete/1, update/2, create/1,
    model_init/0, 'after'/5, before/4]).
-export([record_struct/1]).

-export([verify_and_del_key/2, sid_doc_key/2]).

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
-spec save(datastore:document()) ->
    {ok, datastore:ext_key()} | datastore:generic_error().
save(Document) ->
    model:execute_with_default_context(?MODULE, save, [Document]).

%%--------------------------------------------------------------------
%% @doc
%% {@link model_behaviour} callback update/2.
%% @end
%%--------------------------------------------------------------------
-spec update(datastore:ext_key(), Diff :: datastore:document_diff()) ->
    {ok, datastore:ext_key()} | datastore:update_error().
update(Key, Diff) ->
    model:execute_with_default_context(?MODULE, update, [Key, Diff]).

%%--------------------------------------------------------------------
%% @doc
%% {@link model_behaviour} callback create/1.
%% @end
%%--------------------------------------------------------------------
-spec create(datastore:document()) ->
    {ok, datastore:ext_key()} | datastore:create_error().
create(Document) ->
    model:execute_with_default_context(?MODULE, create, [Document]).

%%--------------------------------------------------------------------
%% @doc
%% {@link model_behaviour} callback get/1.
%% @end
%%--------------------------------------------------------------------
-spec get(datastore:ext_key()) -> {ok, datastore:document()} | datastore:get_error().
get(Key) ->
    model:execute_with_default_context(?MODULE, get, [Key]).

%%--------------------------------------------------------------------
%% @doc
%% Returns list of all records.
%% @end
%%--------------------------------------------------------------------
-spec list() -> {ok, [datastore:document()]} | datastore:generic_error() | no_return().
list() ->
    model:execute_with_default_context(?MODULE, list, [?GET_ALL, []]).

%%--------------------------------------------------------------------
%% @doc
%% {@link model_behaviour} callback delete/1.
%% @end
%%--------------------------------------------------------------------
-spec delete(datastore:ext_key()) -> ok | datastore:generic_error().
delete(Key) ->
    model:execute_with_default_context(?MODULE, delete, [Key]).

%%--------------------------------------------------------------------
%% @doc
%% {@link model_behaviour} callback exists/1.
%% @end
%%--------------------------------------------------------------------
-spec exists(datastore:ext_key()) -> datastore:exists_return().
exists(Key) ->
    ?RESPONSE(model:execute_with_default_context(?MODULE, exists, [Key])).

%%--------------------------------------------------------------------
%% @doc
%% {@link model_behaviour} callback model_init/0.
%% @end
%%--------------------------------------------------------------------
-spec model_init() -> model_behaviour:model_config().
model_init() ->
    ?MODEL_CONFIG(dbsync_bucket, [{file_meta, delete}, {file_meta, delete_links}, {times, delete},
        {custom_metadata, delete}, {file_location, delete}, {change_propagation_controller, delete},
        {change_propagation_controller, delete_links}], ?GLOBALLY_CACHED_LEVEL).

%%--------------------------------------------------------------------
%% @doc
%% {@link model_behaviour} callback 'after'/5.
%% @end
%%--------------------------------------------------------------------
-spec 'after'(ModelName :: model_behaviour:model_type(), Method :: model_behaviour:model_action(),
    Level :: datastore:store_level(), Context :: term(),
    ReturnValue :: term()) -> ok.
'after'(change_propagation_controller = ModelName, delete, _, [Key, _Pred], _ReturnValue) ->
    verify_and_del_key(Key, ModelName);
% TODO - delete old state
%%'after'(_ModelName, delete, ?DISK_ONLY_LEVEL, [Key, _Pred], _ReturnValue) ->
%%    verify_and_del_key(Key, file_meta);
%%'after'(ModelName, delete_links, ?DISK_ONLY_LEVEL, [Key, _Links], _ReturnValue) ->
%%    verify_and_del_key(Key, ModelName);
'after'(_ModelName, _Method, _Level, _Context, _ReturnValue) ->
    ok.

%%--------------------------------------------------------------------
%% @doc
%% {@link model_behaviour} callback before/4.
%% @end
%%--------------------------------------------------------------------
-spec before(ModelName :: model_behaviour:model_type(), Method :: model_behaviour:model_action(),
    Level :: datastore:store_level(), Context :: term()) -> ok | datastore:generic_error().
before(file_meta = ModelName, delete, ?GLOBAL_ONLY_LEVEL, [Key, _Pred]) ->
    save_space_id(ModelName, Key);
before(change_propagation_controller = ModelName, delete, ?GLOBAL_ONLY_LEVEL, [Key, _Pred]) ->
    save_space_id(ModelName, Key);
before(_ModelName, _Method, _Level, _Context) ->
    ok.

%%--------------------------------------------------------------------
%% @doc
%% Verifies if key may be deleted and deletes it.
%% @end
%%--------------------------------------------------------------------
-spec verify_and_del_key(Key :: datastore:ext_key(), ModelName :: model_behaviour:model_type()) -> ok.
verify_and_del_key(Key, change_propagation_controller = ModelName) ->
    Checks = [{change_propagation_controller, foreach_link}],
    verify_and_del_key(Key, ModelName, Checks);
verify_and_del_key(Key, file_meta = ModelName) ->
    Checks = [{file_meta, foreach_link}, {times, exists},
        {custom_metadata, exists}, {file_location, exists}],
    verify_and_del_key(Key, ModelName, Checks);
verify_and_del_key(_Key, _ModelName) ->
    ok.

%%%===================================================================
%%% Internal functions
%%%===================================================================

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Verifies if key may be deleted and deletes it.
%% @end
%%--------------------------------------------------------------------
-spec verify_and_del_key(Key :: datastore:ext_key(), ModelName :: model_behaviour:model_type(),
    ToCheck :: list()) -> ok.
verify_and_del_key(Key, ModelName, Checks) ->
    VerAns = lists:foldl(fun
        ({ModelName, foreach_link}, ok) ->
            HelperFun = fun(LinkName, LinkTarget, Acc) ->
                maps:put(LinkName, LinkTarget, Acc)
            end,

            case erlang:apply(datastore:driver_to_module(datastore:level_to_driver(?DISK_ONLY_LEVEL)),
                foreach_link, [ModelName:model_init(), Key, HelperFun, #{}]) of
                {ok, #{}} ->
                    ok;
                _ ->
                    cannot_clear
            end;
        ({ModelName, Op}, ok) ->
            case erlang:apply(datastore:driver_to_module(datastore:level_to_driver(?DISK_ONLY_LEVEL)),
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
            spawn(fun() ->
                timer:sleep(timer:minutes(5)),
                delete(sid_doc_key(ModelName, Key))
            end),
            ok;
        _ ->
            ok
    end.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Checks and saves space id.
%% @end
%%--------------------------------------------------------------------
-spec save_space_id(ModelName :: model_behaviour:model_type(), Key :: datastore:ext_key()) ->
    ok | {prehook_error, datastore:generic_error()}.
save_space_id(ModelName, Key) ->
    MInit = ModelName:model_init(),
    % Use data store driver explicit (otherwise hooks loop will appear)
    GetAns = model:execute_with_default_context(MInit, get,
        [Key], [{hooks_config, no_hooks}]),

    case GetAns of
        {ok, Doc} ->
            case dbsync_worker:get_space_id(Doc) of
                {ok, SID} ->
                    {ok, _} = save(#document{key = sid_doc_key(ModelName, Key),
                        value = #dbsync_state{entry = {ok, SID}}}),
                    ok;
                {error, not_a_space} ->
                    {ok, _} = save(#document{key = sid_doc_key(ModelName, Key),
                        value = #dbsync_state{entry = {error, not_a_space}}}),
                    ok;
                Other ->
                    {prehook_error, Other}
            end;
        {error, {not_found, _}} ->
            ok;
        Other2 ->
            {prehook_error, Other2}
    end.

%%--------------------------------------------------------------------
%% @doc
%% Returns key for dbsync_state document that holds information about sid.
%% @end
%%--------------------------------------------------------------------
-spec sid_doc_key(ModelName :: model_behaviour:model_type(), Key :: datastore:ext_key()) -> BinKey :: binary().
sid_doc_key(ModelName, Key) ->
    Base = base64:encode(term_to_binary({sid, ModelName, Key})),
    case byte_size(Base) > 120 of
        true ->
            binary:part(Base, {0, 120});
        _ ->
            Base
    end.