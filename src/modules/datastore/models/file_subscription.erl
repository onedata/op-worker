%%%-------------------------------------------------------------------
%%% @author Krzysztof Trzepla
%%% @copyright (C) 2016 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc File subscription model.
%%% @end
%%%-------------------------------------------------------------------
-module(file_subscription).
-author("Krzysztof Trzepla").
-behaviour(model_behaviour).

-include_lib("ctool/include/logging.hrl").
-include("modules/events/types.hrl").
-include("modules/datastore/datastore_specific_models_def.hrl").
-include_lib("cluster_worker/include/modules/datastore/datastore_model.hrl").

-define(BATCH_SIZE, 100).

%% API
-export([add/2, remove/2]).

%% model_behaviour callbacks
-export([save/1, get/1, list/0, exists/1, delete/1, update/2, create/1,
    model_init/0, 'after'/5, before/4]).

%%%===================================================================
%%% API
%%%===================================================================

add(SessId, #subscription{object = SubObject}) ->
    case get_key(SubObject) of
        undefined -> ok;
        Key -> do_add(Key, SessId)
    end.


remove(SessId, #subscription{object = SubObject}) ->
    case get_key(SubObject) of
        undefined -> ok;
        Key -> do_remove(Key, SessId)
    end.

%%%===================================================================
%%% model_behaviour callbacks
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% {@link model_behaviour} callback save/1. 
%% @end
%%--------------------------------------------------------------------
-spec save(datastore:document()) ->
    {ok, datastore:key()} | datastore:generic_error().
save(Document) ->
    datastore:save(?STORE_LEVEL, Document).

%%--------------------------------------------------------------------
%% @doc
%% {@link model_behaviour} callback update/2. 
%% @end
%%--------------------------------------------------------------------
-spec update(datastore:key(), Diff :: datastore:document_diff()) ->
    {ok, datastore:key()} | datastore:update_error().
update(Key, Diff) ->
    datastore:update(?STORE_LEVEL, ?MODEL_NAME, Key, Diff).

%%--------------------------------------------------------------------
%% @doc
%% {@link model_behaviour} callback create/1. 
%% @end
%%--------------------------------------------------------------------
-spec create(datastore:document()) ->
    {ok, datastore:key()} | datastore:create_error().
create(Document) ->
    datastore:create(?STORE_LEVEL, Document).

%%--------------------------------------------------------------------
%% @doc
%% {@link model_behaviour} callback get/1.
%% @end
%%--------------------------------------------------------------------
-spec get(datastore:key()) -> {ok, datastore:document()} | datastore:get_error().
get(#event{object = EvtObject}) ->
    case get_key(EvtObject) of
        undefined -> {error, {not_found, ?MODULE}};
        Key -> ?MODULE:get(Key)
    end;
get(Key) ->
    datastore:get(?STORE_LEVEL, ?MODEL_NAME, Key).

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
-spec delete(datastore:key()) -> ok | datastore:generic_error().
delete(Key) ->
    datastore:delete(?STORE_LEVEL, ?MODEL_NAME, Key).

%%--------------------------------------------------------------------
%% @doc
%% {@link model_behaviour} callback exists/1. 
%% @end
%%--------------------------------------------------------------------
-spec exists(datastore:key()) -> datastore:exists_return().
exists(Key) ->
    ?RESPONSE(datastore:exists(?STORE_LEVEL, ?MODEL_NAME, Key)).

%%--------------------------------------------------------------------
%% @doc
%% {@link model_behaviour} callback model_init/0. 
%% @end
%%--------------------------------------------------------------------
-spec model_init() -> model_behaviour:model_config().
model_init() ->
    ?MODEL_CONFIG(file_subscription_bucket, [], ?GLOBAL_ONLY_LEVEL).

%%--------------------------------------------------------------------
%% @doc
%% {@link model_behaviour} callback 'after'/5. 
%% @end
%%--------------------------------------------------------------------
-spec 'after'(ModelName :: model_behaviour:model_type(),
    Method :: model_behaviour:model_action(),
    Level :: datastore:store_level(), Context :: term(),
    ReturnValue :: term()) -> ok.
'after'(_ModelName, _Method, _Level, _Context, _ReturnValue) ->
    ok.

%%--------------------------------------------------------------------
%% @doc
%% {@link model_behaviour} callback before/4. 
%% @end
%%--------------------------------------------------------------------
-spec before(ModelName :: model_behaviour:model_type(),
    Method :: model_behaviour:model_action(),
    Level :: datastore:store_level(), Context :: term()) ->
    ok | datastore:generic_error().
before(_ModelName, _Method, _Level, _Context) ->
    ok.

%%%===================================================================
%%% Internal functions
%%%===================================================================

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Returns datastore key for subscription concerning file updates. If subscription
%% does not concern file changes 'undefined' is returned.
%% @end
%%--------------------------------------------------------------------
-spec get_key(Object :: event:object() | subscription:object()) -> 
    Key :: datastore:key() | undefiend.
get_key(#file_attr_subscription{file_uuid = FileUuid}) ->
    <<"file_attr.", FileUuid/binary>>;
get_key(#file_location_subscription{file_uuid = FileUuid}) ->
    <<"file_location.", FileUuid/binary>>;
get_key(#permission_changed_subscription{file_uuid = FileUuid}) ->
    <<"permission_changed.", FileUuid/binary>>;
get_key(#file_removal_subscription{file_uuid = FileUuid}) ->
    <<"file_removal.", FileUuid/binary>>;
get_key(#file_renamed_subscription{file_uuid = FileUuid}) ->
    <<"file_renamed.", FileUuid/binary>>;

get_key(#update_event{object = #file_attr{uuid = FileUuid}}) ->
    <<"file_attr.", FileUuid/binary>>;
get_key(#update_event{object = #file_location{uuid = FileUuid}}) ->
    <<"file_location.", FileUuid/binary>>;
get_key(#permission_changed_event{file_uuid = FileUuid}) ->
    <<"permission_changed_event.", FileUuid/binary>>;
get_key(#file_removal_event{file_uuid = FileUuid}) ->
    <<"file_removal.", FileUuid/binary>>;
get_key(#file_renamed_event{top_entry = #file_renamed_entry{old_uuid = FileUuid}}) ->
    <<"file_renamed.", FileUuid/binary>>;

get_key(_) ->
    undefined.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Add session to the list of sessions that are interested in receiving
%% notifications about file changes.
%% @end
%%--------------------------------------------------------------------
-spec do_add(Key :: datastore:key(), SessId :: session:id()) -> ok | {error, Reason :: term()}.
do_add(Key, SessId) ->
    Diff = fun(#file_subscription{sessions = SessIds} = Sub) ->
        Sub#file_subscription{sessions = [SessId | lists:delete(SessId, SessIds)]}
    end,
    case update(Key, Diff) of
        {ok, _} -> ok;
        {error, {not_found, _}} -> do_create(Key, SessId);
        {error, Reason} -> {error, Reason}
    end.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Removes session from the list of sessions that are interested in receiving
%% notifications about file changes.
%% @end
%%--------------------------------------------------------------------
-spec do_remove(Key :: datastore:key(), SessId :: session:id()) -> 
    ok | {error, Reason :: term()}.
do_remove(Key, SessId) ->
    Diff = fun(#file_subscription{sessions = SessIds} = Sub) ->
        Sub#file_subscription{sessions = lists:delete(SessId, SessIds)}
    end,
    case update(Key, Diff) of
        {ok, _} -> ok;
        {error, {not_found, _}} -> ok;
        {error, Reason} -> {error, Reason}
    end.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Creates file subscription entry for given sessions or if it already exists 
%% adds session to the list of sessions that are interested in receiving
%% notifications about file changes.
%% @end
%%--------------------------------------------------------------------
-spec do_create(Key :: datastore:key(), SessId :: session:id()) -> ok | {error, Reason :: term()}.
do_create(Key, SessId) ->
    case create(#document{key = Key, value = #file_subscription{sessions = [SessId]}}) of
        {ok, _} -> ok;
        {error, already_exists} -> do_add(Key, SessId);
        {error, Reason} -> {error, Reason}
    end.