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
-export([add/2, remove/2, cleanup/1]).

%% model_behaviour callbacks
-export([save/1, get/1, list/0, exists/1, delete/1, update/2, create/1,
    model_init/0, 'after'/5, before/4]).

%%%===================================================================
%%% API
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% For subscriptions concerning file changes adds session to the list of
%% sessions that are interested in receiving notifications.
%% @end
%%--------------------------------------------------------------------
-spec add(SessId :: session:id(), Ref :: #subscription{} | subscription:object()) ->
    ok | {error, Reason :: term()}.
add(SessId, #subscription{object = SubObject}) ->
    add(SessId, SubObject);
add(SessId, SubObject) ->
    case get_key(SubObject) of
        undefined -> ok;
        Key -> do_add(Key, SessId)
    end.

%%--------------------------------------------------------------------
%% @doc
%% For subscriptions concerning file changes removes session from the list of
%% sessions that are interested in receiving notifications.
%% @end
%%--------------------------------------------------------------------
-spec remove(SessId :: session:id(), Ref :: #subscription{} | subscription:object()) ->
    ok | {error, Reason :: term()}.
remove(SessId, #subscription{object = SubObject}) ->
    remove(SessId, SubObject);
remove(SessId, SubObject) ->
    case get_key(SubObject) of
        undefined -> ok;
        Key -> do_remove(Key, SessId)
    end.

%%--------------------------------------------------------------------
%% @doc
%% Removes file subscription mapping if there are no sessions interested in
%% receiving notifications.
%% @end
%%--------------------------------------------------------------------
-spec cleanup(Ref :: #subscription{} | subscription:object() | datastore:key()) ->
    ok | {error, Reason :: term()}.
cleanup(#subscription{object = SubObject}) ->
    cleanup(SubObject);
cleanup(Key) when is_binary(Key) ->
    Pred = fun() ->
        case ?MODULE:get(Key) of
            {ok, #document{value = #file_subscription{sessions = []}}} -> true;
            _ -> false
        end
    end,
    datastore:delete(?STORE_LEVEL, ?MODEL_NAME, Key, Pred);
cleanup(SubObject) ->
    case get_key(SubObject) of
        undefined -> ok;
        Key -> cleanup(Key)
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
-spec get(datastore:key() | #event{} | event:object()) ->
    {ok, datastore:document()} | datastore:get_error().
get(#event{object = EvtObject}) ->
    ?MODULE:get(EvtObject);
get(Key) when is_binary(Key) ->
    datastore:get(?STORE_LEVEL, ?MODEL_NAME, Key);
get(EvtObject) ->
    case get_key(EvtObject) of
        undefined -> {error, {not_found, ?MODULE}};
        Key -> ?MODULE:get(Key)
    end.

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
        {ok, Sub#file_subscription{sessions = [SessId | lists:delete(SessId, SessIds)]}}
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
        NewSessIds = lists:delete(SessId, SessIds),
        case NewSessIds of
            [] -> erlang:spawn(?MODULE, cleanup, [Key]);
            _ -> ok
        end,
        {ok, Sub#file_subscription{sessions = NewSessIds}}
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