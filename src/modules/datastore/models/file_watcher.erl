%%%-------------------------------------------------------------------
%%% @author Rafal Slota
%%% @copyright (C) 2015 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc Model for holding list of sessions that are registered for
%%%      attributes and file blocks changes. Key of the model shall match
%%%      correspnding file_meta entry.
%%% @end
%%%-------------------------------------------------------------------
-module(file_watcher).
-author("Rafal Slota").
-behaviour(model_behaviour).

-include("modules/datastore/datastore_specific_model.hrl").

%% model_behaviour callbacks
-export([save/1, get/1, exists/1, delete/1, update/2, create/1, model_init/0,
    'after'/5, before/4, list/0]).
-export([insert_open_watcher/2, insert_attr_watcher/2]).
-export([get_open_watchers/1, get_attr_watchers/1]).


%%--------------------------------------------------------------------
%% @doc
%% Save given session as one that has opened given file.
%% @end
%%--------------------------------------------------------------------
-spec insert_open_watcher(Key :: datastore:key(), SessionId :: session:id()) -> ok | {error, term()}.
insert_open_watcher(Key, SessionId) ->
    datastore:run_synchronized(?MODEL_NAME, res_id(Key),
        fun() ->
            case get(Key) of
                {ok, #document{value = #file_watcher{open_sessions = OpenSess} = Value} = Doc} ->
                    {ok, _} = save(Doc#document{value = Value#file_watcher{open_sessions = lists:umerge([SessionId], OpenSess)}}),
                    ok;
                {error, {not_found, _}} ->
                    {ok, _} = create(#document{key = Key, value = #file_watcher{open_sessions = [SessionId]}}),
                    ok
            end
        end).


%%--------------------------------------------------------------------
%% @doc
%% Save given session as one that is watching changes on file's attributes.
%% @end
%%--------------------------------------------------------------------
-spec insert_attr_watcher(Key :: datastore:key(), SessionId :: session:id()) -> ok | {error, term()}.
insert_attr_watcher(Key, SessionId) ->
    datastore:run_synchronized(?MODEL_NAME, res_id(Key),
        fun() ->
            case get(Key) of
                {ok, #document{value = #file_watcher{attr_sessions = OpenSess} = Value} = Doc} ->
                    {ok, _} = save(Doc#document{value = Value#file_watcher{attr_sessions = lists:umerge([SessionId], OpenSess)}}),
                    ok;
                {error, {not_found, _}} ->
                    {ok, _} = create(#document{key = Key, value = #file_watcher{attr_sessions = [SessionId]}}),
                    ok
            end
        end).


%%--------------------------------------------------------------------
%% @doc
%% Get all sessions that was previousely registered with insert_open_watcher/2
%% @end
%%--------------------------------------------------------------------
-spec get_open_watchers(Key :: datastore:key()) -> [session:id()].
get_open_watchers(Key) ->
    case get(Key) of
        {ok, #document{value = #file_watcher{open_sessions = OpenSess}}} ->
            OpenSess;
        {error, {not_found, _}} ->
            []
    end.


%%--------------------------------------------------------------------
%% @doc
%% Get all sessions that were previously registered with get_attr_watchers/2
%% @end
%%--------------------------------------------------------------------
-spec get_attr_watchers(Key :: datastore:key()) -> [session:id()].
get_attr_watchers(Key) ->
    case get(Key) of
        {ok, #document{value = #file_watcher{attr_sessions = OpenSess}}} ->
            OpenSess;
        {error, {not_found, _}} ->
            []
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
save(#document{key = Key} = Document) when is_binary(Key) ->
    datastore:run_synchronized(?MODEL_NAME, res_id(Key),
        fun() ->
            datastore:save(?STORE_LEVEL, Document)
        end).

%%--------------------------------------------------------------------
%% @doc
%% {@link model_behaviour} callback update/2.
%% @end
%%--------------------------------------------------------------------
-spec update(datastore:key(), Diff :: datastore:document_diff()) ->
    {ok, datastore:key()} | datastore:update_error().
update(Key, Diff) ->
    datastore:update(?STORE_LEVEL, ?MODULE, Key, Diff).

%%--------------------------------------------------------------------
%% @doc
%% {@link model_behaviour} callback create/1.
%% @end
%%--------------------------------------------------------------------
-spec create(datastore:document()) ->
    {ok, datastore:key()} | datastore:create_error().
create(#document{key = Key} = Document) when is_binary(Key) ->
    datastore:run_synchronized(?MODEL_NAME, res_id(Key),
        fun() ->
            datastore:create(?STORE_LEVEL, Document)
        end);
create(Document) ->
    datastore:create(?STORE_LEVEL, Document).

%%--------------------------------------------------------------------
%% @doc
%% {@link model_behaviour} callback get/1.
%% @end
%%--------------------------------------------------------------------
-spec get(datastore:key()) -> {ok, datastore:document()} | datastore:get_error().
get(Key) ->
    datastore:get(?STORE_LEVEL, ?MODULE, Key).

%%--------------------------------------------------------------------
%% @doc
%% {@link model_behaviour} callback delete/1.
%% @end
%%--------------------------------------------------------------------
-spec delete(datastore:key()) -> ok | datastore:generic_error().
delete(Key) ->
    datastore:delete(?STORE_LEVEL, ?MODULE, Key).

%%--------------------------------------------------------------------
%% @doc
%% {@link model_behaviour} callback exists/1.
%% @end
%%--------------------------------------------------------------------
-spec exists(datastore:key()) -> datastore:exists_return().
exists(Key) ->
    ?RESPONSE(datastore:exists(?STORE_LEVEL, ?MODULE, Key)).

%%--------------------------------------------------------------------
%% @doc
%% {@link model_behaviour} callback model_init/0.
%% @end
%%--------------------------------------------------------------------
-spec model_init() -> model_behaviour:model_config().
model_init() ->
    ?MODEL_CONFIG(system, [{file_meta, create}, {file_meta, save}, {file_meta, delete}], ?GLOBAL_ONLY_LEVEL).

%%--------------------------------------------------------------------
%% @doc
%% {@link model_behaviour} callback 'after'/5.
%% @end
%%--------------------------------------------------------------------
-spec 'after'(ModelName :: model_behaviour:model_type(),
    Method :: model_behaviour:model_action(),
    Level :: datastore:store_level(), Context :: term(),
    ReturnValue :: term()) -> ok.
'after'(file_meta, create, _Level, _Context, {ok, Key}) ->
    create(#document{key = Key, value = #file_watcher{}});
'after'(file_meta, save, _Level, _Context, {ok, Key}) ->
    create(#document{key = Key, value = #file_watcher{}});
'after'(file_meta, delete, _Level, _Context, {ok, Key}) ->
    delete(Key);
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

%%--------------------------------------------------------------------
%% @doc
%% Returns list of all records.
%% @end
%%--------------------------------------------------------------------
-spec list() -> {ok, [datastore:document()]} | datastore:generic_error() | no_return().
list() ->
    datastore:list(?STORE_LEVEL, ?MODEL_NAME, ?GET_ALL, []).

-spec res_id(datastore:key()) -> binary().
res_id(Key) ->
    <<"watcher_", Key/binary>>.
