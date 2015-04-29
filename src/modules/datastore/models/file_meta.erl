%%%-------------------------------------------------------------------
%%% @author Rafal Slota
%%% @copyright (C) 2015 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc Model for file's metadata. Implemets low-level metadata operations such as
%%%      walking through file graph.
%%% @end
%%%-------------------------------------------------------------------
-module(file_meta).
-author("Rafal Slota").
-behaviour(model_behaviour).

-include("modules/datastore/datastore.hrl").
-include("modules/fslogic/fslogic_common.hrl").
-include("modules/datastore/datastore_internal.hrl").

-define(ROOT_DIR_UUID, <<"">>).
-define(ROOT_DIR_NAME, <<"">>).

%% model_behaviour callbacks
-export([save/1, get/1, exists/1, delete/1, update/2, create/1, model_init/0,
    'after'/5, before/4]).

-export([resolve_path/1, create/2]).

-type uuid() :: datastore:key().
-type path() :: binary().
-type entry() :: {path, path()} | {uuid, uuid()} | datastore:document().

-export_type([path/0, entry/0]).

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
    datastore:save(globally_cached, Document).

%%--------------------------------------------------------------------
%% @doc
%% {@link model_behaviour} callback update/2.
%% @end
%%--------------------------------------------------------------------
-spec update(datastore:key() | entry(), Diff :: datastore:document_diff()) ->
    {ok, datastore:key()} | datastore:update_error().
update({uuid, Key}, Diff) ->
    update(Key, Diff);
update(#document{value = #file_meta{}, key = Key}, Diff) ->
    update(Key, Diff);
update({path, Path}, Diff) ->
    {ok, #document{} = Document} = resolve_path(Path),
    update(Document, Diff);
update(Key, Diff) ->
    datastore:update(globally_cached, ?MODULE, Key, Diff).

%%--------------------------------------------------------------------
%% @doc
%% {@link model_behaviour} callback create/1.
%% @end
%%--------------------------------------------------------------------
-spec create(datastore:document()) ->
    {ok, datastore:key()} | datastore:create_error().
create(#document{value = #file_meta{name = FileName}} = Document) ->
    case is_valid_filename(FileName) of
        true ->
            datastore:create(globally_cached, Document);
        false ->
            {error, invalid_filename}
    end.


create(Path, #file_meta{name = FileName} = File) ->
    {ok, #document{} = Parent} = resolve_path(Path),
    FileDoc = #document{value = File},
    case create(FileDoc) of
        {ok, UUID} ->
            SavedDoc = FileDoc#document{key = UUID},
            {ok, Scope} = get_scope(Parent),
            datastore:add_links(disk_only, Parent, {FileName, SavedDoc}),
            datastore:add_links(disk_only, SavedDoc, [{parent, Parent}, {scope, Scope}]);
        {error, Reason} ->
            {error, Reason}
    end.

%%--------------------------------------------------------------------
%% @doc
%% {@link model_behaviour} callback get/1.
%% @end
%%--------------------------------------------------------------------
-spec get(datastore:key() | entry()) -> {ok, datastore:document()} | datastore:get_error().
get({uuid, Key}) ->
    get(Key);
get(#document{value = #file_meta{}} = Document) ->
    {ok, Document};
get({path, Path}) ->
    resolve_path(Path);
get(?ROOT_DIR_UUID) ->
    {ok, #document{key = ?ROOT_DIR_UUID, value =
                  #file_meta{name = ?ROOT_DIR_NAME, is_scope = true}}};
get(Key) ->
    datastore:get(globally_cached, ?MODULE, Key).

%%--------------------------------------------------------------------
%% @doc
%% {@link model_behaviour} callback delete/1.
%% @end
%%--------------------------------------------------------------------
-spec delete(datastore:key() | entry()) -> ok | datastore:generic_error().
delete({uuid, Key}) ->
    delete(Key);
delete(#document{value = #file_meta{}, key = Key}) ->
    delete(Key);
delete({path, Path}) ->
    {ok, #document{} = Document} = resolve_path(Path),
    delete(Document);
delete(Key) ->
    datastore:delete(globally_cached, ?MODULE, Key).

%%--------------------------------------------------------------------
%% @doc
%% {@link model_behaviour} callback exists/1.
%% @end
%%--------------------------------------------------------------------
-spec exists(datastore:key()) -> datastore:exists_return().
exists({uuid, Key}) ->
    exists(Key);
exists(#document{value = #file_meta{}, key = Key}) ->
    exists(Key);
exists({path, Path}) ->
    case resolve_path(Path) of
        {ok, #document{}} ->
            true;
        {error, {not_found, _}} ->
            false
    end;
exists(Key) ->
    ?RESPONSE(datastore:exists(globally_cached, ?MODULE, Key)).

%%--------------------------------------------------------------------
%% @doc
%% {@link model_behaviour} callback model_init/0.
%% @end
%%--------------------------------------------------------------------
-spec model_init() -> model_behaviour:model_config().
model_init() ->
    ?MODEL_CONFIG(files, []).

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


-spec resolve_path(path()) -> {ok, datastore:document()} | datastore:generic_error().
resolve_path(Path) ->
    Tokens = fslogic_path:split(Path),
    datastore:link_walk(disk_only, ?RESPONSE(get(?ROOT_DIR_UUID)), Tokens, get_leaf).


is_valid_filename(<<"">>) ->
    false;
is_valid_filename(FileName) when not is_binary(FileName) ->
    false;
is_valid_filename(FileName) when is_binary(FileName) ->
    case binary:matches(FileName, <<?DIRECTORY_SEPARATOR>>) of
        [] -> true;
        _  -> false
    end.


get_scope(#document{value = #file_meta{is_scope = true}} = Document) ->
    {ok, Document};
get_scope(#document{value = #file_meta{is_scope = false}} = Document) ->
    datastore:fetch_link_target(disk_only, Document, scope).


