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

-export([resolve_path/1, create/2, get_scope/1, list_uuids/3, gen_path/1]).

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
    runner(fun() ->
        {ok, #document{} = Document} = resolve_path(Path),
        update(Document, Diff)
    end);
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

create({uuid, ParentUUID}, File) ->
    runner(fun() ->
        {ok, Parent} = get(ParentUUID),
        create(Parent, File)
    end);
create({path, Path}, File) ->
    runner(fun() ->
        {ok, Parent} = resolve_path(Path),
        create(Parent, File)
    end);
create(#document{} = Parent, #file_meta{name = FileName} = File) ->
    runner(fun() ->
        FileDoc = #document{value = File},
        case create(FileDoc) of
            {ok, UUID} ->
                SavedDoc = FileDoc#document{key = UUID},
                {ok, Scope} = get_scope(Parent),
                ok = datastore:add_links(disk_only, Parent, {FileName, SavedDoc}),
                ok = datastore:add_links(disk_only, SavedDoc, [{parent, Parent}, {scope, Scope}]),
                {ok, UUID};
            {error, Reason} ->
                {error, Reason}
        end
    end).

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
delete(#document{value = #file_meta{name = FileName}, key = Key}) ->
    runner(fun() ->
        {ok, {ParentKey, ?MODEL_NAME}} = datastore:fetch_link(disk_only, Key, ?MODEL_NAME,parent),
        ok = datastore:delete_links(disk_only, ParentKey, ?MODEL_NAME, FileName),
        datastore:delete(globally_cached, ?MODULE, Key)
    end);
delete({path, Path}) ->
    runner(fun() ->
        {ok, #document{} = Document} = resolve_path(Path),
        delete(Document)
    end);
delete(Key) ->
    runner(fun() ->
        {ok, #document{} = Document} = get(Key),
        delete(Document)
    end).

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
            false;
        {error, link_not_found} ->
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



list_uuids(Entry, Offset, Count) ->
    runner(fun() ->
        {ok, #document{} = File} = get(Entry),
        Res = datastore:foreach_link(disk_only, File, fun
                (_LinkName, _LinkTarget, {_, 0, _} = Acc) ->
                    Acc;
                (LinkName, {_Key, ?MODEL_NAME}, {Skip, Count1, Acc}) when is_binary(LinkName), Skip > 0 ->
                    {Skip - 1, Count1, Acc};
                (LinkName, {Key, ?MODEL_NAME}, {0, Count1, Acc}) when is_binary(LinkName), Count > 0 ->
                    {0, Count1 - 1, [Key | Acc]};
                (_LinkName, _LinkTarget, AccIn) ->
                    AccIn
            end, {Offset, Count, []}),
        case Res of
            {ok, {_, _, UUIDs}} ->
                {ok, UUIDs};
            {error, Reason} ->
                {error, Reason}
        end
    end).


-spec gen_path(entry()) -> {ok, path()} | datastore:generic_error().
gen_path({path, Path}) when is_binary(Path) ->
    {ok, Path};
gen_path(Entry) ->
    gen_path2(Entry, []).





%%%===================================================================
%%% Internal functions
%%%===================================================================


-spec gen_path2(entry(), [datastore:document()]) -> {ok, path()} | datastore:generic_error().
gen_path2(Entry, Acc) ->
    {ok, #document{} = Doc} = get(Entry),
    case datastore:fetch_link(disk_only, Doc, parent) of
        {ok, {?ROOT_DIR_UUID, _}} ->
            Tokens = [Token || #document{value = #file_meta{name = Token}} <- [Doc | Acc]],
            {ok, fslogic_path:join([<<?DIRECTORY_SEPARATOR>> | Tokens])};
        {ok, {ParentUUID, _}} ->
            gen_path2({uuid, ParentUUID}, [Doc | Acc])
    end.


-spec resolve_path(path()) -> {ok, datastore:document()} | datastore:generic_error().
resolve_path(<<?DIRECTORY_SEPARATOR, Path/binary>>) ->
    case fslogic_path:split(Path) of
        [] ->
            get(?ROOT_DIR_UUID);
        Tokens ->
            datastore:link_walk(disk_only, ?RESPONSE(get(?ROOT_DIR_UUID)), Tokens, get_leaf)
    end.



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


runner(Block) ->
    try Block() of
        {error, link_not_found} -> %% Map links errors to document errors
            {error, {not_found, ?MODEL_NAME}};
        Other -> Other
    catch
        error:{badmatch, {error, Reason}} ->
            {error, Reason};
        error:{badmatch, {ok, Inv}} ->
            {error, {inavlid_reponse, Inv}};
        error:{badmatch, Reason} ->
            {error, Reason};
        error:{case_clause, {error, Reason}} ->
            {error, Reason};
        error:{case_clause, {ok, Inv}} ->
            {error, {inavlid_reponse, Inv}};
        error:{case_clause, Reason} ->
            {error, Reason}
    end.


