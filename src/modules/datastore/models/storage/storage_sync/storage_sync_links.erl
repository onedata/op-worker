%%%-------------------------------------------------------------------
%%% @author Jakub Kudzia
%%% @copyright (C) 2019 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%%--------------------------------------------------------------------
%%% @doc
%%% Model for WRITEME
%%% @end
%%%-------------------------------------------------------------------
-module(storage_sync_links).
-author("Jakub Kudzia").

-include("global_definitions.hrl").
-include("modules/fslogic/fslogic_common.hrl").
-include("modules/datastore/datastore_models.hrl").
-include("modules/datastore/datastore_runner.hrl").
-include_lib("ctool/include/logging.hrl").

%% API
-export([add_link/5, delete_link/4, delete_recursive/3, list/4, list/5]).

% todo debug delete
-export([get_link/2, get_link/4]).

%% datastore_model callbacks
-export([get_ctx/0, get_record_struct/1]).

-type root_id() :: binary().
-type link_name() :: helpers:file_id().
-type link_target() :: root_id() | undefined.
-type fold_fun() :: datastore:fold_fun(link_name(), link_target()).
-type error() :: {error, term()}.

-export_type([link_name/0, link_target/0]).

-define(CTX, #{model => ?MODULE}).
-define(SEPARATOR, "_").
-define(ROOT_ID(RootStorageFileId, SpaceId, StorageId),
    <<"storage_sync_links_", (base64:encode(crypto:hash(md5, [RootStorageFileId, SpaceId, StorageId])))/binary>>).
%%    % todo tutaj leci jakis blad ze nie moze zencodować stringa jak jest hash moze zamienic na base64
%%    <<"storage_sync_links_", SpaceId/binary, "_", StorageId/binary, "_", RootStorageFileId/binary>>).


% TODO
% TODO * deletion of whole trees structure

%%%===================================================================
%%% API functions
%%%===================================================================

-spec get_link(root_id(), link_name()) -> {ok, link_target()} | error().
get_link(RootId, ChildName) ->
    case datastore_model:get_links(?CTX, RootId, all, ChildName) of
        {ok, [#link{target = Target}]} -> {ok, Target};
        Error -> Error
    end.

-spec get_link(helpers:file_id(), od_space:id(), storage:id(), link_name()) -> {ok, link_target()} | error().
get_link(RootStorageFileId, SpaceId, StorageId, ChildName) ->
    get_link(?ROOT_ID(RootStorageFileId, SpaceId, StorageId), ChildName).


-spec add_link(helpers:file_id(), od_space:id(), storage:id(), link_name(), boolean()) -> ok.
add_link(RootStorageFileId, SpaceId, StorageId, ChildStorageFileId, MarkLeaves) ->
    % todo opisać, że MarkLeaves dodaje linka dla liscia z targetem undefined, dzieki czemu na object storage'u bedziemy wiedziec kiedy jest lisc i
    % nie bedziemy bez sensu dla kazdego pliku reg. szukac jego drzewa (bo go nie ma)
    % z kolei na block storage'u zawsze bedzie target ustawiony, ale sync olewa to bo i tak jedzie tylko po jednym katalogu
    ChildrenTokens = fslogic_path:split(ChildStorageFileId) -- fslogic_path:split(RootStorageFileId),
    RootId = ?ROOT_ID(RootStorageFileId, SpaceId, StorageId),
    add_link_recursive(RootId, RootStorageFileId, SpaceId, StorageId, ChildrenTokens, MarkLeaves).

list(RootStorageFileId, SpaceId, StorageId, Limit) ->
    list(?ROOT_ID(RootStorageFileId, SpaceId, StorageId), Limit).

list(RootId, Limit) ->
    list(RootId, #link_token{}, Limit).

-spec list(helpers:file_id(), od_space:id(), storage:id(), datastore_link_iter:token()) ->
    {ok, [link_name()]}. % todo extended info
list(RootStorageFileId, SpaceId, StorageId, Token, Limit) ->
    list(?ROOT_ID(RootStorageFileId, SpaceId, StorageId), Token, Limit).

list(RootId, Token, Limit) ->
    Token2 = utils:ensure_defined(Token, undefined, #link_token{}),
    Opts = #{token => Token2},
    Opts2 = case Limit of
        all -> Opts;
        _ -> Opts#{size => Limit}
    end,
    list_internal(RootId, Opts2).

list_internal(RootId, Opts) ->
    {{ok, ChildrenReversed}, NewToken} = for_each(RootId,
        fun(ChildName, ChildTreeRootId, FilesAcc) ->
            [{ChildName, ChildTreeRootId} | FilesAcc]
        end, [], Opts
    ),
    {{ok, lists:reverse(ChildrenReversed)}, NewToken}.

% todo ogarnac, bo niektore funkcje, ktore maja Rootid jako arugment powinny byc internal

-spec delete_recursive(helpers:file_id(), od_space:id(), storage:id()) -> ok.
delete_recursive(RootStorageFileId, SpaceId, StorageId) ->
    delete_recursive(?ROOT_ID(RootStorageFileId, SpaceId, StorageId)).

delete_recursive(undefined) ->
    ok;
delete_recursive(RootId) ->
    delete_recursive(RootId, #link_token{}).

delete_recursive(RootId, Token) ->
    case list(RootId, Token, 1000) of
        {{ok, Children}, Token2} ->
            delete_children(RootId, Children),
            case Token2#link_token.is_last of
                true ->
                    ok;
                false ->
                    delete_recursive(RootId, Token2)
            end;
        {error, not_found} ->
            ok
    end.

delete_children(_RootId, []) ->
    ok;
delete_children(RootId, [{ChildName, ChildRootId} | Rest]) ->
    delete_recursive(ChildRootId),
    delete_link(RootId, ChildName),
    delete_children(RootId, Rest).


-spec delete_link(helpers:file_id(), od_space:id(), storage:id(), link_name()) -> ok.
delete_link(RootStorageFileId, SpaceId, StorageId, ChildName) ->
    delete_link(?ROOT_ID(RootStorageFileId, SpaceId, StorageId), ChildName).

delete_link(RootId, ChildName) ->
    TreeId = oneprovider:get_id(),
    case datastore_model:delete_links(?CTX, RootId, TreeId, ChildName) of
        [] -> ok;
        ok -> ok
    end.

%%%===================================================================
%%% Internal functions
%%%===================================================================

-spec add_link_internal(root_id(), link_name(), link_target() | undefined) -> ok.
add_link_internal(RootId, ChildName, Target) ->
    TreeId = oneprovider:get_id(),
    case datastore_model:add_links(?CTX, RootId, TreeId, {ChildName, Target}) of
        {ok, _} -> ok;
        {error, already_exists} -> ok
    end.

-spec add_link_recursive(root_id(), helpers:file_id(), od_space:id(), storage:id(),
    [helpers:file_id()], boolean()) -> ok.
add_link_recursive(_RootId, _RootStorageFileId, _SpaceId, _StorageId, [], _MarkLeaves) ->
    ok;
add_link_recursive(RootId, RootStorageFileId, SpaceId, StorageId, [ChildName | RestChildren], MarkLeaves) ->
    ChildStorageFileId = filename:join([RootStorageFileId, ChildName]),
    ChildRootId = case get_link(RootStorageFileId, SpaceId, StorageId, ChildName) of
        {ok, ChildRootId0} ->
            ChildRootId0;
        {error, not_found} ->
            ChildRootId0 = case {length(RestChildren) =:= 0, MarkLeaves} of
                {true, true} -> undefined;
                _ -> ?ROOT_ID(ChildStorageFileId, SpaceId, StorageId)
            end,
            add_link_internal(RootId, ChildName, ChildRootId0),
            ChildRootId0
    end,
    add_link_recursive(ChildRootId, ChildStorageFileId, SpaceId, StorageId, RestChildren, MarkLeaves).

-spec delete_link_internal(root_id(), link_name()) -> ok.
delete_link_internal(RootId, ChildName) ->
    TreeId = oneprovider:get_id(),
    datastore_model:delete_links(?CTX, RootId, TreeId, ChildName).


-spec for_each(root_id(), fold_fun(), Acc0 :: term(), datastore:fold_opts()) ->
    {{ok, Acc :: term()}, datastore_links_iter:token()} | {error, term()}.
for_each(RootId, Callback, Acc0, Opts) ->
    datastore_model:fold_links(?CTX, RootId, all,
        fun(#link{name = StorageFileId, target = Target}, Acc) ->
            {ok, Callback(StorageFileId, Target, Acc)}
        end, Acc0, Opts).


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
-spec get_record_struct(datastore_model:record_version()) -> datastore_model:record_struct().
get_record_struct(1) ->
    {record, []}.
