%%%-------------------------------------------------------------------
%%% @author Jakub Kudzia
%%% @copyright (C) 2019 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%%--------------------------------------------------------------------
%%% @doc
%%% In this module, util functions for operating on storage_sync_links
%%% are implemented.
%%% storage_sync_links are links used by storage import to compare lists of
%%% files on storage with lists of files in the Onedata system.
%%% For each directory on synced storage, separate links tree is created.
%%% Functions with suffix `_recursive` in this module operate on whole
%%% trees structure.
%%% Assume that there is a file on storage with
%%% an absolute path: <<"/root1/dir1/dir2/dir3/leaf">>
%%% Trees structure will look like presented below: (notation: TREE_ID -> {LinkName, LinkValue})
%%%    * ?ROOT_ID(<<"/root1">>) -> {<<"dir1">>, ?ROOT_ID(<<"/root1/dir1">>)}
%%%    * ?ROOT_ID(<<"/root1/dir1">>) -> {<<"dir2">>, ?ROOT_ID(<<"/root1/dir1/dir2">>)}
%%%    * ?ROOT_ID(<<"/root1/dir1/dir2">>) -> {<<"dir3">>, ?ROOT_ID(<<"/root1/dir1/dir2/dir3">>)}
%%%    * ?ROOT_ID(<<"/root1/dir1/dir2/dir3">>) -> {<<"leaf">>, LeafValue)}
%%% @end
%%%-------------------------------------------------------------------
-module(storage_sync_links).
-author("Jakub Kudzia").

-include("global_definitions.hrl").

%% API
-export([add_link_recursive/4, list/4, delete_recursive/2]).

%% exported for CT tests
-export([get_link/2, get_link/3, list/3, delete_link/3]).

%% datastore_model callbacks
-export([get_ctx/0]).

-type root_id() :: binary().
-type link_name() :: helpers:file_id().
-type link() :: {link_name(), link_target()}.
-type link_target() :: root_id() | undefined.
-type fold_fun() :: datastore:fold_fun({link_name(), link_target()}).
-type error() :: {error, term()}.

-export_type([link/0]).

-define(CTX, #{model => ?MODULE}).
% RootId of a links tree associated with StorageFileId directory
-define(FOREST_ID(StorageFileId, StorageId),
    <<"storage_sync_links_", (base64:encode(crypto:hash(md5, [StorageFileId, StorageId])))/binary>>).

%%%===================================================================
%%% API functions
%%%===================================================================

%%-------------------------------------------------------------------
%% @doc
%% This function adds link associated with ChildStorageFileId to trees
%% structure with root at StorageFileId.
%% Separate tree is created for each directory.
%% This function is recursive, which means, that if ChildStorageFileId
%% is not a direct child of StorageFileId, the function will create
%% intermediate links and trees.
%% e. g.
%% call add_link_recursive(<<"/root1">>, StorageId, <<"/root1/dir1/dir2/dir3/leaf">>, MarkLeaves)
%% will create the following trees and links: (notation: TREE_ID -> {LinkName, LinkValue})
%%    * ?ROOT_ID(<<"/root1">>) -> {<<"dir1">>, ?ROOT_ID(<<"/root1/dir1">>)}
%%    * ?ROOT_ID(<<"/root1/dir1">>) -> {<<"dir2">>, ?ROOT_ID(<<"/root1/dir1/dir2">>)}
%%    * ?ROOT_ID(<<"/root1/dir1/dir2">>) -> {<<"dir3">>, ?ROOT_ID(<<"/root1/dir1/dir2/dir3">>)}
%%    * ?ROOT_ID(<<"/root1/dir1/dir2/dir3">>) -> {<<"leaf">>, LeafValue)}
%%
%% Depending on the value of the MarkLeaves flag, LeafValue can be:
%%    * undefined if MarkLeaves == true
%%    * ?ROOT_ID(<<"/root1/dir1/dir2/dir3/leaf">> if MarkLeaves == false.
%% @end
%%-------------------------------------------------------------------
-spec add_link_recursive(helpers:file_id(), storage:id(), link_name(), boolean()) -> ok.
add_link_recursive(StorageFileId, StorageId, ChildStorageFileId, MarkLeaves) ->
    ChildrenTokens = filepath_utils:split(ChildStorageFileId) -- filepath_utils:split(StorageFileId),
    RootId = ?FOREST_ID(StorageFileId, StorageId),
    add_link_recursive(RootId, StorageFileId, StorageId, ChildrenTokens, MarkLeaves).

-spec list(helpers:file_id(), storage:id(), datastore_links_iter:token(), non_neg_integer()) ->
    {{ok, [link()]}, datastore_links_iter:token()} | {error, term()}.
list(StorageFileId, StorageId, Token, Limit) ->
    list_internal(?FOREST_ID(StorageFileId, StorageId), Token, Limit).


%%-------------------------------------------------------------------
%% @doc
%% This function adds deletes whole trees structure with root at
%% StorageFileId.
%% @end
%%-------------------------------------------------------------------
-spec delete_recursive(helpers:file_id(), storage:id()) -> ok.
delete_recursive(StorageFileId, StorageId) ->
    delete_recursive_internal(?FOREST_ID(StorageFileId, StorageId)).

%%%===================================================================
%%% functions exported for CT tests
%%%===================================================================

-spec get_link(root_id(), link_name()) -> {ok, link_target()} | error().
get_link(RootId, ChildName) ->
    case datastore_model:get_links(?CTX, RootId, all, ChildName) of
        {ok, [#link{target = Target}]} -> {ok, Target};
        Error -> Error
    end.

-spec get_link(helpers:file_id(), storage:id(), link_name()) -> {ok, link_target()} | error().
get_link(StorageFileId, StorageId, ChildName) ->
    get_link(?FOREST_ID(StorageFileId, StorageId), ChildName).

-spec list(helpers:file_id(), storage:id(), non_neg_integer()) ->
    {{ok, [link()]}, datastore_links_iter:token()} | {error, term()}.
list(StorageFileId, StorageId, Limit) ->
    list_internal(?FOREST_ID(StorageFileId, StorageId), #link_token{}, Limit).

-spec delete_link(helpers:file_id(), storage:id(), link_name()) -> ok.
delete_link(StorageFileId, StorageId, ChildName) ->
    delete_link_internal(?FOREST_ID(StorageFileId, StorageId), ChildName).

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

-spec add_link_recursive(root_id(), helpers:file_id(), storage:id(), [helpers:file_id()], boolean()) -> ok.
add_link_recursive(_RootId, _StorageFileId, _StorageId, [], _MarkLeaves) ->
    ok;
add_link_recursive(RootId, StorageFileId, StorageId, [ChildName | RestChildren], MarkLeaves) ->
    ChildStorageFileId = filename:join([StorageFileId, ChildName]),
    ChildRootId = case get_link(RootId, ChildName) of
        {ok, ChildRootId0} ->
            ChildRootId0;
        {error, not_found} ->
            ChildRootId0 = case {length(RestChildren) =:= 0, MarkLeaves} of
                {true, true} -> undefined;
                _ -> ?FOREST_ID(ChildStorageFileId, StorageId)
            end,
            add_link_internal(RootId, ChildName, ChildRootId0),
            ChildRootId0
    end,
    add_link_recursive(ChildRootId, ChildStorageFileId, StorageId, RestChildren, MarkLeaves).

-spec list_internal(root_id(), undefined | datastore_links_iter:token(), non_neg_integer()) ->
    {{ok, [link()]}, datastore_links_iter:token()} | {error, term()}.
list_internal(RootId, Token, Limit) ->
    Token2 = utils:ensure_defined(Token, #link_token{}),
    Opts = #{token => Token2},
    Opts2 = case Limit of
        all -> Opts;
        _ -> Opts#{size => Limit}
    end,
    list_internal(RootId, Opts2).

-spec list_internal(root_id(), datastore_links_iter:fold_opts()) ->
    {{ok, [link()]}, datastore_links_iter:token()} | {error, term()}.
list_internal(RootId, Opts) ->
    Result = for_each(RootId, fun({ChildName, ChildTreeRootId}, FilesAcc) ->
        [{ChildName, ChildTreeRootId} | FilesAcc]
    end, [], Opts),
    case Result of
        {{ok, ChildrenReversed}, NewToken} ->
            {{ok, lists:reverse(ChildrenReversed)}, NewToken};
        Error = {error, _} ->
            Error
    end.

-spec delete_recursive_internal(root_id()) -> ok.
delete_recursive_internal(RootId) ->
    delete_recursive_internal(RootId, #link_token{}).

-spec delete_recursive_internal(undefined | root_id(), datastore_links_iter:token()) -> ok.
delete_recursive_internal(RootId, Token) ->
    case list_internal(RootId, Token, 1000) of
        {{ok, Children}, Token2} ->
            delete_children(RootId, Children),
            case Token2#link_token.is_last of
                true ->
                    ok;
                false ->
                    delete_recursive_internal(RootId, Token2)
            end;
        {error, not_found} ->
            ok
    end.

-spec delete_children(root_id(), [link()]) -> ok.
delete_children(_RootId, []) ->
    ok;
delete_children(RootId, [{ChildName, undefined} | Rest]) ->
    delete_link_internal(RootId, ChildName),
    delete_children(RootId, Rest);
delete_children(RootId, [{ChildName, ChildRootId} | Rest]) ->
    delete_recursive_internal(ChildRootId),
    delete_link_internal(RootId, ChildName),
    delete_children(RootId, Rest).

-spec delete_link_internal(root_id(), link_name()) -> ok.
delete_link_internal(RootId, ChildName) ->
    TreeId = oneprovider:get_id(),
    case datastore_model:delete_links(?CTX, RootId, TreeId, ChildName) of
        [] -> ok;
        ok -> ok
    end.

-spec for_each(root_id(), fold_fun(), Acc0 :: term(), datastore:fold_opts()) ->
    {{ok, Acc :: term()}, datastore_links_iter:token()} | {error, term()}.
for_each(RootId, Callback, Acc0, Opts) ->
    datastore_model:fold_links(?CTX, RootId, all,
        fun(#link{name = StorageFileId, target = Target}, Acc) ->
            {ok, Callback({StorageFileId, Target}, Acc)}
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
