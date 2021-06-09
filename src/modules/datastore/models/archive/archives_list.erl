%%%-------------------------------------------------------------------
%%% @author Jakub Kudzia
%%% @copyright (C) 2021 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% This module implements list structure which is used to store
%%% ids of archives created from the given dataset.
%%% Each dataset is associated with a forest in which each provider
%%% has its own tree.
%%% @end
%%%-------------------------------------------------------------------
-module(archives_list).
-author("Jakub Kudzia").

-include("global_definitions.hrl").
-include("modules/datastore/datastore_models.hrl").
-include("modules/datastore/datastore_runner.hrl").
-include_lib("ctool/include/errors.hrl").

%% API
-export([add/4, list/2, delete/4, index/2, length/1, is_empty/1]).

-define(CTX, (archive:get_ctx())).
-define(CTX(Scope), ?CTX#{scope => Scope}).
-define(FOREST(DatasetId), <<"ARCHIVES_LIST_", DatasetId/binary>>).
-define(LOCAL_TREE_ID, oneprovider:get_id()).
-define(LINK(LinkName, LinkValue), {LinkName, LinkValue}).

% link_name() consists of 2 parts:
%  * timestamp part - equal to ?EPOCH_INFINITY - Timestamp.
%    Thanks to that links are sorted in descending order by their timestamps (the newest is first).
%  * id part - which is basically id of an archive, this part allows to distinguish links associated
%    with archives created at the same second.
-type link_name() :: binary().
-type link_target() :: archive:id().
-type tree_id() :: od_provider:id().
-type forest() :: binary(). % ?FOREST(DatasetId)
-type index() :: link_name().
-type entry() :: {link_name(), link_target()}.
-type offset() :: integer().
-type limit() :: pos_integer().

-export_type([index/0, entry/0, opts/0]).

-type opts() :: #{
    offset => offset(),
    start_index => index(),
    limit => limit()
}.

%%%===================================================================
%%% API functions
%%%===================================================================

-spec add(dataset:id(), od_space:id(), archive:id(), archive:timestamp()) -> ok.
add(DatasetId, SpaceId, ArchiveId, Timestamp) ->
    Link = ?LINK(index(ArchiveId, Timestamp), ArchiveId),
    case datastore_model:add_links(?CTX(SpaceId), ?FOREST(DatasetId), ?LOCAL_TREE_ID, Link) of
        {ok, _} -> ok;
        {error, already_exists} -> ok
    end.


-spec delete(dataset:id(), od_space:id(), archive:id(), archive:timestamp()) -> ok.
delete(DatasetId, SpaceId, ArchiveId, Timestamp) ->
    Forest = ?FOREST(DatasetId),
    case datastore_model:get_links(?CTX, Forest, all, index(ArchiveId, Timestamp)) of
        {error, not_found} ->
            ok;
        {ok, Links} ->
            lists:foreach(fun(#link{tree_id = ProviderId, name = LinkName}) ->
                case oneprovider:is_self(ProviderId) of
                    true -> delete_local(Forest, SpaceId, LinkName);
                    false -> delete_remote(Forest, SpaceId, LinkName, ProviderId)
                end
            end, Links)
    end.


-spec list(dataset:id(), opts()) -> [entry()].
list(DatasetId, Opts) ->
    {ok, Archives} = fold_links(DatasetId, fun(Index, ArchiveId, Acc) ->
        [{Index, ArchiveId} | Acc]
    end, [], sanitize_listing_opts(Opts)),
    lists:reverse(Archives).


-spec index(archive:id(), archive:timestamp()) -> link_name().
index(ArchiveId, Timestamp) ->
    TimestampPart = (integer_to_binary(?EPOCH_INFINITY - Timestamp)),
    <<TimestampPart/binary, ArchiveId/binary>>.


-spec length(dataset:id()) -> non_neg_integer().
length(DatasetId) ->
    % TODO VFS-7613 use datastore function that returns number of links in a forest
    {ok, Length} = fold_links(DatasetId, fun(_, _, LengthAcc) -> LengthAcc + 1 end, 0, #{}),
    Length.


-spec is_empty(dataset:id()) -> boolean().
is_empty(DatasetId) ->
    % TODO VFS-7613 use datastore function that returns number of links in a forest
    {ok, IsEmpty} = fold_links(DatasetId, fun(_, _, _) -> false end, true, #{offset => 0, size => 1}),
    IsEmpty.

%%%===================================================================
%%% Internal functions
%%%===================================================================

-spec fold_links(
    dataset:id(),
    fun((link_name(), link_target(), Acc0 :: term()) -> Acc :: term()),
    Acc0 :: term(), datastore_model:fold_opts()) ->
    {ok, Acc :: term()} | {error, term()}.
fold_links(DatasetId, Callback, Acc0, Opts) ->
    datastore_model:fold_links(?CTX, ?FOREST(DatasetId), all,
        fun(#link{name = Name, target = Target}, Acc) -> {ok, Callback(Name, Target, Acc)} end,
        Acc0, Opts
    ).


-spec delete_local(forest(), od_space:id(), link_name()) -> ok.
delete_local(Forest, SpaceId, LinkName) ->
    ok = datastore_model:delete_links(?CTX(SpaceId), Forest, ?LOCAL_TREE_ID, LinkName).


-spec delete_remote(forest(), od_space:id(), link_name(), tree_id()) -> ok.
delete_remote(Forest, SpaceId, LinkName, ProviderId) ->
    ok = datastore_model:mark_links_deleted(?CTX(SpaceId), Forest, ProviderId, LinkName).


-spec sanitize_listing_opts(opts()) -> datastore_model:fold_opts().
sanitize_listing_opts(Opts) ->
    SanitizedOpts = try
        middleware_sanitizer:sanitize_data(Opts, #{
            at_least_one => #{
                offset => {integer, any},
                start_index => {binary, any}
            },
            optional => #{limit => {integer, {not_lower_than, 1}}}
        })
    catch _:_ ->
        %% TODO VFS-7208 do not catch after introducing API errors to fslogic
        throw(?EINVAL)
    end,

    kv_utils:copy_found([
        {offset, offset},
        {limit, size},
        {start_index, prev_link_name}
    ], SanitizedOpts).