%%%-------------------------------------------------------------------
%%% @author Jakub Kudzia
%%% @copyright (C) 2021 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% Module which implements generic datasets structure using datastore links.
%%% Each link is associated with exactly one dataset.
%%%
%%% Link value store dataset's name.
%%%
%%% Link names are of type dataset:path() which means that is a slash separated
%%% path to root file of a dataset, where each element is uuid of corresponding
%%% directory/file.
%%% e. g.
%%% Let's assume that there is a directory "c": /space1/a/b/c
%%% where uuids of the directories are as following:
%%%  * space1 - space1_uuid
%%%  * a - a_uuid
%%%  * b - b_uuid
%%%  * c - c_uuid
%%%
%%% If a dataset is established on this directory, it's path will be in format:
%%% /space1_uuid/a_uuid/b_uuid/c_uuid
%%%
%%% This format allows to easily determine parent-child relation between datasets.
%%% e. g.
%%% Let's assume that there is a file f created under the following path: /space1/a/b/c/d/e/f
%%% If we establish a dataset also on this file, we will have 2 links:
%%% * /space1_uuid/a_uuid/b_uuid/c_uuid
%%% * /space1_uuid/a_uuid/b_uuid/c_uuid/d_uuid/e_uuid/f_uuid
%%%
%%% Now, if we list top datasets in the space, we expect to see only dataset "c".
%%% If we list the dataset c though, we expect to see dataset "f" as
%%% its direct children (as there are no other datasets established on "d" neither on "e".
%%% @end
%%%-------------------------------------------------------------------
-module(datasets_structure).
-author("Jakub Kudzia").

-include("global_definitions.hrl").
-include("modules/dataset/dataset.hrl").
-include("modules/datastore/datastore_models.hrl").
-include_lib("ctool/include/errors.hrl").
-include_lib("ctool/include/logging.hrl").

%% API
-export([add/4, get/3, delete/3, list_top_datasets/3, list_children_datasets/4, move/5, pack_entry_index/2]).

%% Test API
-export([list_all_unsafe/2, delete_all_unsafe/2]).


-type link_name() :: dataset:path().
-type link_value() :: dataset:name().
-type link() :: {link_name(), link_value()}.
-type internal_link() :: datastore_links:link().
-type link_revision() :: datastore_links:link_rev().
-type list_mode() :: top | {children, ParentDatasetPath :: link_name()}.

-type entry() :: {dataset:id(), dataset:name(), index()}.
-type entries() :: [entry()].

-type fold_acc() :: term().
-type fold_fun() :: fun((internal_link(), fold_acc()) -> {ok | stop, fold_acc()} | {error, term()}).

-type tree_id() :: oneprovider:id().
-type forest_type() :: binary().

% @formatter:off

-type offset() :: integer().
-type index() :: binary().
-type limit() :: non_neg_integer().


-type opts() :: #{
    offset => offset(),
    start_index => index(),
    limit => limit()
}.

% @formatter:on

-export_type([opts/0, entries/0, entry/0, index/0]).

-define(CTX, (dataset:get_ctx())).

-define(CTX(Scope), ?CTX#{scope => Scope}).
-define(FOREST(Type, SpaceId), str_utils:join_binary([<<"DATASETS">>, Type, SpaceId], ?FOREST_SEP)).
-define(LOCAL_TREE_ID, oneprovider:get_id()).
-define(LINK(LinkName, LinkValue), {LinkName, LinkValue}).
-define(ENTRY(DatasetPath, DatasetName), {dataset_path:to_id(DatasetPath), DatasetName}).

-define(DEFAULT_BATCH_SIZE, op_worker:get_env(default_ls_batch_limit, 5000)).
-define(DEFAULT_OFFSET, 0).

-define(FOREST_SEP, <<"###">>).
-define(VALUE_SEP, <<"///">>).

%%%===================================================================
%%% API functions
%%%===================================================================

-spec add(od_space:id(), forest_type(), link_name(), link_value()) -> ok.
add(SpaceId, ForestType, DatasetPath, DatasetName) ->
    Link = ?LINK(DatasetPath, DatasetName),
    case datastore_model:add_links(?CTX(SpaceId), ?FOREST(ForestType, SpaceId), ?LOCAL_TREE_ID, Link) of
        {ok, _} -> ok;
        {error, already_exists} -> ok
    end.


-spec get(od_space:id(), forest_type(), link_name()) ->
    {ok, entry()} | {error, term()}.
get(SpaceId, ForestType, DatasetPath) ->
    case datastore_model:get_links(?CTX(SpaceId), ?FOREST(ForestType, SpaceId), all, DatasetPath) of
        {ok, [#link{target = LinkValue}]} ->
            {ok, build_entry(DatasetPath, LinkValue)};
        Error = {error, _} ->
            Error
    end.


-spec delete(od_space:id(), forest_type(), link_name()) -> ok.
delete(SpaceId, ForestType, DatasetPath) ->
    case datastore_model:get_links(?CTX, ?FOREST(ForestType, SpaceId), all, DatasetPath) of
        {ok, [#link{tree_id = TreeId, name = DatasetPath, rev = Rev}]} ->
            % pass Rev to ensure that link with the same Rev is deleted
            case oneprovider:is_self(TreeId) of
                true -> delete_local(SpaceId, ForestType, DatasetPath, Rev);
                false -> delete_remote(SpaceId, ForestType, TreeId, DatasetPath, Rev)
            end;
        ?ERROR_NOT_FOUND ->
            ok
    end.


-spec list_top_datasets(od_space:id(), forest_type(), opts()) -> {ok, entries(), IsLast :: boolean()}.
list_top_datasets(SpaceId, ForestType, Opts) ->
    list_internal(SpaceId, ForestType, top, Opts).


-spec list_children_datasets(od_space:id(), forest_type(), link_name(), opts()) -> {ok, entries(), IsLast :: boolean()}.
list_children_datasets(SpaceId, ForestType, DatasetPath, Opts) ->
    list_internal(SpaceId, ForestType, {children, DatasetPath}, Opts).


-spec move(od_space:id(), forest_type(), link_name(), link_name(), dataset:name()) -> ok.
move(SpaceId, ForestType, SourceDatasetPath, SourceDatasetPath, TargetName) ->
    % dataset path has not changed, only its name has been changed
    delete(SpaceId, ForestType, SourceDatasetPath),
    add(SpaceId, ForestType, SourceDatasetPath, TargetName);
move(SpaceId, ForestType, SourceDatasetPath, TargetDatasetPath, TargetName) ->
    % move link to moved dataset
    add(SpaceId, ForestType, TargetDatasetPath, TargetName),
    delete(SpaceId, ForestType, SourceDatasetPath),

    % move links to nested datasets of the moved dataset
    move_all_descendants(SpaceId, ForestType, SourceDatasetPath, TargetDatasetPath).


-spec pack_entry_index(dataset:name(), dataset:id()) -> index().
pack_entry_index(DatasetName, DatasetId) ->
    str_utils:join_binary([DatasetName, DatasetId], <<"#">>).

%%%===================================================================
%%% Test functions
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% This functions removes all links associated with given space.
%% NOTE!!!
%% THIS FUNCTION IS MEANT TO BE USED ONLY IN TESTS!!!
%% DO NOT USE IN PRODUCTION CODE!!!
%% @end
%%--------------------------------------------------------------------
-spec delete_all_unsafe(od_space:id(), forest_type()) -> ok.
delete_all_unsafe(SpaceId, ForestType) ->
    {ok, AllLinks} = list_all_unsafe(SpaceId, ForestType),
    lists:foreach(fun({DatasetPath, _}) ->
        ok = delete(SpaceId, ForestType, DatasetPath)
    end, AllLinks).


%%--------------------------------------------------------------------
%% @doc
%% This functions lists all links associated with given space.
%% NOTE!!!
%% THIS FUNCTION IS MEANT TO BE USED ONLY IN TESTS!!!
%% DO NOT USE IN PRODUCTION CODE!!!
%% @end
%%--------------------------------------------------------------------
-spec list_all_unsafe(od_space:id(), forest_type()) -> {ok, [{link_name(), entry()}]}.
list_all_unsafe(SpaceId, ForestType) ->
    fold(SpaceId, ForestType, fun(#link{name = LinkName, target = LinkValue}, Acc) ->
        {ok, [{LinkName, build_entry(LinkName, LinkValue)} | Acc]}
    end, [], #{}).

%%%===================================================================
%%% Internal functions
%%%===================================================================

-spec delete_local(od_space:id(), forest_type(), link_name(), link_revision()) -> ok.
delete_local(SpaceId, ForestType, LinkName, Revision) ->
    datastore_model:delete_links(?CTX(SpaceId), ?FOREST(ForestType, SpaceId), ?LOCAL_TREE_ID, {LinkName, Revision}).


-spec delete_remote(od_space:id(), forest_type(), tree_id(), link_name(), link_revision()) -> ok.
delete_remote(SpaceId, ForestType, TreeId, LinkName, Revision) ->
    ok = datastore_model:mark_links_deleted(?CTX(SpaceId), ?FOREST(ForestType, SpaceId), TreeId, {LinkName, Revision}).


-spec list_internal(od_space:id(), forest_type(), list_mode(), opts()) ->
    {ok, entries(), boolean()}.
list_internal(SpaceId, ForestType, ListMode, Opts) ->
    SanitizedOpts = sanitize_opts(Opts),
    Limit = maps:get(limit, SanitizedOpts),
    {ok, ReversedDatasets} = collect_children(SpaceId, ForestType, Limit, ListMode),
    SortedDatasets = sort(ReversedDatasets),
    {StrippedDatasets, EndReached} = strip(SortedDatasets, SanitizedOpts),
    {ok, StrippedDatasets, EndReached}.


-spec collect_children(od_space:id(), forest_type(), limit(), list_mode()) -> {ok, entries()}.
collect_children(SpaceId, ForestType, Limit, top) ->
    collect_children(SpaceId, ForestType, undefined, undefined, undefined, Limit, []);
collect_children(SpaceId, ForestType, Limit, {children, ListedDatasetPath}) ->
    collect_children(SpaceId, ForestType, ListedDatasetPath, undefined, ListedDatasetPath, Limit, []).


-spec collect_children(od_space:id(), forest_type(), link_name() | undefined, link_name() | undefined,
    link_name() | undefined, limit(), entries()) -> {ok, entries()}.
collect_children(SpaceId, ForestType, ListedDatasetPath, LastIncludedDatasetPath0, StartIndex, Limit, FinalListReversed) ->
    InternalOpts0 = #{size => Limit},
    InternalOpts = case StartIndex =:= undefined of
        true -> InternalOpts0;
        false -> InternalOpts0#{offset => 1, prev_link_name => StartIndex}
    end,
    IsSpaceListed = ListedDatasetPath =:= undefined,
    {ok, SpacePath} = dataset_path:get_space_path(SpaceId),
    {ok, {ReversedList, LastIncludedDatasetPath, LastProcessed, EndReached}} = fold(SpaceId, ForestType,
        fun(#link{name = DatasetPath, target = DatasetId},
            {CollectedAcc, PrevIncludedDatasetPath, _PrevProcessedDatasetPath, _EndReached}
        ) ->
            case IsSpaceListed orelse is_prefix(ListedDatasetPath, DatasetPath) of
                true ->
                    case ListedDatasetPath =:= DatasetPath of
                        true ->
                            % it is the dataset that is listed, skip it
                            {ok, {CollectedAcc, PrevIncludedDatasetPath, DatasetPath, false}};
                        false ->
                            case
                                    PrevIncludedDatasetPath =/= undefined andalso
                                    is_prefix(PrevIncludedDatasetPath, DatasetPath)
                            of
                                true ->
                                    % it is a nested dataset, skip it
                                    {ok, {CollectedAcc, PrevIncludedDatasetPath, DatasetPath, false}};
                                false ->
                                    % if entry of dataset attached to space directory is listed
                                    % we can stop listing
                                    SpaceDatasetListed = IsSpaceListed andalso DatasetPath =:= SpacePath,
                                    OkOrStop = case SpaceDatasetListed of
                                        true -> stop;
                                        false -> ok
                                    end,
                                    {OkOrStop, {[build_entry(DatasetPath, DatasetId) | CollectedAcc], DatasetPath, DatasetPath,
                                        SpaceDatasetListed}}

                            end
                    end;
                false ->
                    % link does not start with the prefix, we can stop the fold
                    {stop, {CollectedAcc, PrevIncludedDatasetPath, DatasetPath, true}}
            end
        end,
        {[], LastIncludedDatasetPath0, StartIndex, true},
        InternalOpts
    ),

    case EndReached of
        true ->
            {ok, ReversedList ++ FinalListReversed};
        false ->
            collect_children(SpaceId, ForestType, ListedDatasetPath, LastIncludedDatasetPath, LastProcessed, Limit,
                ReversedList ++ FinalListReversed)
    end.


-spec sort(entries()) -> entries().
sort(Datasets) ->
    lists:sort(fun({DatasetId1, DatasetName1, _}, {DatasetId2, DatasetName2, _}) ->
        DatasetName1 < DatasetName2 orelse (DatasetName1 =:= DatasetName2 andalso DatasetId1 =< DatasetId2)
    end, Datasets).


-spec strip(entries(), opts()) -> {entries(), EndReached :: boolean()}.
strip(Entries, Opts) ->
    StartIndex = maps:get(start_index, Opts, <<>>),
    Offset = maps:get(offset, Opts, 0),
    Limit = maps:get(limit, Opts),
    Length = length(Entries),
    StartingPoint = lists_utils:foldl_while(fun
        ({_DatasetId, _DatasetName, Index}, AccOffset) ->
            case StartIndex =< Index of
                true -> {halt, AccOffset};
                false -> {cont, AccOffset + 1}
            end
    end, 0, Entries),
    FinalOffset = max(StartingPoint + Offset, 0) + 1,
    case FinalOffset > Length of
        true ->
            {[], true};
        false ->
            {lists:sublist(Entries, FinalOffset, Limit), FinalOffset + Limit > Length}
    end.


-spec move_all_descendants(od_space:id(), forest_type(), link_name(), link_name()) -> ok.
move_all_descendants(SpaceId, ForestType, SourceDatasetPath, TargetDatasetPath) ->
    move_all_descendants(SpaceId, ForestType, SourceDatasetPath, TargetDatasetPath, SourceDatasetPath).


-spec move_all_descendants(od_space:id(), forest_type(), link_name(), link_name(), index()) -> ok.
move_all_descendants(SpaceId, ForestType, SourceDatasetPath, TargetDatasetPath, StartIndex) ->
    {ok, DescendantDatasetsReversed, AllListed} = get_descendants_batch_reversed(SpaceId, ForestType, SourceDatasetPath,
        StartIndex, ?DEFAULT_BATCH_SIZE),
    move_descendants_batch(SpaceId, ForestType, SourceDatasetPath, TargetDatasetPath, DescendantDatasetsReversed),
    case AllListed of
        true ->
            ok;
        false ->
            {NextStartIndex, _} = hd(DescendantDatasetsReversed),
            move_all_descendants(SpaceId, ForestType, SourceDatasetPath, TargetDatasetPath, NextStartIndex)
    end.


-spec move_descendants_batch(od_space:id(), forest_type(), dataset:path(), dataset:path(), [link()]) -> ok.
move_descendants_batch(SpaceId, ForestType, SourceDatasetPath, TargetDatasetPath, DescendantDatasets) ->
    PrefixLen = byte_size(SourceDatasetPath) + 1, % +1 is for slash
    lists:foreach(fun({DatasetPath, DatasetName}) ->
        Suffix = binary:part(DatasetPath, PrefixLen, byte_size(DatasetPath) - PrefixLen),
        NewDatasetPath = filename:join(TargetDatasetPath, Suffix),
        add(SpaceId, ForestType, NewDatasetPath, DatasetName),
        delete(SpaceId, ForestType, DatasetPath)
    end, DescendantDatasets).


%%--------------------------------------------------------------------
%% @private
%% @doc
%% This function returns batch of descendant datasets in a reverse order.
%% Descendant means that it may not be just direct children but
%% all datasets which paths start with prefix ParentDatasetPath.
%% @end
%%--------------------------------------------------------------------
-spec get_descendants_batch_reversed(od_space:id(), forest_type(), link_name(), index(), limit()) ->
    {ok, [link()], AllListed :: boolean()}.
get_descendants_batch_reversed(SpaceId, ForestType, ParentDatasetPath, StartIndex, Limit) ->
    {ok, {LinksReversed, EndReached, ListedLinksCount}} = fold(SpaceId, ForestType,
        fun(#link{name = DatasetPath, target = DatasetName}, {ListAcc, _EndReached, ListedLinksCount}) ->
            case {is_prefix(ParentDatasetPath, DatasetPath), ParentDatasetPath =/= DatasetPath} of
                {true, true} ->  {ok, {[{DatasetPath, DatasetName} | ListAcc], false, ListedLinksCount + 1}};
                {true, false} ->  {ok, {ListAcc, false, ListedLinksCount + 1}};
                {false, _} -> {stop, {ListAcc, true, ListedLinksCount + 1}}
            end
        end, {[], false, 0}, #{prev_link_name => StartIndex, size => Limit}),

    {ok, LinksReversed, EndReached orelse ListedLinksCount < Limit}.


-spec is_prefix(binary(), binary()) -> boolean().
is_prefix(Prefix, String) ->
    str_utils:binary_starts_with(String, Prefix).


-spec fold(od_space:id(), forest_type(), fold_fun(), fold_acc(), datastore_model:fold_opts()) ->
    {ok, fold_acc()} | {error, term()}.
fold(SpaceId, ForestType, Fun, AccIn, Opts) ->
    datastore_model:fold_links(?CTX(SpaceId), ?FOREST(ForestType, SpaceId), all, Fun, AccIn, Opts).


-spec sanitize_opts(opts()) -> opts().
sanitize_opts(Opts) ->
    % TODO VFS-7560 try to remove code duplication in this and file_meta_forest modules
    InternalOpts1 = #{limit => sanitize_limit(Opts)},
    InternalOpts2 = InternalOpts1#{offset => sanitize_offset(Opts)},
    maps_utils:put_if_defined(InternalOpts2, start_index, sanitize_start_index(Opts)).


-spec sanitize_limit(opts()) -> limit().
sanitize_limit(Opts) ->
    case maps:get(limit, Opts, undefined) of
        undefined ->
            ?DEFAULT_BATCH_SIZE;
        Limit when is_integer(Limit) andalso Limit > 0 ->
            Limit;
        Limit when is_integer(Limit) ->
            throw(?ERROR_BAD_VALUE_TOO_LOW(limit, 1));
        _ ->
            throw(?ERROR_BAD_VALUE_INTEGER(limit))
    end.


-spec sanitize_offset(opts()) -> offset().
sanitize_offset(Opts) ->
    sanitize_offset(Opts, true).


-spec sanitize_offset(opts(), AllowNegative :: boolean()) -> offset().
sanitize_offset(Opts, AllowNegative) ->
    case maps:get(offset, Opts, ?DEFAULT_OFFSET) of
        Offset when is_integer(Offset) ->
            StartIndex = maps:get(start_index, Opts, undefined),
            case {AllowNegative andalso StartIndex =/= undefined, Offset >= 0} of
                {true, _} ->
                    Offset;
                {false, true} ->
                    Offset;
                {false, false} ->
                    % if LastName is undefined, Offset cannot be negative
                    throw(?ERROR_BAD_VALUE_TOO_LOW(offset, 0))
            end;
        _ ->
            throw(?ERROR_BAD_VALUE_INTEGER(offset))
    end.


-spec sanitize_start_index(opts()) -> index() | undefined.
sanitize_start_index(Opts) ->
    case maps:get(start_index, Opts, undefined) of
        undefined ->
            undefined;
        Binary when is_binary(Binary) ->
            Binary;
        _ ->
            throw(?ERROR_BAD_VALUE_BINARY(start_index))
    end.


-spec build_entry(dataset:path(), dataset:name()) -> entry().
build_entry(DatasetPath, DatasetName) ->
    DatasetId = dataset_path:to_id(DatasetPath),
    {dataset_path:to_id(DatasetPath), DatasetName, pack_entry_index(DatasetName, DatasetId)}.