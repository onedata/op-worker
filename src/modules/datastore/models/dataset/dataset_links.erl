%%%-------------------------------------------------------------------
%%% @author Jakub Kudzia
%%% @copyright (C) 2021 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% 
%%% @end
%%%-------------------------------------------------------------------
-module(dataset_links).
-author("Jakub Kudzia").

-include("global_definitions.hrl").
-include("modules/datastore/datastore_models.hrl").
-include_lib("ctool/include/errors.hrl").
-include_lib("ctool/include/logging.hrl").

% TODO VFS-7363 add docs & specs




% TODO trzeba bedzie zrobic moduł, który przykryje mi drzewo i korzystac z niego do implementacji drzewa attached
% i detached

%% API
-export([add/4, list_space/2, list/2, move/6, delete/2]).

%% Test API
-export([list_all_unsafe/1, delete_all_unsafe/1]).

-type link_name() :: file_meta:uuid_based_path().
-type link_value() :: {dataset:id(), file_meta:name()}.
-type encoded_link_value() :: binary().
-type link() :: {link_name(), link_value()}.
-type internal_link() :: datastore_links:link().
-type link_revision() :: datastore_links:link_rev().

-type entry() :: {file_meta:uuid(), file_meta:name()}.
-type entries() :: [entry()].



-type fold_acc() :: term().
-type fold_fun() :: fun((internal_link(), fold_acc()) -> {ok | stop, fold_acc()} | {error, term()}).

-type tree_id() :: oneprovider:id().
-type forest() :: binary().


% @formatter:off

-type offset() :: integer().
-type start_index() :: binary().
-type limit() :: non_neg_integer().


-type opts() :: #{
    offset => offset(),
    start_index => start_index(),   % todo % czy na pewno ??
    limit => limit()
}.

% @formatter:on

-define(CTX, (dataset:get_ctx())).

-define(CTX(Scope), ?CTX#{scope => Scope}).
-define(FOREST(SpaceId), <<"DATASETS###", SpaceId/binary>>).
-define(LOCAL_TREE_ID, oneprovider:get_id()).
-define(LINK(Name, Value), {Name, Value}).
-define(DEFAULT_BATCH_SIZE, application:get_env(?APP_NAME, ls_batch_size, 5000)).


-define(SEPARATOR, <<"###">>).

% todo czym jest link_value ??
% TODO MOVE musi ogarnac, ze moze sie zmienic guid pliku

%%%===================================================================
%%% API functions
%%%===================================================================

add(SpaceId, DatasetId, Uuid, DatasetName) ->
    {ok, LinkName} = link_name(Uuid, SpaceId),
    add_internal(SpaceId, DatasetId, LinkName, DatasetName).


list_space(SpaceId, Opts) ->
    list_internal(SpaceId, undefined, Opts).


list(DatasetId, Opts) ->
    {ok, Doc} = dataset:get(DatasetId),
    {ok, Uuid} = dataset:get_uuid(Doc),
    {ok, SpaceId} = dataset:get_space_id(Doc),
    LinkName = link_name(Uuid, SpaceId),
    list_internal(SpaceId, LinkName, Opts).


move(SpaceId, DatasetId, Uuid, NewName, SourceParentUuid, SourceParentUuid) ->
    rename(SpaceId, DatasetId, Uuid, NewName);
move(SpaceId, DatasetId, Uuid, NewName, SourceParentUuid, TargetParentUuid) ->
    SourceParentLinkName = link_name(SourceParentUuid, SpaceId),
    TargetParentLinkName = link_name(TargetParentUuid, SpaceId),
    SourceLinkName = filename:join([SourceParentLinkName, Uuid]),
    TargetLinkName = filename:join([TargetParentLinkName, Uuid]),

    {ok, NestedDatasetPaths} = get_all_nested_datasets(SpaceId, SourceLinkName),

    % move link to moved dataset
    delete_internal(SpaceId, SourceLinkName),
    add_internal(SpaceId, DatasetId, TargetLinkName, NewName),

    % move links to nested datasets of the moved dataset
    PrefixLen = byte_size(SourceLinkName) + 1, % +1 is for slash
    lists:foreach(fun({LinkName, DatasetId, Name}) ->
        delete_internal(SpaceId, LinkName),
        Suffix = binary:part(LinkName, PrefixLen, byte_size(LinkName) - PrefixLen),
        NewLinkName = filename:join(TargetLinkName, Suffix),
        add_internal(SpaceId, DatasetId, NewLinkName, Name)
    end, NestedDatasetPaths).


-spec get_all_nested_datasets(od_space:id(), identifier()) -> {ok, [link_name()]}.
get_all_nested_datasets(SpaceId, LinkName) ->
    % TODO VFS-7363 use batches
    fold(SpaceId, fun(#link{name = Path, target = Name}, Acc) ->
        case is_prefix(LinkName, Path) of
            true ->  {ok, [{Path, Name} | Acc]};
            false -> {stop, Acc}
        end
    end, [], #{prev_link_name => LinkName, offset => 1}).


delete(SpaceId, Uuid) ->
    {ok, LinkName} = link_name(Uuid, SpaceId),
    delete_internal(SpaceId, LinkName).


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
-spec delete_all_unsafe(od_space:id()) -> ok.
delete_all_unsafe(SpaceId) ->
    {ok, AllLinks} = list_all_unsafe(SpaceId),
    lists:foreach(fun({Uuid, _}) ->
        ok = delete(SpaceId, Uuid)
    end, AllLinks).


%%--------------------------------------------------------------------
%% @doc
%% This functions lists all links associated with given space.
%% NOTE!!!
%% THIS FUNCTION IS MEANT TO BE USED ONLY IN TESTS!!!
%% DO NOT USE IN PRODUCTION CODE!!!
%% @end
%%--------------------------------------------------------------------
-spec list_all_unsafe(od_space:id()) -> {ok, [{file_meta:uuid(), link_value()}]}.
list_all_unsafe(SpaceId) ->
    fold(SpaceId, fun(#link{target = LinkValue}, Acc) ->
        {ok, [decode_link_value(LinkValue) | Acc]}
    end, [], #{}).

%%%===================================================================
%%% Internal functions
%%%===================================================================

add_internal(SpaceId, DatasetId, LinkName, DatasetName) ->
    LinkValue = encode_link_value(DatasetId, DatasetName),
    Link = ?LINK(LinkName, LinkValue),
    case datastore_model:add_links(?CTX(SpaceId), ?FOREST(SpaceId), ?LOCAL_TREE_ID, Link) of
        {ok, _} -> ok;
        {error, already_exists} -> ok
    end.


delete_internal(SpaceId, LinkName) ->
    case get(SpaceId, LinkName) of
        {ok, [#link{tree_id = TreeId, name = LinkName, rev = Rev}]} ->
            % TODO VFS-7363 do we need to check Rev?
            % pass Rev to ensure that link with the same Rev is deleted
            case oneprovider:is_self(TreeId) of
                true -> delete_local(SpaceId, LinkName, Rev);
                false -> delete_remote(SpaceId, TreeId, LinkName, Rev)
            end;
        ?ERROR_NOT_FOUND ->
            ok
    end.



-spec link_name(file_meta:uuid(), od_space:id()) -> {ok, link_name()}.
link_name(Uuid, SpaceId) ->
    paths_cache:get_uuid_based(SpaceId, Uuid).


-spec space_dir_link_name(od_space:id()) -> {ok, link_name()}.
space_dir_link_name(SpaceId) ->
    link_name(fslogic_uuid:spaceid_to_space_dir_uuid(SpaceId), SpaceId).


-spec get(od_space:id(), link_name()) ->
    {ok, [internal_link()]} | [{ok, [internal_link()]}] | {error, term()}.
get(SpaceId, LinkName) ->
    datastore_model:get_links(?CTX(SpaceId), ?FOREST(SpaceId), all, LinkName).


-spec delete_local(od_space:id(), link_name(), link_revision()) -> ok.
delete_local(SpaceId, LinkName, Revision) ->
    datastore_model:delete_links(?CTX(SpaceId), ?FOREST(SpaceId), ?LOCAL_TREE_ID, {LinkName, Revision}).


-spec delete_remote(od_space:id(), tree_id(), link_name(), link_revision()) -> ok.
delete_remote(SpaceId, TreeId, LinkName, Revision) ->
    ok = datastore_model:mark_links_deleted(?CTX(SpaceId), ?FOREST(SpaceId), TreeId, {LinkName, Revision}).


list_internal(SpaceId, ListedDatasetPath, Opts) ->
    SanitizedOpts = sanitize_opts(Opts),
    {ok, ReversedDatasets, IsLast} = collect(SpaceId, ListedDatasetPath, maps:get(limit, SanitizedOpts)),
    SortedDatasets = sort(ReversedDatasets),
    {StrippedDatasets, EndReached} = strip(SortedDatasets, Opts),
    {ok, StrippedDatasets, IsLast andalso EndReached}.



collect(SpaceId, ListedDatasetPath, Limit) ->
    InternalOpts0 = #{size => Limit},
    IsSpaceListed = ListedDatasetPath =:= undefined,
    InternalOpts = case IsSpaceListed of
        true -> InternalOpts0#{offset => 0};
        false -> InternalOpts0#{offset => 1, prev_link_name => ListedDatasetPath}
    end,
    {ok, SpacePath} = space_dir_link_name(SpaceId),
    {ok, {RevList, _, Count}} = fold(SpaceId,
        fun(#link{name = DatasetPath, target = LinkValue}, {CollectedAcc, PrevIncludedDatasetPath, ListedCountAcc}) ->
            case  IsSpaceListed orelse is_prefix(ListedDatasetPath, DatasetPath) of
                true ->
                    % todo CurrendDataset i LinkName mogą być równe w przypadku konfilktów, ogarnąć !!!
                    case
                        PrevIncludedDatasetPath =/= undefined
                        andalso is_prefix(PrevIncludedDatasetPath, DatasetPath)
                    of
                        true ->
                            % it is a nested dataset, skip it
                            {ok, {CollectedAcc, PrevIncludedDatasetPath, ListedCountAcc + 1}};
                        false ->
                            % below case is needed because uuid_based_path() returned from paths_cache
                            % has SpaceId instead of SpaceUuid which is inconsistent
                            OkOrStop = case IsSpaceListed andalso DatasetPath =:= SpacePath of
                                true -> stop;
                                false -> ok
                            end,
                            {OkOrStop, {[decode_link_value(LinkValue) | CollectedAcc], DatasetPath, ListedCountAcc + 1}}
                    end;
                false ->
                    % link does not start with the prefix, we can stop the fold
                    {stop, {CollectedAcc, DatasetPath, ListedCountAcc}}
            end
        end,
        {[], undefined, 0},
        InternalOpts
    ),
    {ok, RevList, Count < Limit}.


-spec sort([entries()]) -> [[entries()]].
sort(Datasets) ->
    lists:sort(fun({_, DatasetName1}, {_, DatasetName2}) ->
        DatasetName1 =< DatasetName2
    end, Datasets).


strip(SortedDatasets, Opts) ->
    % TODO-VFS-7363 use start_id for iterating using batches
    %%    StartId = maps:get(start_id, Opts, <<>>),
    Offset = maps:get(offset, Opts, 0),
    Limit = maps:get(limit, Opts),
    {lists:sublist(SortedDatasets, max(Offset + 1, 1), Limit), max(Offset + 1, 1) + Limit >= length(SortedDatasets)}.



rename(SpaceId, DatasetId, Uuid, NewName) ->
    {ok, LinkName} = link_name(Uuid, SpaceId),
    delete_internal(SpaceId, LinkName),
    add_internal(SpaceId, DatasetId, LinkName, NewName).


-spec is_prefix(binary(), binary()) -> boolean().
is_prefix(Prefix, String) ->
    str_utils:binary_starts_with(String, Prefix).



-spec fold(forest(), fold_fun(), fold_acc(), opts()) ->
    {ok, fold_acc()} | {error, term()}.
fold(SpaceId, Fun, AccIn, Opts) ->
    datastore_model:fold_links(?CTX(SpaceId), ?FOREST(SpaceId), all, Fun, AccIn, Opts).


-spec sanitize_opts(opts()) -> opts().
sanitize_opts(Opts) ->
    InternalOpts1 = #{limit => sanitize_limit(Opts)},
    InternalOpts2 = maps_utils:put_if_defined(InternalOpts1, offset, sanitize_offset(Opts)),
    InternalOpts3 = maps_utils:put_if_defined(InternalOpts2, start_index, sanitize_start_index(Opts)),
    validate_starting_opts(InternalOpts3).


-spec validate_starting_opts(opts()) -> opts().
validate_starting_opts(InternalOpts) ->
    % at least one of: offset, prev_link_name must be defined so that we know
    % were to start listing
    case map_size(maps:with([offset, start_id], InternalOpts)) > 0 of
        true -> InternalOpts;
        false ->
            %%  TODO VFS-7208 uncomment after introducing API errors to fslogic
            %% throw(?ERROR_MISSING_AT_LEAST_ONE_VALUE([offset, token, start_index])),
            throw(?EINVAL)
    end.


-spec sanitize_limit(opts()) -> limit().
sanitize_limit(Opts) ->
    case maps:get(limit, Opts, undefined) of
        undefined ->
            ?DEFAULT_BATCH_SIZE;
        Limit when is_integer(Limit) andalso Limit >= 0 ->
            Limit;
        %% TODO VFS-7208 uncomment after introducing API errors to fslogic
        %% Size when is_integer(Limit) ->
        %%     throw(?ERROR_BAD_VALUE_TOO_LOW(limit, 0));
        _ ->
            %% TODO VFS-7208 uncomment after introducing API errors to fslogic
            %% throw(?ERROR_BAD_VALUE_INTEGER(limit))
            throw(?EINVAL)
    end.


-spec sanitize_offset(opts()) -> offset() | undefined.
sanitize_offset(Opts) ->
    sanitize_offset(Opts, true).


-spec sanitize_offset(opts(), AllowNegative :: boolean()) -> offset() | undefined.
sanitize_offset(Opts, AllowNegative) ->
    case maps:get(offset, Opts, undefined) of
        undefined -> undefined;
        Offset when is_integer(Offset) ->
            StartIndex = maps:get(start_index, Opts, undefined),
            case {AllowNegative andalso StartIndex =/= undefined, Offset >= 0} of
                {true, _} ->
                    Offset;
                {false, true} ->
                    Offset;
                {false, false} ->
                    % if LastName is undefined, Offset cannot be negative
                    %% throw(?ERROR_BAD_VALUE_TOO_LOW(offset, 0));
                    throw(?EINVAL)
            end;
        _ ->
            %% TODO VFS-7208 uncomment after introducing API errors to fslogic
            %% throw(?ERROR_BAD_VALUE_INTEGER(offset))
            throw(?EINVAL)
    end.


-spec sanitize_start_index(opts()) -> start_index() | undefined.
sanitize_start_index(Opts) ->
    case maps:get(start_index, Opts, undefined) of
        undefined ->
            undefined;
        Binary when is_binary(Binary) ->
            Binary;
        _ ->
            %% TODO VFS-7208 uncomment after introducing API errors to fslogic
            %% throw(?ERROR_BAD_VALUE_BINARY(start_index))
            throw(?EINVAL)
    end.


-spec encode_link_value(dataset:id(), file_meta:name()) -> encoded_link_value().
encode_link_value(DatasetId, DatasetName) ->
    str_utils:join_binary([DatasetId, DatasetName], ?SEPARATOR).


-spec decode_link_value(encoded_link_value()) -> link_value().
decode_link_value(LinkValue) ->
    list_to_tuple(binary:split(LinkValue, ?SEPARATOR)).