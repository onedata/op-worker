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
-module(datasets_structure).
-author("Jakub Kudzia").

-include("global_definitions.hrl").
-include("modules/dataset/dataset.hrl").
-include("modules/datastore/datastore_models.hrl").
-include_lib("ctool/include/errors.hrl").
-include_lib("ctool/include/logging.hrl").

% TODO VFS-7363 add docs & specs




% TODO trzeba bedzie zrobic moduł, który przykryje mi drzewo i korzystac z niego do implementacji drzewa attached
% i detached

%% API
-export([add/4, get/3, delete/3, list_space/3, list/4, move/5]).

%% Test API
-export([list_all_unsafe/2, delete_all_unsafe/2]).

-type link_name() :: dataset:path().
-type link_value() :: dataset:id().
-type encoded_link_value() :: binary().
-type link() :: {link_name(), link_value()}.
-type internal_link() :: datastore_links:link().
-type link_revision() :: datastore_links:link_rev().

-type entry() :: {file_meta:uuid(), file_meta:name()}.
-type entries() :: [entry()].



-type fold_acc() :: term().
-type fold_fun() :: fun((internal_link(), fold_acc()) -> {ok | stop, fold_acc()} | {error, term()}).

-type tree_id() :: oneprovider:id().
-type forest_type() :: binary(). % ?ATTACHED_FOREST | ?DETACHED_FOREST


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
-define(FOREST(Type, SpaceId), str_utils:join_binary([<<"DATASETS">>, Type, SpaceId], ?SEPARATOR)).
-define(LOCAL_TREE_ID, oneprovider:get_id()).
-define(LINK(Name, Value), {Name, Value}).
-define(DEFAULT_BATCH_SIZE, application:get_env(?APP_NAME, ls_batch_size, 5000)).


-define(SEPARATOR, <<"###">>).

% todo czym jest link_value ??
% TODO MOVE musi ogarnac, ze moze sie zmienic guid pliku

% todo trzeba uwazac kiedy podawac sciezke, a keidy nie? bo mzoe juz byc nieaktualna?
%%%===================================================================
%%% API functions
%%%===================================================================

add(SpaceId, ForestType, DatasetPath, DatasetId) ->
    Link = ?LINK(DatasetPath, DatasetId),
    case datastore_model:add_links(?CTX(SpaceId), ?FOREST(ForestType, SpaceId), ?LOCAL_TREE_ID, Link) of
        {ok, _} -> ok;
        {error, already_exists} -> ok
    end.


-spec get(od_space:id(), forest_type(), link_name()) ->
    {ok, link_value()} | {error, term()}.
get(SpaceId, ForestType, DatasetPath) ->
    case datastore_model:get_links(?CTX(SpaceId), ?FOREST(ForestType, SpaceId), all, DatasetPath) of
        {ok, [#link{target = DatasetId}]} ->
            {ok, DatasetId};
        Error = {error, _} ->
            Error
    end.


delete(SpaceId, ForestType, DatasetPath) ->
    case datastore_model:get_links(?CTX, ?FOREST(ForestType, SpaceId), all, DatasetPath) of
        {ok, [#link{tree_id = TreeId, name = DatasetPath, rev = Rev}]} ->
            % TODO VFS-7363 do we need to check Rev?
            % pass Rev to ensure that link with the same Rev is deleted
            case oneprovider:is_self(TreeId) of
                true -> delete_local(SpaceId, ForestType, DatasetPath, Rev);
                false -> delete_remote(SpaceId, ForestType, TreeId, DatasetPath, Rev)
            end;
        ?ERROR_NOT_FOUND ->
            ok
    end.


-spec list_space(od_space:id(), forest_type(), opts()) -> {ok, [link_value()], IsLast :: boolean()}.
list_space(SpaceId, ForestType, Opts) ->
    list_internal(SpaceId, ForestType, undefined, Opts).


-spec list(od_space:id(), forest_type(), link_name(), opts()) -> {ok, [link_value()], IsLast :: boolean()}.
list(SpaceId, ForestType, DatasetPath, Opts) ->
    list_internal(SpaceId, ForestType, DatasetPath, Opts).


move(SpaceId, ForestType, DatasetId, SourceDatasetPath, TargetDatasetPath) ->
    {ok, NestedDatasetPaths} = get_all_nested_datasets(SpaceId, ForestType, SourceDatasetPath),

    % move link to moved dataset
    delete(SpaceId, ForestType, SourceDatasetPath),
    add(SpaceId, ForestType, TargetDatasetPath, DatasetId),

    % move links to nested datasets of the moved dataset
    PrefixLen = byte_size(SourceDatasetPath) + 1, % +1 is for slash
    lists:foreach(fun({DatasetPath, DatasetId}) ->
        delete(SpaceId, ForestType, DatasetPath),
        Suffix = binary:part(DatasetPath, PrefixLen, byte_size(DatasetPath) - PrefixLen),
        NewDatasetPath = filename:join(TargetDatasetPath, Suffix),
        add(SpaceId, ForestType, NewDatasetPath, DatasetId)
    end, NestedDatasetPaths).




%%-spec get_all_nested_datasets(od_space:id(), identifier()) -> {ok, [link_name()]}.
get_all_nested_datasets(SpaceId, ForestType, ParentDatasetPath) ->
    % TODO VFS-7363 use batches
    fold(SpaceId, ForestType, fun(#link{name = DatasetPath, target = DatasetId}, Acc) ->
        case is_prefix(ParentDatasetPath, DatasetPath) of
            true ->  {ok, [{DatasetPath, DatasetId} | Acc]};
            false -> {stop, Acc}
        end
    end, [], #{prev_link_name => ParentDatasetPath, offset => 1}).



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
-spec list_all_unsafe(od_space:id(), forest_type()) -> {ok, [{link_name(), link_value()}]}.
list_all_unsafe(SpaceId, ForestType) ->
    fold(SpaceId, ForestType, fun(#link{name = LinkName, target = LinkValue}, Acc) ->
        {ok, [{LinkName, LinkValue} | Acc]}
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


list_internal(SpaceId, ForestType, ListedDatasetPath, Opts) ->
    SanitizedOpts = sanitize_opts(Opts),
    {ok, ReversedDatasets, IsLast} = collect(SpaceId, ForestType, ListedDatasetPath, maps:get(limit, SanitizedOpts)),
    SortedDatasets = sort(ReversedDatasets),
    {StrippedDatasets, EndReached} = strip(SortedDatasets, Opts),
    {ok, StrippedDatasets, IsLast andalso EndReached}.


collect(SpaceId, ForestType, ListedDatasetPath, Limit) ->
    InternalOpts0 = #{size => Limit},
    IsSpaceListed = ListedDatasetPath =:= undefined,
    InternalOpts = case IsSpaceListed of
        true -> InternalOpts0#{offset => 0};
        false -> InternalOpts0#{offset => 1, prev_link_name => ListedDatasetPath}
    end,
    {ok, SpacePath} = dataset_path:get_space_path(SpaceId),
    {ok, {RevList, _, Count}} = fold(SpaceId, ForestType,
        fun(#link{name = DatasetPath, target = DatasetId}, {CollectedAcc, PrevIncludedDatasetPath, ListedCountAcc}) ->
            case IsSpaceListed orelse is_prefix(ListedDatasetPath, DatasetPath) of
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
                            {OkOrStop, {[DatasetId | CollectedAcc], DatasetPath, ListedCountAcc + 1}}
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


-spec is_prefix(binary(), binary()) -> boolean().
is_prefix(Prefix, String) ->
    str_utils:binary_starts_with(String, Prefix).


-spec fold(od_space:id(), forest_type(), fold_fun(), fold_acc(), opts()) ->
    {ok, fold_acc()} | {error, term()}.
fold(SpaceId, ForestType, Fun, AccIn, Opts) ->
    datastore_model:fold_links(?CTX(SpaceId), ?FOREST(ForestType, SpaceId), all, Fun, AccIn, Opts).


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