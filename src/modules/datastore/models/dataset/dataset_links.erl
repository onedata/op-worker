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

%% API
-export([add/3, list_space/2, list/3, move/5, delete/2]).

%% Test API
-export([list_all_unsafe/1, delete_all_unsafe/1, get/2, fold/4]).

-type link_name() :: file_meta:uuid_based_path().
-type link_value() :: file_meta:name(). % todo jk is it needed?
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

-define(CTX, #{
    model => ?MODULE,
    sync_enabled => true,
    remote_driver => datastore_remote_driver,
    mutator => oneprovider:get_id_or_undefined(),
    local_links_tree_id => oneprovider:get_id_or_undefined()
}).

-define(CTX(Scope), ?CTX#{scope => Scope}).
-define(FOREST(SpaceId), <<"DATASETS###", SpaceId/binary>>).
-define(LOCAL_TREE_ID, oneprovider:get_id()).
-define(LINK(Name, Value), {Name, Value}).
-define(DEFAULT_BATCH_SIZE, application:get_env(?APP_NAME, ls_batch_size, 5000)).
-define(PATH(UuidBasedPath), {uuid_based_path, UuidBasedPath}).

-record(identifier, {
    uuid :: file_meta:uuid(),
    uuid_path :: file_meta:uuid_based_path()
}).

%%%===================================================================
%%% API functions
%%%===================================================================

add(SpaceId, UuidOrIdentifier, DatasetName) ->
    Identifier = ensure_identifier(UuidOrIdentifier, SpaceId),
    Link = ?LINK(Identifier#identifier.uuid_path, DatasetName),
    case datastore_model:add_links(?CTX(SpaceId), ?FOREST(SpaceId), ?LOCAL_TREE_ID, Link) of
        {ok, _} -> ok;
        {error, already_exists} -> ok
    end.


list_space(SpaceId, Opts) ->
    list_internal(SpaceId, undefined, Opts).

list(SpaceId, UuidOrIdentifier, Opts) ->
    Identifier = ensure_identifier(UuidOrIdentifier, SpaceId),
    list_internal(SpaceId, Identifier#identifier.uuid_path, Opts).


move(SpaceId, UuidOrIdentifier, NewName, SourceParentUuid, SourceParentUuid) ->
    ParentIdentifier = ensure_identifier(SourceParentUuid, SpaceId),
    RenamedDatasetIdentifier = child_identifier(ParentIdentifier, UuidOrIdentifier),
    rename(SpaceId, RenamedDatasetIdentifier, NewName);
move(SpaceId, UuidOrIdentifier, NewName, SourceParentUuid, TargetParentUuid) ->
    SourceParentIdentifier = ensure_identifier(SourceParentUuid, SpaceId),
    SourceIdentifier = child_identifier(SourceParentIdentifier, UuidOrIdentifier),
    SourceDatasetPath = SourceIdentifier#identifier.uuid_path,
    TargetParentIdentifier = ensure_identifier(TargetParentUuid, SpaceId),
    TargetIdentifier = child_identifier(TargetParentIdentifier, UuidOrIdentifier),
    TargetDatasetPath = TargetIdentifier#identifier.uuid_path,

    {ok, NestedDatasetPaths} = get_all_nested_datasets(SpaceId, SourceIdentifier),

    % move link to moved dataset
    delete(SpaceId, SourceIdentifier),
    add(SpaceId, TargetIdentifier, NewName),

    % move links to nested datasets of the moved dataset
    PrefixLen = byte_size(SourceDatasetPath) + 1, % +1 is for slash
    lists:foreach(fun({LinkName, Name}) ->
        delete(SpaceId, identifier_by_path(LinkName)),
        Suffix = binary:part(LinkName, PrefixLen, byte_size(LinkName) - PrefixLen),
        NewLinkName = filename:join(TargetDatasetPath, Suffix),
        add(SpaceId, identifier_by_path(NewLinkName), Name)
    end, NestedDatasetPaths).


-spec get_all_nested_datasets(od_space:id(), identifier()) -> {ok, [link_name()]}.
get_all_nested_datasets(SpaceId, Identifier) ->
    DatasetPath = Identifier#identifier.uuid_path,
    % TODO VFS-7363 use batches
    fold(SpaceId, fun(#link{name = Path, target = Name}, Acc) ->
        case is_prefix(DatasetPath, Path) of
            true ->  {ok, [{Path, Name} | Acc]};
            false -> {stop, Acc}
        end
    end, [], #{prev_link_name => DatasetPath, offset => 1}).


delete(SpaceId, UuidOrIdentifier) ->
    Identifier = ensure_identifier(UuidOrIdentifier, SpaceId),
    case get(SpaceId, Identifier#identifier.uuid_path) of
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
    fold(SpaceId, fun(#link{name = LinkName,target = DatasetName}, Acc) ->
        Uuid = lists:last(filename:split(LinkName)),
        % below case is needed because uuid_based_path() returned from paths_cache
        % has SpaceId instead of SpaceUuid which is inconsistent
        Uuid2 = case Uuid =:= SpaceId of
            true -> fslogic_uuid:spaceid_to_space_dir_uuid(SpaceId);
            false -> Uuid
        end,
        {ok, [{Uuid2, DatasetName} | Acc]}
    end, [], #{}).

%%%===================================================================
%%% Internal functions
%%%===================================================================

-spec link_name(file_meta:uuid(), od_space:id()) -> {ok, link_name()}.
link_name(Uuid, SpaceId) ->
    % todo maybe rename to dataset_path? or dataset_uuid_path?
    case fslogic_uuid:is_space_dir_uuid(Uuid) of
        true ->
            space_dir_link_name(SpaceId);
        false ->
            paths_cache:get_uuid_based(SpaceId, Uuid)
    end.


-spec space_dir_link_name(od_space:id()) -> {ok, link_name()}.
space_dir_link_name(SpaceId) ->
    paths_cache:get_uuid_based(SpaceId, fslogic_uuid:spaceid_to_space_dir_uuid(SpaceId)).


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
        fun(#link{name = DatasetPath, target = DatasetName}, {CollectedAcc, PrevIncludedDatasetPath, ListedCountAcc}) ->
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
                            {OkOrStop, Uuid} = case {IsSpaceListed, DatasetPath =:= SpacePath} of
                                {true, true} -> {stop, fslogic_uuid:spaceid_to_space_dir_uuid(SpaceId)};
                                {false, true} -> {ok, fslogic_uuid:spaceid_to_space_dir_uuid(SpaceId)};
                                {_, false} -> {ok, lists:last(filename:split(DatasetPath))}
                            end,
                            {OkOrStop, {[{Uuid, DatasetName} | CollectedAcc], DatasetPath, ListedCountAcc + 1}}
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



rename(SpaceId, UuidOrIdentifier, NewName) ->
    delete(SpaceId, UuidOrIdentifier),
    add(SpaceId, UuidOrIdentifier, NewName).


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


identifier_by_path(UuidBasedPath) ->
    #identifier{
        uuid_path = UuidBasedPath,
        uuid = lists:last(filename:split(UuidBasedPath))
    }.


ensure_identifier(Id = #identifier{}, _SpaceId) ->
    Id;
ensure_identifier(Uuid, SpaceId) ->
    {ok, UuidBasedPath} = link_name(Uuid, SpaceId),
    #identifier{
        uuid = Uuid,
        uuid_path = UuidBasedPath
    }.

child_identifier(_, ChildIdentifier = #identifier{}) ->
    ChildIdentifier;
child_identifier(#identifier{uuid_path = ParentPath}, ChildUuid) ->
    #identifier{
        uuid = ChildUuid,
        uuid_path = filename:join(ParentPath, ChildUuid)
    }.

