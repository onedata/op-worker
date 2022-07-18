%%%--------------------------------------------------------------------
%%% @author Michal Stanisz
%%% @copyright (C) 2022 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%--------------------------------------------------------------------
%%% @doc 
%%% This module implements `recursive_listing` behaviour callbacks to allow 
%%% for recursive listing of datasets tree structure. 
%%% For options description consult `recursive_listing` module doc.
%%% @end
%%%--------------------------------------------------------------------
-module(recursive_dataset_listing).
-author("Michal Stanisz").

-include("modules/fslogic/data_access_control.hrl").
-include("modules/dataset/dataset.hrl").
-include("proto/oneprovider/provider_messages.hrl").
-include_lib("ctool/include/logging.hrl").

-behaviour(recursive_listing_behaviour).

%% API
-export([list/3]).

%% `recursive_listing_behaviour` callbacks
-export([
    is_traversable_object/1,
    get_object_id/1,  get_object_path/1, get_object_name/2, get_parent_id/2,
    build_listing_opts/4,
    check_access/2, list_children_with_access_check/3,
    is_listing_finished/1
]).

-type object_id() :: binary(). % actually dataset:id(), but without undefined.
-type object() :: dataset_api:info().
-type name() :: file_meta:name().
-type path() :: file_meta:path().
-type object_listing_opts() :: #{
    offset => datasets_structure:offset(),
    start_index => datasets_structure:index(),
    limit => datasets_structure:limit(),
    is_finished => boolean()
}. % dataset_api:listing_opts() with additional is_finished field
-type pagination_token() :: recursive_listing:pagination_token().
-type result() :: recursive_listing:record().

% For detailed options description see module doc.
-type options() :: #{
    % NOTE: pagination_token and start_after_path are mutually exclusive
    pagination_token => pagination_token(),
    start_after_path => path(),
    prefix => recursive_listing:prefix(),
    limit => recursive_listing:limit()
}.

-export_type([result/0, pagination_token/0, options/0]).

%%%===================================================================
%%% API
%%%===================================================================

-spec list(user_ctx:ctx(), object(), options()) -> result().
list(UserCtx, DatasetInfo, ListOpts) ->
    recursive_listing:list(?MODULE, UserCtx, DatasetInfo, ListOpts#{include_traversable => true}).


%%%===================================================================
%%% `recursive_listing_behaviour` callbacks
%%%===================================================================

-spec is_traversable_object(object()) -> {boolean(), object()}.
is_traversable_object(DatasetInfo) ->
    {true, DatasetInfo}.


-spec get_object_id(object()) -> object_id().
get_object_id(#dataset_info{id = Id}) ->
    Id.


-spec get_object_path(object()) -> {path(), object()}.
get_object_path(#dataset_info{root_file_guid = RootFileGuid, id = Id} = DatasetInfo) ->
    SpaceId = file_id:guid_to_space_id(RootFileGuid),
    {ok, FileDoc} = file_meta:get(file_id:guid_to_uuid(RootFileGuid)),
    {ok, EffAncestorDatasets} = dataset_eff_cache:get_eff_ancestor_datasets(FileDoc),
    PathTokens = lists:map(fun(AncestorDatasetId) ->
        {ok, Info} = dataset_api:get_info(AncestorDatasetId),
        {Name, _} = get_object_name(Info, undefined),
        Name
    end, [Id | EffAncestorDatasets]),
    {filepath_utils:join([SpaceId | lists:reverse(PathTokens)]), DatasetInfo}.


-spec get_object_name(object(), user_ctx:ctx() | undefined) -> {name(), object()}.
get_object_name(#dataset_info{root_file_path = RootFilePath} = DatasetInfo, _UserCtx) ->
    {filename:basename(RootFilePath), DatasetInfo}.


-spec get_parent_id(object(), user_ctx:ctx()) -> object_id().
get_parent_id(#dataset_info{parent = ParentId}, _UserCtx) ->
    case ParentId of
        undefined -> <<>>;
        _ -> ParentId
    end.


-spec build_listing_opts(name(), recursive_listing:limit(), boolean(), object_id()) -> 
    object_listing_opts().
build_listing_opts(StartName, Limit, _IsContinuous, _ParentGuid) ->
    %% @TODO VFS-9678 - properly handle listing datasets with name conflicts
    StartIndex = datasets_structure:pack_entry_index(StartName, <<>>),
    #{limit => Limit, offset => 0, start_index => StartIndex, is_finished => false}.


-spec check_access(object(), user_ctx:ctx()) -> ok | {error, ?EACCES}.
check_access(_DatasetInfo, _UserCtx) ->
    ok.


-spec list_children_with_access_check(object(), object_listing_opts(), user_ctx:ctx()) ->
    {ok, [object()], object_listing_opts(), object()}.
list_children_with_access_check(#dataset_info{id = Id} = DatasetInfo, ListOpts, _UserCtx) ->
    % NOTE: no need to check access privileges as dataset listing is controlled only by space privs 
    % and should be checked on higher levels.
    {ok, {Children, IsLast}} = dataset_api:list_children_datasets(Id, ListOpts, ?EXTENDED_INFO),
    LastIndex = case Children of
        [] -> 
            <<>>;
        _ -> 
            #dataset_info{index = Index} = lists:last(Children),
            Index
    end,
    % set offset to 1 to ensure that listing is exclusive
    {ok, Children, #{is_finished => IsLast, offset => 1, start_index => LastIndex}, DatasetInfo}.


-spec is_listing_finished(object_listing_opts()) -> boolean().
is_listing_finished(#{is_finished := IsFinished}) ->
    IsFinished.
