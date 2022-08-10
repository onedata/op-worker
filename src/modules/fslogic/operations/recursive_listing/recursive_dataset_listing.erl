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

-behaviour(recursive_listing).

%% API
-export([list/3]).

%% `recursive_listing_behaviour` callbacks
-export([
    is_branching_node/1,
    get_node_id/1, get_node_name/2, get_node_path/1, get_parent_id/2,
    init_node_listing_state/4,
    list_children/3,
    is_node_listing_finished/1
]).

-type node_id() :: binary(). % actually dataset:id(), but without undefined.
-type tree_node() :: dataset_api:info().
-type node_name() :: file_meta:node_name().
-type node_path() :: file_meta:node_path().
-type node_listing_state() :: #{
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
    start_after_path => node_path(),
    prefix => recursive_listing:prefix(),
    limit => recursive_listing:limit()
}.

-export_type([result/0, pagination_token/0, options/0]).

%%%===================================================================
%%% API
%%%===================================================================

-spec list(user_ctx:ctx(), tree_node(), options()) -> result().
list(UserCtx, DatasetInfo, ListOpts) ->
    recursive_listing:list(?MODULE, UserCtx, DatasetInfo, ListOpts#{include_branching => true}).


%%%===================================================================
%%% `recursive_listing` callbacks
%%%===================================================================

-spec is_branching_node(tree_node()) -> {boolean(), tree_node()}.
is_branching_node(DatasetInfo) ->
    {true, DatasetInfo}.


-spec get_node_id(tree_node()) -> node_id().
get_node_id(#dataset_info{id = Id}) ->
    Id.


-spec get_node_path(tree_node()) -> {node_path(), tree_node()}.
get_node_path(#dataset_info{root_file_guid = RootFileGuid, id = Id} = DatasetInfo) ->
    SpaceId = file_id:guid_to_space_id(RootFileGuid),
    {ok, FileDoc} = file_meta:get(file_id:guid_to_uuid(RootFileGuid)),
    {ok, EffAncestorDatasets} = dataset_eff_cache:get_eff_ancestor_datasets(FileDoc),
    PathTokens = lists:map(fun(AncestorDatasetId) ->
        {ok, Info} = dataset_api:get_info(AncestorDatasetId),
        {Name, _} = get_node_name(Info, undefined),
        Name
    end, [Id | EffAncestorDatasets]),
    {filepath_utils:join([SpaceId | lists:reverse(PathTokens)]), DatasetInfo}.


-spec get_node_name(tree_node(), user_ctx:ctx() | undefined) -> {node_name(), tree_node()}.
get_node_name(#dataset_info{root_file_path = RootFilePath} = DatasetInfo, _UserCtx) ->
    {filename:basename(RootFilePath), DatasetInfo}.


-spec get_parent_id(tree_node(), user_ctx:ctx()) -> node_id().
get_parent_id(#dataset_info{parent = ParentId}, _UserCtx) ->
    case ParentId of
        undefined -> <<>>;
        _ -> ParentId
    end.


-spec init_node_listing_state(node_name(), recursive_listing:limit(), boolean(), node_id()) -> 
    node_listing_state().
init_node_listing_state(StartName, Limit, _IsContinuous, _ParentGuid) ->
    %% @TODO VFS-9678 - properly handle listing datasets with name conflicts
    StartIndex = datasets_structure:pack_entry_index(StartName, <<>>),
    #{limit => Limit, offset => 0, start_index => StartIndex, is_finished => false}.


-spec list_children(tree_node(), node_listing_state(), user_ctx:ctx()) ->
    {ok, [tree_node()], node_listing_state(), tree_node()}.
list_children(#dataset_info{id = Id} = DatasetInfo, ListOpts, _UserCtx) ->
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


-spec is_node_listing_finished(node_listing_state()) -> boolean().
is_node_listing_finished(#{is_finished := IsFinished}) ->
    IsFinished.
