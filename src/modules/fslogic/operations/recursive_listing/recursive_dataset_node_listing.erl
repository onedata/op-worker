%%%--------------------------------------------------------------------
%%% @author Michal Stanisz
%%% @copyright (C) 2022 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%--------------------------------------------------------------------
%%% @doc 
%%% This module implements `recursive_listing_node_behaviour` behaviour callbacks to allow 
%%% for recursive listing of datasets tree structure. 
%%% Datasets are listed lexicographically ordered by path.
%%% For each such dataset returns dataset_info record along with the path to the dataset relative 
%%% to the top dataset.
%%% @end
%%%--------------------------------------------------------------------
-module(recursive_dataset_node_listing).
-author("Michal Stanisz").

-include("modules/fslogic/data_access_control.hrl").
-include("modules/dataset/dataset.hrl").
-include("proto/oneprovider/provider_messages.hrl").
-include_lib("ctool/include/logging.hrl").

-behaviour(recursive_listing_node_behaviour).

%% `recursive_listing_node_behaviour` callbacks
-export([
    is_branching_node/1,
    get_node_id/1, get_node_name/2, get_node_path_tokens/1, get_parent_id/2,
    init_node_iterator/4,
    get_next_batch/3,
    is_node_listing_finished/1
]).

-type node_id() :: binary(). % actually dataset:id(), but without undefined.
-type tree_node() :: dataset_api:info().
-type node_name() :: file_meta:node_name().
-type node_path() :: file_meta:node_path().
-type node_iterator() :: #{
    offset => datasets_structure:offset(),
    start_index => datasets_structure:index(),
    limit => datasets_structure:limit(),
    is_finished => boolean()
}. % dataset_api:listing_opts() with additional is_finished field
-type pagination_token() :: recursive_listing:pagination_token().
-type entry() :: recursive_listing:result_entry(node_path(), tree_node()).
-type result() :: recursive_listing:result(node_path(), entry()).

% For detailed options description see `recursive_listing` module doc.
-type options() :: #{
    % NOTE: pagination_token and start_after_path are mutually exclusive
    pagination_token => pagination_token(),
    start_after_path => node_path(),
    prefix => recursive_listing:prefix(),
    limit => recursive_listing:limit()
}.

-export_type([result/0, pagination_token/0, options/0]).

%%%===================================================================
%%% `recursive_listing` callbacks
%%%===================================================================

-spec is_branching_node(tree_node()) -> {boolean(), tree_node()}.
is_branching_node(DatasetInfo) ->
    {true, DatasetInfo}.


-spec get_node_id(tree_node()) -> node_id().
get_node_id(#dataset_info{id = Id}) ->
    Id.


-spec get_node_path_tokens(tree_node()) -> {[node_name()], tree_node()}.
get_node_path_tokens(#dataset_info{root_file_guid = RootFileGuid, id = Id} = DatasetInfo) ->
    SpaceId = file_id:guid_to_space_id(RootFileGuid),
    {ok, FileDoc} = file_meta:get(file_id:guid_to_uuid(RootFileGuid)),
    {ok, EffAncestorDatasets} = dataset_eff_cache:get_eff_ancestor_datasets(FileDoc),
    PathTokens = lists:map(fun(AncestorDatasetId) ->
        {ok, Info} = dataset_api:get_info(AncestorDatasetId),
        {Name, _} = get_node_name(Info, undefined),
        Name
    end, [Id | EffAncestorDatasets]),
    {[SpaceId | lists:reverse(PathTokens)], DatasetInfo}.


-spec get_node_name(tree_node(), user_ctx:ctx() | undefined) -> {node_name(), tree_node()}.
get_node_name(#dataset_info{root_file_path = RootFilePath} = DatasetInfo, _UserCtx) ->
    {filename:basename(RootFilePath), DatasetInfo}.


-spec get_parent_id(tree_node(), user_ctx:ctx()) -> node_id().
get_parent_id(#dataset_info{parent = ParentId}, _UserCtx) ->
    case ParentId of
        undefined -> <<>>;
        _ -> ParentId
    end.


-spec init_node_iterator(node_name(), recursive_listing:limit(), boolean(), node_id()) -> 
    node_iterator().
init_node_iterator(StartName, Limit, _IsContinuous, _ParentGuid) ->
    %% @TODO VFS-9678 - properly handle listing datasets with name conflicts
    StartIndex = datasets_structure:pack_entry_index(StartName, <<>>),
    #{limit => Limit, offset => 0, start_index => StartIndex, is_finished => false}.


-spec get_next_batch(tree_node(), node_iterator(), user_ctx:ctx()) ->
    {ok, [tree_node()], node_iterator(), tree_node()}.
get_next_batch(#dataset_info{id = Id} = DatasetInfo, ListOpts, _UserCtx) ->
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


-spec is_node_listing_finished(node_iterator()) -> boolean().
is_node_listing_finished(#{is_finished := IsFinished}) ->
    IsFinished.
