%%%-------------------------------------------------------------------
%%% @author Jakub Kudzia
%%% @copyright (C) 2021 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% This module is responsible for handing requests operating on datasets.
%%% @end
%%%-------------------------------------------------------------------
-module(dataset_req).
-author("Jakub Kudzia").

-include("modules/fslogic/fslogic_common.hrl").
-include("proto/oneprovider/provider_messages.hrl").


%% API
-export([
    establish/2,
    reattach/2,
    detach/2,
    remove/2,
    get_attrs/2,
    get_file_eff_summary/2,
    list_space/4,
    list/3
]).

% TODO sprawdzanie uprawnień
%%%===================================================================
%%% API functions
%%%===================================================================

-spec establish(file_ctx:ctx(), user_ctx:ctx()) -> fslogic_worker:provider_response().
establish(FileCtx, UserCtx) ->
    {ok, DatasetId} = dataset_api:establish(FileCtx),
    ?PROVIDER_OK_RESP(#dataset_established{id = DatasetId}).


-spec reattach(dataset:id(), user_ctx:ctx()) -> fslogic_worker:provider_response().
reattach(DatasetId, UserCtx) ->
    ok = dataset_api:reattach(DatasetId),
    ?PROVIDER_OK_RESP.


-spec detach(dataset:id(), user_ctx:ctx()) -> fslogic_worker:provider_response().
detach(DatasetId, UserCtx) ->
    ok = dataset_api:detach(DatasetId),
    ?PROVIDER_OK_RESP.


-spec remove(dataset:id(), user_ctx:ctx()) -> fslogic_worker:provider_response().
remove(DatasetId, UserCtx) ->
    ok = dataset_api:remove(DatasetId),
    ?PROVIDER_OK_RESP.


-spec get_attrs(dataset:id(), user_ctx:ctx()) -> fslogic_worker:provider_response().
get_attrs(DatasetId, UserCtx) ->
    {ok, Info} = dataset_api:get_info(DatasetId),
    ?PROVIDER_OK_RESP(#dataset_info{
        id = DatasetId,
        state = maps:get(<<"state">>, Info),
        guid = maps:get(<<"fileRootGuid">>, Info),
        path = maps:get(<<"fileRootPath">>, Info),
        type = maps:get(<<"fileRootType">>, Info),
        creation_time = maps:get(<<"creationTime">>, Info),
        parent = maps:get(<<"parentDatasetId">>, Info)
    }).


-spec get_file_eff_summary(file_ctx:ctx(), user_ctx:ctx()) -> fslogic_worker:provider_response().
get_file_eff_summary(FileCtx, UserCtx) ->
    {ok, Summary} = dataset_api:get_effective_summary(FileCtx),
    ?PROVIDER_OK_RESP(#file_eff_dataset_summary{
        direct_dataset = maps:get(<<"directDataset">>, Summary),
        eff_ancestor_datasets = maps:get(<<"effectiveAncestorDatasets">>, Summary)
    }).


-spec list_space(od_space:id(), dataset:state(), user_ctx:ctx(), datasets_structure:opts()) ->
    fslogic_worker:provider_response().
list_space(SpaceId, State, UserCtx, Opts) ->
    {ok, Datasets, IsLast} = dataset_api:list_top_datasets(SpaceId, State, Opts),
    ?PROVIDER_OK_RESP(#nested_datasets{datasets = Datasets, is_last = IsLast}).


-spec list(dataset:id(), user_ctx:ctx(), datasets_structure:opts()) ->
    fslogic_worker:provider_response().
list(Dataset, UserCtx, Opts) ->
    {ok, Datasets, IsLast} = dataset_api:list(Dataset, Opts),
    ?PROVIDER_OK_RESP(#nested_datasets{datasets = Datasets, is_last = IsLast}).