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
    update_attrs/2,
    list_space/3,
    list/3
]).

% TODO dodanie wiadomosci do protokolu i przeciagnieci operacji do lfm
% TODO sprawdzanie uprawnień
%%%===================================================================
%%% API functions
%%%===================================================================

establish(FileCtx, UserCtx) ->
    {ok, DatasetId} = dataset_api:establish(FileCtx),
    ?PROVIDER_OK_RESP(#dataset_established{id = DatasetId}).


reattach(DatasetId, UserCtx) ->
    ok = dataset_api:reattach(DatasetId),
    ?PROVIDER_OK_RESP.


detach(DatasetId, UserCtx) ->
    ok = dataset_api:detach(DatasetId),
    ?PROVIDER_OK_RESP.


remove(DatasetId, UserCtx) ->
    ok = dataset_api:remove(DatasetId),
    ?PROVIDER_OK_RESP.


get_attrs(DatasetId, UserCtx) ->
    {ok, AttrsMap} = dataset_api:get_info(DatasetId),
    ?PROVIDER_OK_RESP(#dataset_attrs{
        id = DatasetId,
        uuid = maps:get(<<"uuid">>, AttrsMap)
    }).


update_attrs(DatasetId, UserCtx) ->
    ok = dataset_api:update_attrs(DatasetId),
    ?PROVIDER_OK_RESP.


list_space(SpaceId, UserCtx, Opts) ->
    {ok, Datasets, IsLast} = dataset_api:list_top_datasets(SpaceId, Opts),
    ?PROVIDER_OK_RESP(#nested_datasets{datasets = Datasets, is_last = IsLast}).


list(Dataset, UserCtx, Opts) ->
    {ok, Datasets, IsLast} = dataset_api:list(Dataset, Opts),
    ?PROVIDER_OK_RESP(#nested_datasets{datasets = Datasets, is_last = IsLast}).


%%%===================================================================
%%% Internal functions
%%%===================================================================




