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
-include("modules/fslogic/data_access_control.hrl").
-include("proto/oneprovider/provider_messages.hrl").
-include_lib("ctool/include/privileges.hrl").


%% API
-export([
    establish/2,
    reattach/3,
    detach/3,
    remove/3,
    get_info/3,
    get_file_eff_summary/2,
    list_top_datasets/4,
    list/4
]).

% TODO sprawdzanie uprawnień
%%%===================================================================
%%% API functions
%%%===================================================================

-spec establish(file_ctx:ctx(), user_ctx:ctx()) -> fslogic_worker:provider_response().
establish(FileCtx0, UserCtx) ->
    space_logic:assert_has_eff_privilege(FileCtx0, UserCtx, ?SPACE_MANAGE_DATASETS),
    FileCtx1 = fslogic_authz:ensure_authorized(
        UserCtx, FileCtx0,
        [?TRAVERSE_ANCESTORS]
    ),

    establish_insecure(FileCtx1).


-spec reattach(file_ctx:ctx(), dataset:id(), user_ctx:ctx()) -> fslogic_worker:provider_response().
reattach(SpaceDirCtx, DatasetId, UserCtx) ->
    assert_has_eff_privilege(SpaceDirCtx, UserCtx, ?SPACE_MANAGE_DATASETS),

    ok = dataset_api:reattach(DatasetId),
    ?PROVIDER_OK_RESP.


-spec detach(file_ctx:ctx(), dataset:id(), user_ctx:ctx()) -> fslogic_worker:provider_response().
detach(SpaceDirCtx, DatasetId, UserCtx) ->
    assert_has_eff_privilege(SpaceDirCtx, UserCtx, ?SPACE_MANAGE_DATASETS),

    ok = dataset_api:detach(DatasetId),
    ?PROVIDER_OK_RESP.


-spec remove(file_ctx:ctx(), dataset:id(), user_ctx:ctx()) -> fslogic_worker:provider_response().
remove(SpaceDirCtx, DatasetId, UserCtx) ->
    assert_has_eff_privilege(SpaceDirCtx, UserCtx, ?SPACE_MANAGE_DATASETS),

    ok = dataset_api:remove(DatasetId),
    ?PROVIDER_OK_RESP.


-spec get_info(file_ctx:ctx(), dataset:id(), user_ctx:ctx()) -> fslogic_worker:provider_response().
get_info(SpaceDirCtx, DatasetId, UserCtx) ->
    assert_has_eff_privilege(SpaceDirCtx, UserCtx, ?SPACE_VIEW),

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
get_file_eff_summary(FileCtx0, UserCtx) ->
    assert_has_eff_privilege(FileCtx0, UserCtx, ?SPACE_VIEW),
    FileCtx1 = fslogic_authz:ensure_authorized(
        UserCtx, FileCtx0,
        [?TRAVERSE_ANCESTORS]
    ),

    {ok, Summary} = dataset_api:get_effective_summary(FileCtx1),
    ?PROVIDER_OK_RESP(#file_eff_dataset_summary{
        direct_dataset = maps:get(<<"directDataset">>, Summary),
        eff_ancestor_datasets = maps:get(<<"effectiveAncestorDatasets">>, Summary)
    }).


-spec list_top_datasets(od_space:id(), dataset:state(), user_ctx:ctx(), datasets_structure:opts()) ->
    fslogic_worker:provider_response().
list_top_datasets(SpaceId, State, UserCtx, Opts) ->
    UserId = user_ctx:get_user_id(UserCtx),
    space_logic:assert_has_eff_privilege(SpaceId, UserId, ?SPACE_VIEW),

    {ok, Datasets, IsLast} = dataset_api:list_top_datasets(SpaceId, State, Opts),
    ?PROVIDER_OK_RESP(#nested_datasets{datasets = Datasets, is_last = IsLast}).


-spec list(file_ctx:ctx(), dataset:id(), user_ctx:ctx(), datasets_structure:opts()) ->
    fslogic_worker:provider_response().
list(SpaceDirCtx, Dataset, UserCtx, Opts) ->
    assert_has_eff_privilege(SpaceDirCtx, UserCtx, ?SPACE_VIEW),

    {ok, Datasets, IsLast} = dataset_api:list(Dataset, Opts),
    ?PROVIDER_OK_RESP(#nested_datasets{datasets = Datasets, is_last = IsLast}).

%%%===================================================================
%%% Internal functions
%%%===================================================================

-spec establish_insecure(file_ctx:ctx()) -> fslogic_worker:provider_response().
establish_insecure(FileCtx) ->
    {ok, DatasetId} = dataset_api:establish(FileCtx),
    ?PROVIDER_OK_RESP(#dataset_established{id = DatasetId}).


-spec assert_has_eff_privilege(file_ctx:ctx(), user_ctx:ctx(), privileges:space_privilege()) -> ok.
assert_has_eff_privilege(FileCtx, UserCtx, Privilege) ->
    UserId = user_ctx:get_user_id(UserCtx),
    SpaceId = file_ctx:get_space_id_const(FileCtx),
    space_logic:assert_has_eff_privilege(SpaceId, UserId, Privilege).