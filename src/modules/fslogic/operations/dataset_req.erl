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

-include("modules/dataset/dataset.hrl").
-include("modules/fslogic/fslogic_common.hrl").
-include("modules/fslogic/data_access_control.hrl").
-include("proto/oneprovider/provider_messages.hrl").
-include_lib("ctool/include/privileges.hrl").


%% API
-export([
    establish/3,
    update/6,
    remove/3,
    get_info/3,
    get_file_eff_summary/2,
    list_top_datasets/5,
    list_children_datasets/5
]).

%%%===================================================================
%%% API functions
%%%===================================================================

-spec establish(file_ctx:ctx(), data_access_control:bitmask(), user_ctx:ctx()) -> fslogic_worker:provider_response().
establish(FileCtx0, ProtectionFlags, UserCtx) ->
    assert_has_eff_privilege(FileCtx0, UserCtx, ?SPACE_MANAGE_DATASETS),
    FileCtx1 = fslogic_authz:ensure_authorized(UserCtx, FileCtx0, [?TRAVERSE_ANCESTORS]),

    establish_insecure(FileCtx1, ProtectionFlags).


-spec update(file_ctx:ctx(), dataset:id(), dataset:state() | undefined, data_access_control:bitmask(),
    data_access_control:bitmask(), user_ctx:ctx()) -> fslogic_worker:provider_response().
update(SpaceDirCtx, DatasetId, NewDatasetState, FlagsToSet, FlagsToUnset, UserCtx) ->
    assert_has_eff_privilege(SpaceDirCtx, UserCtx, ?SPACE_MANAGE_DATASETS),

    {ok, DatasetDoc} = dataset:get(DatasetId),
    FileCtx0 = dataset_api:get_associated_file_ctx(DatasetDoc),
    fslogic_authz:ensure_authorized(UserCtx, FileCtx0, [?TRAVERSE_ANCESTORS]),

    ok = dataset_api:update(DatasetDoc, NewDatasetState, FlagsToSet, FlagsToUnset),
    ?PROVIDER_OK_RESP.


-spec remove(file_ctx:ctx(), dataset:id(), user_ctx:ctx()) -> fslogic_worker:provider_response().
remove(SpaceDirCtx, DatasetId, UserCtx) ->
    assert_has_eff_privilege(SpaceDirCtx, UserCtx, ?SPACE_MANAGE_DATASETS),

    {ok, DatasetDoc} = dataset:get(DatasetId),
    {ok, CurrentState} = dataset:get_state(DatasetDoc),

    case CurrentState =:= ?ATTACHED_DATASET of
        true ->
            FileCtx0 = dataset_api:get_associated_file_ctx(DatasetDoc),
            fslogic_authz:ensure_authorized(UserCtx, FileCtx0, [?TRAVERSE_ANCESTORS]);
        false ->
            ok
    end,

    ok = dataset_api:remove(DatasetDoc),
    ?PROVIDER_OK_RESP.


-spec get_info(file_ctx:ctx(), dataset:id(), user_ctx:ctx()) -> fslogic_worker:provider_response().
get_info(SpaceDirCtx, DatasetId, UserCtx) ->
    assert_has_eff_privilege(SpaceDirCtx, UserCtx, ?SPACE_VIEW),

    {ok, Info} = dataset_api:get_info(DatasetId),
    ?PROVIDER_OK_RESP(Info).


-spec get_file_eff_summary(file_ctx:ctx(), user_ctx:ctx()) -> fslogic_worker:provider_response().
get_file_eff_summary(FileCtx0, UserCtx) ->
    assert_has_eff_privilege(FileCtx0, UserCtx, ?SPACE_VIEW),
    FileCtx1 = fslogic_authz:ensure_authorized(
        UserCtx, FileCtx0,
        [?TRAVERSE_ANCESTORS]
    ),

    {ok, Summary} = dataset_api:get_effective_summary(FileCtx1),
    ?PROVIDER_OK_RESP(Summary).


-spec list_top_datasets(od_space:id(), dataset:state(), user_ctx:ctx(), dataset_api:listing_opts(),
    dataset_api:listing_mode()) -> fslogic_worker:provider_response().
list_top_datasets(SpaceId, State, UserCtx, Opts, ListingMode) ->
    UserId = user_ctx:get_user_id(UserCtx),
    space_logic:assert_has_eff_privilege(SpaceId, UserId, ?SPACE_VIEW),

    {ok, Datasets, IsLast} = dataset_api:list_top_datasets(SpaceId, State, Opts, ListingMode),
    ?PROVIDER_OK_RESP(#datasets{datasets = Datasets, is_last = IsLast}).


-spec list_children_datasets(file_ctx:ctx(), dataset:id(), user_ctx:ctx(), dataset_api:listing_opts(),
    dataset_api:listing_mode()) -> fslogic_worker:provider_response().
list_children_datasets(SpaceDirCtx, Dataset, UserCtx, Opts, ListingMode) ->
    assert_has_eff_privilege(SpaceDirCtx, UserCtx, ?SPACE_VIEW),

    {ok, Datasets, IsLast} = dataset_api:list_children_datasets(Dataset, Opts, ListingMode),
    ?PROVIDER_OK_RESP(#datasets{datasets = Datasets, is_last = IsLast}).

%%%===================================================================
%%% Internal functions
%%%===================================================================

-spec establish_insecure(file_ctx:ctx(), data_access_control:bitmask()) ->
    fslogic_worker:provider_response().
establish_insecure(FileCtx, ProtectionFlags) ->
    {ok, DatasetId} = dataset_api:establish(FileCtx, ProtectionFlags),
    ?PROVIDER_OK_RESP(#dataset_established{id = DatasetId}).


-spec assert_has_eff_privilege(file_ctx:ctx(), user_ctx:ctx(), privileges:space_privilege()) -> ok.
assert_has_eff_privilege(FileCtx, UserCtx, Privilege) ->
    UserId = user_ctx:get_user_id(UserCtx),
    SpaceId = file_ctx:get_space_id_const(FileCtx),
    space_logic:assert_has_eff_privilege(SpaceId, UserId, Privilege).