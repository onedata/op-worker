%%%-------------------------------------------------------------------
%%% @author Bartosz Walkowicz
%%% @copyright (C) 2021 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% Interface for managing datasets (requests are delegated to middleware_worker).
%%% @end
%%%-------------------------------------------------------------------
-module(opl_datasets).
-author("Bartosz Walkowicz").

-include("middleware/middleware.hrl").
-include("proto/oneprovider/provider_messages.hrl").  %% TODO remove

%% API
-export([
    list_top_datasets/5,
    list_children_datasets/4,
    establish/3,
    get_info/2,
    update/5,
    remove/2,
    get_file_eff_summary/2
]).

-type info() :: #dataset_info{}.
-type file_eff_summary() :: #file_eff_dataset_summary{}.

-export_type([info/0, file_eff_summary/0]).


%%%===================================================================
%%% API
%%%===================================================================


-spec list_top_datasets(
    session:id(),
    od_space:id(),
    dataset:state(),
    dataset_api:listing_opts(),
    undefined | dataset_api:listing_mode()
) ->
    {ok, {dataset_api:entries(), boolean()}} | errors:error().
list_top_datasets(SessionId, SpaceId, State, Opts, ListingMode) ->
    SpaceGuid = fslogic_uuid:spaceid_to_space_dir_guid(SpaceId),

    middleware_worker:exec(SessionId, SpaceGuid, #list_top_datasets{
        state = State,
        opts = Opts,
        mode = utils:ensure_defined(ListingMode, ?BASIC_INFO)
    }).


-spec list_children_datasets(
    session:id(),
    dataset:id(),
    dataset_api:listing_opts(),
    undefined | dataset_api:listing_mode()
) ->
    {ok, {dataset_api:entries(), boolean()}} | errors:error().
list_children_datasets(SessionId, DatasetId, Opts, ListingMode) ->
    SpaceGuid = dataset_id_to_space_guid(DatasetId),

    middleware_worker:exec(SessionId, SpaceGuid, #list_children_datasets{
        id = DatasetId,
        opts = Opts,
        mode = utils:ensure_defined(ListingMode, ?BASIC_INFO)
    }).


-spec establish(session:id(), lfm:file_key(), data_access_control:bitmask()) ->
    {ok, dataset:id()} | errors:error().
establish(SessionId, FileKey, ProtectionFlags) ->
    FileGuid = lfm_file_key:resolve_file_key(SessionId, FileKey, do_not_resolve_symlink),

    middleware_worker:exec(SessionId, FileGuid, #establish_dataset{
        protection_flags = ProtectionFlags
    }).


-spec get_info(session:id(), dataset:id()) ->
    {ok, info()} | errors:error().
get_info(SessionId, DatasetId) ->
    SpaceGuid = dataset_id_to_space_guid(DatasetId),

    middleware_worker:exec(SessionId, SpaceGuid, #get_dataset_info{id = DatasetId}).


-spec update(
    session:id(),
    dataset:id(),
    undefined | dataset:state(),
    data_access_control:bitmask(),
    data_access_control:bitmask()
) ->
    ok | errors:error().
update(SessionId, DatasetId, NewState, FlagsToSet, FlagsToUnset) ->
    SpaceGuid = dataset_id_to_space_guid(DatasetId),

    middleware_worker:exec(SessionId, SpaceGuid, #update_dataset{
        id = DatasetId,
        state = NewState,
        flags_to_set = FlagsToSet,
        flags_to_unset = FlagsToUnset
    }).


-spec remove(session:id(), dataset:id()) -> ok | errors:error().
remove(SessionId, DatasetId) ->
    SpaceGuid = dataset_id_to_space_guid(DatasetId),

    middleware_worker:exec(SessionId, SpaceGuid, #remove_dataset{id = DatasetId}).


-spec get_file_eff_summary(session:id(), lfm:file_key()) ->
    {ok, file_eff_summary()} | errors:error().
get_file_eff_summary(SessionId, FileKey) ->
    FileGuid = lfm_file_key:resolve_file_key(SessionId, FileKey, do_not_resolve_symlink),

    middleware_worker:exec(SessionId, FileGuid, #get_file_eff_dataset_summary{}).


%%%===================================================================
%%% Internal functions
%%%===================================================================


%% @private
-spec dataset_id_to_space_guid(dataset:id()) -> fslogic_worker:file_guid().
dataset_id_to_space_guid(DatasetId) ->
    {ok, SpaceId} = dataset:get_space_id(DatasetId),
    fslogic_uuid:spaceid_to_space_dir_guid(SpaceId).
