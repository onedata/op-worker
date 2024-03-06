%%%-------------------------------------------------------------------
%%% @author Katarzyna Such
%%% @copyright (C) 2024 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @doc
%%% Utility functions for testing storage import.
%%% @end
%%%-------------------------------------------------------------------
-module(space_setup_utils).
-author("Katarzyna Such").

-include("modules/fslogic/fslogic_common.hrl").
-include_lib("space_setup_utils.hrl").
-include_lib("ctool/include/test/test_utils.hrl").

-type posix_storage_params() :: #posix_storage_params{}.
-type support_spec() :: #support_spec{}.
-type space_spec() :: #space_spec{}.

-export_type([posix_storage_params/0, support_spec/0]).

%% API
-export([create_storage/2, set_up_space/1]).


% TODO VFS-11796 do not use defined default parameters - add onepanel rpc instead
-define(SUPPORT_PARAMETERS, #support_parameters{
    accounting_enabled = false,
    dir_stats_service_enabled = true,
    dir_stats_service_status = disabled
}).


%%%===================================================================
%%% API functions
%%%===================================================================


-spec create_storage(oct_background:node_selector(), posix_storage_params()) -> od_storage:id().
create_storage(Provider, #posix_storage_params{mount_point = MountPoint}) ->
    ?assertMatch(ok, opw_test_rpc:call(Provider, filelib, ensure_path, [MountPoint])),
    panel_test_rpc:add_storage(Provider,
        #{?RAND_STR() => #{<<"type">> => <<"posix">>, <<"mountPoint">> => MountPoint}}).


-spec set_up_space(space_spec()) -> oct_background:entity_id().
set_up_space(SpaceSpec = #space_spec{
    name = SpaceName,
    owner = OwnerSelector,
    users = Users,
    supports = SupportSpecs
}) ->
    OwnerId = oct_background:get_user_id(OwnerSelector),
    SpaceId = ozw_test_rpc:create_space(OwnerId, atom_to_binary(SpaceName)),

    SupportToken = ozw_test_rpc:create_space_support_token(OwnerId, SpaceId),
    support_space(SupportSpecs, SupportToken),

    add_users_to_space(Users, SpaceId),
    force_fetch_entities(SpaceId, SpaceSpec),

    SpaceId.


%%%===================================================================
%%% Internal functions
%%%===================================================================


%% @private
-spec support_space([support_spec()], tokens:serialized()) -> ok.
support_space(SupportSpecs, SupportToken) ->
    lists:foreach(fun(#support_spec{provider = Provider, storage_spec = StorageSpec, size = Size}) ->
        StorageId = case is_binary(StorageSpec) of
            true -> StorageSpec;
            false -> create_storage(Provider, StorageSpec)
        end,
        opw_test_rpc:support_space(Provider, StorageId, SupportToken, Size, ?SUPPORT_PARAMETERS)
    end, SupportSpecs).


%% @private
-spec add_users_to_space([oct_background:entity_selector()], oct_background:entity_id()) -> ok.
add_users_to_space(Users, SpaceId) ->
    lists:foreach(fun(User) ->
        UserId = oct_background:get_user_id(User),
        ozw_test_rpc:add_user_to_space(SpaceId, UserId)
    end, Users).


%% @private
-spec force_fetch_entities(od_space:id(), space_spec()) -> ok.
force_fetch_entities(SpaceId, #space_spec{
    owner = OwnerSelector,
    users = Users
}) ->
    opt:force_fetch_entity(od_space, SpaceId),

    lists:foreach(fun(User) ->
        UserId = oct_background:get_user_id(User),
        opt:force_fetch_entity(od_user, UserId)
    end, [OwnerSelector | Users]).
