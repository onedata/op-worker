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

-include_lib("space_setup_utils.hrl").
-include_lib("ctool/include/test/test_utils.hrl").

%% API
-export([create_storage/2, set_up_space/1]).


-spec create_storage(oct_background:node_selector(), posix_storage_params()) -> binary().
create_storage(Provider, StorageSpec) ->
    {ok, StorageId} = panel_test_rpc:add_storage(Provider,
        build_create_storage_data(StorageSpec)),
    StorageId.


-spec support_space([support_spec()], binary()) -> ok.
support_space(Supports, SerializedToken) ->
    lists:foreach(fun(
        #support_spec{provider = Provider, storage_params = StorageSpec,size = Size}) ->
        StorageId = create_storage(Provider, StorageSpec),
        opw_test_rpc:support_space(Provider, StorageId, SerializedToken, Size)
    end, Supports),
    ok.


-spec add_users_to_space([oct_background:entity_selector()], oct_background:entity_id()) -> ok.
add_users_to_space(Users, SpaceId) ->
    lists:foreach(fun(User) ->
    UserId = oct_background:get_user_id(User),
    ozw_test_rpc:add_user_to_space(SpaceId, UserId)
    end, Users),
    ok.


-spec set_up_space(space_spec()) -> oct_background:entity_id().
set_up_space(SpaceSpec) ->
    #space_spec{name = SpaceName, owners = [Owner], users = Users, supports = Supports} = SpaceSpec,
    OwnerId = oct_background:get_user_id(Owner),
    SpaceId = ozw_test_rpc:create_space(OwnerId, atom_to_binary(SpaceName)),
    SerializedToken = ozw_test_rpc:create_space_support_token(OwnerId, SpaceId),

    support_space(Supports, SerializedToken),
    add_users_to_space(Users, SpaceId),

    SpaceId.


%% @private
build_create_storage_data(#posix_storage_params{mount_point = MountPoint}) ->
    Name = binary_to_atom(?RAND_STR()),
    #{atom_to_binary(Name) => #{<<"type">> => <<"posix">>, <<"mountPoint">> => MountPoint}}.
