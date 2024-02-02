%%%-------------------------------------------------------------------
%%% @author Katarzyna Such
%%% @copyright (C) 2024 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @doc
%%% Util functions for performing operations concerning storage import tests
%%% @end
%%%-------------------------------------------------------------------
-module(storage_import_utils).
-author("Katarzyna Such").

-include_lib("storage_import_utils.hrl").

%% API
-export([create_storage/2, create_space/1]).


create_storage(Provider, StorageRecord) ->
    {_Name, {ok, StorageId}} = panel_test_rpc:add_storage(Provider,
        get_posix_data_from_record(StorageRecord)),
    StorageId.


create_space(SpaceRecord) ->
    #space_spec{name = SpaceName, owner = Owner, users = Users,
        supports = Supports } = SpaceRecord,
    OwnerId = oct_background:get_user_id(Owner),
    SpaceId = ozw_test_rpc:create_space(OwnerId, atom_to_binary(SpaceName)),
    Token = ozw_test_rpc:create_space_support_token(OwnerId, SpaceId),
    {ok, SerializedToken} = tokens:serialize(Token),

    lists:foreach(fun(Support) ->
        #support_spec{provider = Provider, storage = StorageRecord,
            size = Size} = Support,
        StorageId = create_storage(Provider, StorageRecord),
        opw_test_rpc:support_space(Provider, StorageId, SerializedToken, Size)
    end, Supports),
    lists:foreach(fun(User) ->
        UserId = oct_background:get_user_id(User),
        ozw_test_rpc:add_user_to_space(SpaceId, UserId)
    end, Users).


%% @private
get_posix_data_from_record(StorageRecord) ->
    #storage_spec{name = Name, params = ParamsRecord} = StorageRecord,
    #posix_storage_params{type = Type, mountPoint = MountPoint} = ParamsRecord,
    #{atom_to_binary(Name) => #{<<"type">> => Type, <<"mountPoint">> => MountPoint}}.
