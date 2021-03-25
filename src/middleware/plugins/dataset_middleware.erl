%%%-------------------------------------------------------------------
%%% @author Bartosz Walkowicz
%%% @copyright (C) 2021 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% This module handles middleware operations (create, get, update, delete)
%%% corresponding to datasets.
%%% @end
%%%-------------------------------------------------------------------
-module(dataset_middleware).
-author("Bartosz Walkowicz").

-behaviour(middleware_plugin).

-include("middleware/middleware.hrl").
-include("modules/dataset/dataset.hrl").
-include("modules/fslogic/data_access_control.hrl").
-include("modules/logical_file_manager/lfm.hrl").
-include_lib("ctool/include/errors.hrl").

-export([
    operation_supported/3,
    data_spec/1,
    fetch_entity/1,
    authorize/2,
    validate/2
]).
-export([create/1, get/2, update/1, delete/1]).


-define(ALL_PROTECTION_FLAGS, [?DATA_PROTECTION_BIN, ?METADATA_PROTECTION_BIN]).


%%%===================================================================
%%% API
%%%===================================================================


%%--------------------------------------------------------------------
%% @doc
%% {@link middleware_plugin} callback operation_supported/3.
%% @end
%%--------------------------------------------------------------------
-spec operation_supported(middleware:operation(), gri:aspect(),
    middleware:scope()) -> boolean().
operation_supported(create, instance, private) -> true;

operation_supported(get, instance, private) -> true;

operation_supported(update, instance, private) -> true;

operation_supported(delete, instance, private) -> true;

operation_supported(_, _, _) -> false.


%%--------------------------------------------------------------------
%% @doc
%% {@link middleware_plugin} callback data_spec/1.
%% @end
%%--------------------------------------------------------------------
-spec data_spec(middleware:req()) -> undefined | middleware_sanitizer:data_spec().
data_spec(#op_req{operation = create, gri = #gri{aspect = instance}}) -> #{
    required => #{
        <<"rootFileId">> => {binary, fun(ObjectId) ->
            {true, middleware_utils:decode_object_id(ObjectId, <<"rootFileId">>)}
        end}
    },
    optional => #{
        <<"protectionFlags">> => {list_of_binaries, ?ALL_PROTECTION_FLAGS}
    }
};

data_spec(#op_req{operation = get, gri = #gri{aspect = instance}}) ->
    undefined;

data_spec(#op_req{operation = update, gri = #gri{aspect = instance}}) -> #{
    at_least_one => #{
        <<"state">> => {atom, [?ATTACHED_DATASET, ?DETACHED_DATASET]},
        <<"setProtectionFlags">> => {list_of_binaries, ?ALL_PROTECTION_FLAGS},
        <<"unsetProtectionFlags">> => {list_of_binaries, ?ALL_PROTECTION_FLAGS}
    }
};

data_spec(#op_req{operation = delete, gri = #gri{aspect = instance}}) ->
    undefined.


%%--------------------------------------------------------------------
%% @doc
%% {@link middleware_plugin} callback fetch_entity/1.
%% @end
%%--------------------------------------------------------------------
-spec fetch_entity(middleware:req()) ->
    {ok, middleware:versioned_entity()} | errors:error().
fetch_entity(#op_req{auth = ?NOBODY}) ->
    ?ERROR_UNAUTHORIZED;

fetch_entity(#op_req{operation = Op, auth = ?USER(_UserId), gri = #gri{
    id = DatasetId,
    aspect = instance,
    scope = private
}}) when
    Op =:= get;
    Op =:= update;
    Op =:= delete
->
    case dataset:get(DatasetId) of
        {ok, DatasetDoc} ->
            {ok, {DatasetDoc, 1}};
        {error, _} = Error ->
            Error
    end.


%%--------------------------------------------------------------------
%% @doc
%% {@link middleware_plugin} callback authorize/2.
%%
%% Checks only membership in space. Dataset management privileges
%% are checked later by fslogic layer.
%% @end
%%--------------------------------------------------------------------
-spec authorize(middleware:req(), middleware:entity()) -> boolean().
authorize(#op_req{operation = create, auth = Auth, gri = #gri{aspect = instance}, data = Data}, _) ->
    RootFileGuid = maps:get(<<"rootFileId">>, Data),
    SpaceId = file_id:guid_to_space_id(RootFileGuid),
    middleware_utils:is_eff_space_member(Auth, SpaceId);

authorize(#op_req{operation = Op, auth = Auth, gri = #gri{aspect = instance}}, DatasetDoc) when
    Op =:= get;
    Op =:= update;
    Op =:= delete
->
    {ok, SpaceId} = dataset:get_space_id(DatasetDoc),
    middleware_utils:is_eff_space_member(Auth, SpaceId).


%%--------------------------------------------------------------------
%% @doc
%% {@link middleware_plugin} callback validate/2.
%% @end
%%--------------------------------------------------------------------
-spec validate(middleware:req(), middleware:entity()) -> ok | no_return().
validate(#op_req{operation = create, gri = #gri{aspect = instance}, data = Data}, _) ->
    RootFileGuid = maps:get(<<"rootFileId">>, Data),
    SpaceId = file_id:guid_to_space_id(RootFileGuid),
    middleware_utils:assert_space_supported_locally(SpaceId);

validate(#op_req{operation = Op, gri = #gri{aspect = instance}}, DatasetDoc) when
    Op =:= get;
    Op =:= update;
    Op =:= delete
->
    {ok, SpaceId} = dataset:get_space_id(DatasetDoc),
    middleware_utils:assert_space_supported_locally(SpaceId).


%%--------------------------------------------------------------------
%% @doc
%% {@link middleware_plugin} callback create/1.
%% @end
%%--------------------------------------------------------------------
-spec create(middleware:req()) -> middleware:create_result().
create(#op_req{auth = Auth, data = Data, gri = #gri{aspect = instance} = GRI}) ->
    SessionId = Auth#auth.session_id,
    FileKey = {guid, maps:get(<<"rootFileId">>, Data)},
    ProtectionFlags = protection_flags_to_bitmask(maps:get(<<"protectionFlags">>, Data, [])),

    {ok, DatasetId} = ?check(lfm:establish_dataset(SessionId, FileKey, ProtectionFlags)),
    {ok, DatasetInfo} = ?check(lfm:get_dataset_info(SessionId, DatasetId)),
    {ok, resource, {GRI#gri{id = DatasetId}, DatasetInfo}}.


%%--------------------------------------------------------------------
%% @doc
%% {@link middleware_plugin} callback get/2.
%% @end
%%--------------------------------------------------------------------
-spec get(middleware:req(), middleware:entity()) -> middleware:get_result().
get(#op_req{auth = Auth, gri = #gri{id = DatasetId, aspect = instance}}, _) ->
    {ok, ?check(lfm:get_dataset_info(Auth#auth.session_id, DatasetId))}.


%%--------------------------------------------------------------------
%% @doc
%% {@link middleware_plugin} callback update/1.
%% @end
%%--------------------------------------------------------------------
-spec update(middleware:req()) -> middleware:update_result().
update(#op_req{auth = Auth, gri = #gri{id = DatasetId, aspect = instance}, data = Data}) ->
    ?check(lfm:update_dataset(
        Auth#auth.session_id, DatasetId,
        maps:get(<<"state">>, Data, undefined),
        protection_flags_to_bitmask(maps:get(<<"setProtectionFlags">>, Data, [])),
        protection_flags_to_bitmask(maps:get(<<"unsetProtectionFlags">>, Data, []))
    )).


%%--------------------------------------------------------------------
%% @doc
%% {@link middleware_plugin} callback delete/1.
%% @end
%%--------------------------------------------------------------------
-spec delete(middleware:req()) -> middleware:delete_result().
delete(#op_req{auth = Auth, gri = #gri{id = DatasetId, aspect = instance}}) ->
    ?check(lfm:remove_dataset(Auth#auth.session_id, DatasetId)).


%%%===================================================================
%%% Internal functions
%%%===================================================================


%% @private
-spec protection_flags_to_bitmask([binary()]) -> data_access_control:bitmask().
protection_flags_to_bitmask(ProtectionFlags) ->
    lists:foldl(fun(ProtectionFlag, Bitmask) ->
        ?set_flags(Bitmask, protection_flag_to_bitmask(ProtectionFlag))
    end, ?no_flags_mask, ProtectionFlags).


%% @private
-spec protection_flag_to_bitmask(binary()) -> data_access_control:bitmask().
protection_flag_to_bitmask(?DATA_PROTECTION_BIN) -> ?DATA_PROTECTION;
protection_flag_to_bitmask(?METADATA_PROTECTION_BIN) -> ?METADATA_PROTECTION.
