%%%-------------------------------------------------------------------
%%% @author Michal Stanisz
%%% @copyright (C) 2019 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% Interface for reading and manipulating od_harvester records synchronized
%%% via Graph Sync. Requests are delegated to gs_client_worker, which decides
%%% if they should be served from cache or handled by Onezone.
%%% NOTE: This is the only valid way to interact with od_storage records, to
%%% ensure consistency, no direct requests to datastore or OZ REST should
%%% be performed.
%%% @end
%%%-------------------------------------------------------------------
-module(storage_logic).
-author("Michal Stanisz").

-include("graph_sync/provider_graph_sync.hrl").
-include("modules/datastore/datastore_models.hrl").
-include("modules/fslogic/fslogic_common.hrl").
-include_lib("ctool/include/logging.hrl").
-include_lib("ctool/include/aai/aai.hrl").
-include_lib("ctool/include/errors.hrl").

-export([create/1, get/1, delete/1]).
-export([support_space/3, support_space/4]).
-export([update_space_support_size/3]).
-export([revoke_support/2]).
-export([set_qos_parameters/2, get_qos_parameters/1]).
-export([describe/1]).

%%%===================================================================
%%% API
%%%===================================================================

-spec create(storage_config:doc()) -> {ok, od_storage:id()} | errors:error().
create(StorageConfig) ->
    case storage_config:save_doc(StorageConfig) of
        {ok, StorageId} ->
            StorageName = storage_config:get_name(StorageConfig),
            case create_in_zone(StorageId, StorageName) of
                {ok, _} ->
                    ok = on_storage_created(StorageId),
                    {ok, StorageId};
                Error ->
                    delete_in_zone(StorageId),
                    Error
            end;
        Other ->
            Other
    end.


%% @private
-spec create_in_zone(od_storage:id(), storage_config:name()) ->
    {ok, od_storage:doc()} | errors:error().
create_in_zone(StorageId, StorageName) ->
    gs_client_worker:request(?ROOT_SESS_ID, #gs_req_graph{
        operation = create,
        gri = #gri{type = od_storage, id = StorageId, aspect = instance},
        data = #{<<"name">> => StorageName}
    }).


-spec get(od_storage:id()) -> {ok, od_storage:doc()} | errors:error().
get(StorageId) ->
    gs_client_worker:request(?ROOT_SESS_ID, #gs_req_graph{
        operation = get,
        gri = #gri{type = od_storage, id = StorageId, aspect = instance},
        subscribe = true
    }).


delete(StorageId) ->
    case delete_in_zone(StorageId) of
        ok -> storage_config:delete(StorageId);
        Error -> Error
    end.


%% @private
-spec delete_in_zone(od_storage:id()) -> ok | errors:error().
delete_in_zone(StorageId) ->
    gs_client_worker:request(?ROOT_SESS_ID, #gs_req_graph{
        operation = delete,
        gri = #gri{type = od_storage, id = StorageId, aspect = instance}
    }).


-spec support_space(od_storage:id(), tokens:serialized(), SupportSize :: integer()) ->
    {ok, od_space:id()} | errors:error().
support_space(StorageId, Token, SupportSize) ->
    support_space(?ROOT_SESS_ID, StorageId, Token, SupportSize).

-spec support_space(SessionId :: gs_client_worker:client(),
    od_storage:id(), tokens:serialized(), SupportSize :: integer()) ->
    {ok, od_space:id()} | errors:error().
support_space(SessionId, StorageId, SerializedToken, SupportSize) ->
%% @TODO VFS-5497 Checking whether provider can support space not needed when multisupport is implemented (will be checked in zone)
    case can_support_space(SerializedToken) of
        {ok, SpaceId} ->
            Data = #{<<"token">> => SerializedToken, <<"size">> => SupportSize},
            Result = gs_client_worker:request(SessionId, #gs_req_graph{
                operation = create,
                gri = #gri{type = od_storage, id = StorageId, aspect = support},
                data = Data
            }),

            ?CREATE_RETURN_ID(?ON_SUCCESS(Result, fun(_) ->
                on_space_supported(SpaceId, StorageId),
                gs_client_worker:invalidate_cache(od_provider, oneprovider:get_id()),
                gs_client_worker:invalidate_cache(od_space, SpaceId),
                gs_client_worker:invalidate_cache(od_storage, StorageId)
            end));
        Error ->
            Error
    end.


%% @private
-spec can_support_space(tokens:serialized()) -> {ok, od_space:id()} | errors:error().
can_support_space(SerializedToken) ->
    case tokens:deserialize(SerializedToken) of
        {ok, #token{type = TokenType}} ->
            case TokenType of
                ?INVITE_TOKEN(?SPACE_SUPPORT_TOKEN, SpaceId) ->
                    case provider_logic:supports_space(SpaceId) of
                        true ->
                            ?ERROR_RELATION_ALREADY_EXISTS(
                                od_space, SpaceId, od_provider, oneprovider:get_id()
                            );
                        false ->
                            {ok, SpaceId}
                    end;
                _ ->
                    ?ERROR_NOT_AN_INVITE_TOKEN(?SPACE_SUPPORT_TOKEN)
                end;
        {error, _} = Error ->
            Error
    end.


-spec update_space_support_size(od_storage:id(), od_space:id(), NewSupportSize :: integer()) ->
    ok | errors:error().
update_space_support_size(StorageId, SpaceId, NewSupportSize) ->
    OccupiedSize = space_quota:current_size(SpaceId),
    update_space_support_size(StorageId, SpaceId, NewSupportSize, OccupiedSize).


%% @private
-spec update_space_support_size(od_storage:id(), od_space:id(), NewSupportSize :: integer(),
    CurrentOccupiedSize :: non_neg_integer()) ->
    ok | errors:error().
update_space_support_size(_StorageId, _SpaceId, NewSupportSize, CurrentOccupiedSize)
    when NewSupportSize < CurrentOccupiedSize ->
    ?ERROR_BAD_VALUE_TOO_LOW(<<"size">>, CurrentOccupiedSize);

update_space_support_size(StorageId, SpaceId, NewSupportSize, _CurrentOccupiedSize) ->
    Data = #{<<"size">> => NewSupportSize},
    Result = gs_client_worker:request(?ROOT_SESS_ID, #gs_req_graph{
        operation = update, data = Data,
        gri = #gri{type = od_storage, id = StorageId, aspect = {space, SpaceId}}
    }),
    ?ON_SUCCESS(Result, fun(_) ->
        gs_client_worker:invalidate_cache(od_space, SpaceId),
        gs_client_worker:invalidate_cache(od_storage, StorageId),
        gs_client_worker:invalidate_cache(od_provider, oneprovider:get_id())
    end).


-spec revoke_support(od_storage:id(), od_space:id()) -> ok | errors:error().
revoke_support(StorageId, SpaceId) ->
    Result = gs_client_worker:request(?ROOT_SESS_ID, #gs_req_graph{
        operation = delete,
        gri = #gri{type = od_storage, id = StorageId, aspect = {space, SpaceId}}
    }),
    ?ON_SUCCESS(Result, fun(_) ->
        on_space_unsupported(SpaceId, StorageId),
        gs_client_worker:invalidate_cache(od_space, SpaceId),
        gs_client_worker:invalidate_cache(od_storage, StorageId),
        gs_client_worker:invalidate_cache(od_provider, oneprovider:get_id())
    end).


-spec set_qos_parameters(od_storage:id(), od_storage:qos_parameters()) -> ok | errors:error().
set_qos_parameters(StorageId, QosParameters) ->
    Result = gs_client_worker:request(?ROOT_SESS_ID, #gs_req_graph{
        operation = update,
        gri = #gri{type = od_storage, id = StorageId, aspect = instance},
        data = #{<<"qos_parameters">> => QosParameters}
    }),
    ?ON_SUCCESS(Result, fun(_) ->
        gs_client_worker:invalidate_cache(od_storage, StorageId)
    end).


-spec get_qos_parameters(od_storage:id()) -> {ok, od_storage:qos_parameters()} | errors:error().
get_qos_parameters(StorageId) ->
    case ?MODULE:get(StorageId) of
        {ok, #document{value = #od_storage{qos_parameters = QosParameters}}} -> {ok, QosParameters};
        Error -> Error
    end.

-spec describe(od_storage:id()) ->
    {ok, #{binary() := binary() | boolean() | undefined}} | {error, term()}.
describe(StorageId) ->
    case storage_config:describe(StorageId) of
        {ok, Map} ->
            {ok, QosParameters} = get_qos_parameters(StorageId),
            {ok, Map#{
                <<"qosParameters">> => QosParameters
            }};
        {error, _} = Error -> Error
    end.


%% @private
-spec on_storage_created(od_storage:id()) -> ok.
on_storage_created(StorageId) ->
    ok = storage_config:on_storage_created(StorageId).


%% @private
-spec on_space_supported(od_space:id(), od_storage:id()) -> ok.
on_space_supported(SpaceId, StorageId) ->
    ok = space_strategies:add_storage(SpaceId, StorageId).


%% @private
-spec on_space_unsupported(od_space:id(), od_storage:id()) -> ok.
on_space_unsupported(SpaceId, StorageId) ->
    autocleaning_api:disable(SpaceId),
    autocleaning_api:delete_config(SpaceId),
    file_popularity_api:disable(SpaceId),
    file_popularity_api:delete_config(SpaceId),
    space_strategies:delete(SpaceId, StorageId),
    main_harvesting_stream:space_unsupported(SpaceId).
