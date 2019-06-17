%%%-------------------------------------------------------------------
%%% @author Bartosz Walkowicz
%%% @copyright (C) 2019 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% This module handles op logic operations corresponding to op_replica model.
%%% @end
%%%-------------------------------------------------------------------
-module(op_replica).
-author("Bartosz Walkowicz").

-include("op_logic.hrl").
-include("http/rest/rest_api/rest_errors.hrl").
-include_lib("ctool/include/api_errors.hrl").

-export([op_logic_plugin/0]).
-export([operation_supported/3]).
-export([create/1, get/2, delete/1]).
-export([authorize/2, data_signature/1]).


%%%===================================================================
%%% API
%%%===================================================================


%%--------------------------------------------------------------------
%% @doc
%% Returns the op logic plugin module that handles model logic.
%% @end
%%--------------------------------------------------------------------
op_logic_plugin() ->
    op_replica.


%%--------------------------------------------------------------------
%% @doc
%% Determines if given operation is supported based on operation, aspect and
%% scope (entity type is known based on the plugin itself).
%% @end
%%--------------------------------------------------------------------
-spec operation_supported(op_logic:operation(), op_logic:aspect(),
    op_logic:scope()) -> boolean().
operation_supported(create, instance, private) -> true;
operation_supported(create, replicate_by_index, private) -> true;

operation_supported(get, distribution, private) -> true;

operation_supported(delete, instance, private) -> true;
operation_supported(delete, evict_by_index, private) -> true;

operation_supported(_, _, _) -> false.


%%--------------------------------------------------------------------
%% @doc
%% Creates a resource (aspect of entity) based on op logic request.
%% @end
%%--------------------------------------------------------------------
-spec create(op_logic:req()) -> op_logic:create_result().
create(#op_req{client = Cl, data = Data, gri = #gri{id = FileGuid, aspect = instance}}) ->
    SessionId = Cl#client.id,
    SpaceId = file_id:guid_to_space_id(FileGuid),
    Callback = maps:get(<<"url">>, Data, undefined),
    ReplicatingProvider = maps:get(<<"provider_id">>, Data, oneprovider:get_id()),

    ensure_space_support(SpaceId, [ReplicatingProvider]),
    ensure_file_exists(Cl, FileGuid),

    case logical_file_manager:schedule_file_replication(
        SessionId,
        {guid, FileGuid},
        ReplicatingProvider,
        Callback
    ) of
        {ok, TransferId} ->
            {ok, value, TransferId};
        Error ->
            Error
    end;

create(#op_req{client = Cl, data = Data, gri = #gri{id = IndexName, aspect = replicate_by_index}}) ->
    SessionId = Cl#client.id,
    SpaceId = maps:get(<<"space_id">>, Data),
    Callback = maps:get(<<"url">>, Data, undefined),
    QueryViewOptions = index_utils:sanitize_query_options(Data),
    ReplicatingProvider = maps:get(<<"provider_id">>, Data, oneprovider:get_id()),

    ensure_space_support(SpaceId, [ReplicatingProvider]),
    ensure_index_support(SpaceId, IndexName, [ReplicatingProvider]),

    case logical_file_manager:schedule_replication_by_index(
        SessionId,
        ReplicatingProvider,
        Callback,
        SpaceId,
        IndexName,
        QueryViewOptions
    ) of
        {ok, TransferId} ->
            {ok, value, TransferId};
        Error ->
            Error
    end.


%%--------------------------------------------------------------------
%% @doc
%% Retrieves a resource (aspect of entity) based on op logic request and
%% prefetched entity.
%% @end
%%--------------------------------------------------------------------
-spec get(op_logic:req(), op_logic:entity()) -> op_logic:get_result().
get(#op_req{client = Cl, gri = #gri{id = FileGuid, aspect = distribution}}, _) ->
    SpaceId = file_id:guid_to_space_id(FileGuid),
    ensure_space_supported_locally(SpaceId),
    logical_file_manager:get_file_distribution(Cl#client.id, {guid, FileGuid}).


%%--------------------------------------------------------------------
%% @doc
%% Deletes a resource (aspect of entity) based on op logic request.
%% @end
%%--------------------------------------------------------------------
-spec delete(op_logic:req()) -> op_logic:delete_result().
delete(#op_req{client = Cl, data = Data, gri = #gri{id = FileGuid, aspect = instance}}) ->
    SessionId = Cl#client.id,
    SpaceId = file_id:guid_to_space_id(FileGuid),
    EvictingProvider = maps:get(<<"provider_id">>, Data, oneprovider:get_id()),
    ReplicatingProvider = maps:get(<<"migration_provider_id">>, Data, undefined),

    ensure_space_support(SpaceId, [EvictingProvider, ReplicatingProvider]),
    ensure_file_exists(Cl, FileGuid),

    case logical_file_manager:schedule_replica_eviction(
        SessionId,
        {guid, FileGuid},
        EvictingProvider,
        ReplicatingProvider
    ) of
        {ok, TransferId} ->
            {ok, value, TransferId};
        Error ->
            Error
    end;

delete(#op_req{client = Cl, data = Data, gri = #gri{id = IndexName, aspect = evict_by_index}}) ->
    SessionId = Cl#client.id,
    SpaceId = maps:get(<<"space_id">>, Data),
    QueryViewOptions = index_utils:sanitize_query_options(Data),

    EvictingProvider = maps:get(<<"provider_id">>, Data, oneprovider:get_id()),
    ReplicatingProvider = maps:get(<<"migration_provider_id">>, Data, undefined),

    TargetProviders = [EvictingProvider, ReplicatingProvider],
    ensure_space_support(SpaceId, TargetProviders),
    ensure_index_support(SpaceId, IndexName, TargetProviders),

    case logical_file_manager:schedule_replica_eviction_by_index(
        SessionId,
        EvictingProvider,
        ReplicatingProvider,
        SpaceId,
        IndexName,
        QueryViewOptions
    ) of
        {ok, TransferId} ->
            {ok, value, TransferId};
        Error ->
            Error
    end.


%%--------------------------------------------------------------------
%% @doc
%% Determines if requesting client is authorized to perform given operation,
%% based on op logic request and prefetched entity.
%% @end
%%--------------------------------------------------------------------
-spec authorize(op_logic:req(), entity_logic:entity()) -> boolean().
authorize(#op_req{client = Cl, gri = #gri{id = Guid, aspect = instance}}, _) ->
    is_eff_space_member(Cl, file_id:guid_to_space_id(Guid));
authorize(#op_req{client = Cl, gri = #gri{aspect = replicate_by_index}} = Req, _) ->
    is_eff_space_member(Cl, maps:get(<<"space_id">>, Req#op_req.data));
authorize(#op_req{client = Cl, gri = #gri{aspect = evict_by_index}} = Req, _) ->
    is_eff_space_member(Cl, maps:get(<<"space_id">>, Req#op_req.data)).


%%--------------------------------------------------------------------
%% @doc
%% Returns data signature for given request.
%% Returns a map with 'required', 'optional' and 'at_least_one' keys.
%% Under each of them, there is a map:
%%      Key => {type_constraint, value_constraint}
%% Which means how value of given Key should be validated.
%% @end
%%--------------------------------------------------------------------
-spec data_signature(op_logic:req()) -> op_validator:data_signature().
data_signature(#op_req{operation = create, gri = #gri{aspect = instance}}) -> #{
    optional => #{
        <<"provider_id">> => {binary, any},
        <<"url">> => {binary, any}
    }
};

data_signature(#op_req{operation = create, gri = #gri{aspect = replicate_by_index}}) -> #{
    required => #{
        <<"space_id">> => {binary, any}
    },
    optional => #{
        <<"provider_id">> => {binary, any},
        <<"url">> => {binary, any},
        <<"descending">> => {boolean, any},
        <<"limit">> => {integer, {not_lower_than, 1}},
        <<"skip">> => {integer, {not_lower_than, 1}},
        <<"stale">> => {binary, [<<"ok">>, <<"update_after">>, <<"false">>]},
        <<"spatial">> => {boolean, any},
        <<"inclusive_end">> => {boolean, any},
        <<"start_range">> => {binary, any},
        <<"end_range">> => {binary, any},
        <<"startkey">> => {binary, any},
        <<"endkey">> => {binary, any},
        <<"key">> => {binary, any},
        <<"keys">> => {binary, any},
        <<"bbox">> => {binary, any}
    }
};

data_signature(#op_req{operation = delete, gri = #gri{aspect = instance}}) -> #{
    optional => #{
        <<"provider_id">> => {binary, any},
        <<"migration_provider_id">> => {binary, any}
    }
};

data_signature(#op_req{operation = delete, gri = #gri{aspect = evict_by_index}}) -> #{
    required => #{
        <<"space_id">> => {binary, any}
    },
    optional => #{
        <<"provider_id">> => {binary, any},
        <<"migration_provider_id">> => {binary, any},
        <<"descending">> => {boolean, any},
        <<"limit">> => {integer, {not_lower_than, 1}},
        <<"skip">> => {integer, {not_lower_than, 1}},
        <<"stale">> => {binary, [<<"ok">>, <<"update_after">>, <<"false">>]},
        <<"spatial">> => {boolean, any},
        <<"inclusive_end">> => {boolean, any},
        <<"start_range">> => {binary, any},
        <<"end_range">> => {binary, any},
        <<"startkey">> => {binary, any},
        <<"endkey">> => {binary, any},
        <<"key">> => {binary, any},
        <<"keys">> => {binary, any},
        <<"bbox">> => {binary, any}
    }
};

data_signature(_) -> #{}.


%%%===================================================================
%%% Internal functions
%%%===================================================================


%% @private
-spec is_eff_space_member(op_logic:client(), op_space:id()) -> boolean().
is_eff_space_member(#client{id = SessionId}, SpaceId) ->
    {ok, UserId} = session:get_user_id(SessionId),
    user_logic:has_eff_space(SessionId, UserId, SpaceId).


%% @private
-spec ensure_space_support(od_space:id(), [undefined | od_provider:id()]) ->
    ok | no_return().
ensure_space_support(SpaceId, TargetProviders) ->
    ensure_space_supported_locally(SpaceId),
    lists:foreach(fun(ProviderId) ->
        ensure_space_supported_by_provider(SpaceId, ProviderId)
    end, TargetProviders).


%% @private
ensure_space_supported_locally(SpaceId) ->
    case provider_logic:supports_space(SpaceId) of
        true -> ok;
        false -> throw(?ERROR_SPACE_NOT_SUPPORTED)
    end.


%% @private
-spec ensure_space_supported_by_provider(od_space:id(), od_provider:id()) ->
    ok | no_return().
ensure_space_supported_by_provider(_SpaceId, undefined) ->
    ok;
ensure_space_supported_by_provider(SpaceId, ProviderId) ->
    case space_logic:is_supported(?ROOT_SESS_ID, SpaceId, ProviderId) of
        true ->
            ok;
        false ->
            throw(?ERROR_SPACE_NOT_SUPPORTED_BY(ProviderId))
    end.


%% @private
-spec ensure_file_exists(op_logic:client(), file_id:file_guid()) ->
    ok | no_return().
ensure_file_exists(#client{id = SessionId}, FileGuid) ->
    case logical_file_manager:stat(SessionId, {guid, FileGuid}) of
        {ok, _} ->
            ok;
        _ ->
            throw(?ERROR_NOT_FOUND)
    end.


%% @private
-spec ensure_index_support(od_space:id(), binary(),
    [undefined | od_provider:id()]) -> ok | no_return().
ensure_index_support(SpaceId, IndexName, TargetProviders) ->
    lists:foreach(fun(ProviderId) ->
        ensure_index_supported_by_provider(SpaceId, IndexName, ProviderId)
    end, TargetProviders).


%% @private
-spec ensure_index_supported_by_provider(od_space:id(), binary(),
    od_provider:id()) -> ok | no_return().
ensure_index_supported_by_provider(_SpaceId, _IndexName, undefined) ->
    ok;
ensure_index_supported_by_provider(SpaceId, IndexName, ProviderId) ->
    case index:is_supported(SpaceId, IndexName, ProviderId) of
        true ->
            ok;
        false ->
            throw(?ERROR_INDEX_NOT_SUPPORTED_BY(ProviderId))
    end.
