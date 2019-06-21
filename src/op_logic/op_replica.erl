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

-behaviour(op_logic_behaviour).

-include("op_logic.hrl").
-include("http/rest/rest_api/rest_errors.hrl").
-include_lib("ctool/include/api_errors.hrl").

-export([op_logic_plugin/0]).
-export([operation_supported/3]).
-export([create/1, get/2, update/1, delete/1]).
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

    op_logic_utils:ensure_space_support(SpaceId, [ReplicatingProvider]),
    op_logic_utils:ensure_file_exists(Cl, FileGuid),

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

    op_logic_utils:ensure_space_support(SpaceId, [ReplicatingProvider]),
    op_logic_utils:ensure_index_support(SpaceId, IndexName, [ReplicatingProvider]),

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
    end;

create(_) ->
    ?ERROR_NOT_SUPPORTED.


%%--------------------------------------------------------------------
%% @doc
%% Retrieves a resource (aspect of entity) based on op logic request and
%% prefetched entity.
%% @end
%%--------------------------------------------------------------------
-spec get(op_logic:req(), op_logic:entity()) -> op_logic:get_result().
get(#op_req{client = Cl, gri = #gri{id = FileGuid, aspect = distribution}}, _) ->
    SpaceId = file_id:guid_to_space_id(FileGuid),
    op_logic_utils:ensure_space_supported_locally(SpaceId),
    logical_file_manager:get_file_distribution(Cl#client.id, {guid, FileGuid});

get(_, _) ->
    ?ERROR_NOT_SUPPORTED.


%%--------------------------------------------------------------------
%% @doc
%% Updates a resource (aspect of entity) based on op logic request.
%% @end
%%--------------------------------------------------------------------
-spec update(op_logic:req()) -> op_logic:update_result().
update(_) ->
    ?ERROR_NOT_SUPPORTED.


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

    op_logic_utils:ensure_space_support(SpaceId, [EvictingProvider, ReplicatingProvider]),
    op_logic_utils:ensure_file_exists(Cl, FileGuid),

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
    op_logic_utils:ensure_space_support(SpaceId, TargetProviders),
    op_logic_utils:ensure_index_support(SpaceId, IndexName, TargetProviders),

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
    end;

delete(_) ->
    ?ERROR_NOT_SUPPORTED.


%%--------------------------------------------------------------------
%% @doc
%% Determines if requesting client is authorized to perform given operation,
%% based on op logic request and prefetched entity.
%% @end
%%--------------------------------------------------------------------
-spec authorize(op_logic:req(), entity_logic:entity()) -> boolean().
authorize(#op_req{operation = create, gri = #gri{id = Guid, aspect = instance}} = Req, _) ->
    SpaceId = file_id:guid_to_space_id(Guid),
    op_logic_utils:is_eff_space_member(Req#op_req.client, SpaceId);

authorize(#op_req{operation = create, gri = #gri{aspect = replicate_by_index}} = Req, _) ->
    SpaceId = maps:get(<<"space_id">>, Req#op_req.data),
    op_logic_utils:is_eff_space_member(Req#op_req.client, SpaceId);

authorize(#op_req{operation = get, gri = #gri{id = Guid, aspect = distribution}} = Req, _) ->
    SpaceId = file_id:guid_to_space_id(Guid),
    op_logic_utils:is_eff_space_member(Req#op_req.client, SpaceId);

authorize(#op_req{operation = delete, gri = #gri{id = Guid, aspect = instance}} = Req, _) ->
    SpaceId = file_id:guid_to_space_id(Guid),
    op_logic_utils:is_eff_space_member(Req#op_req.client, SpaceId);

authorize(#op_req{operation = delete, gri = #gri{aspect = evict_by_index}} = Req, _) ->
    SpaceId = maps:get(<<"space_id">>, Req#op_req.data),
    op_logic_utils:is_eff_space_member(Req#op_req.client, SpaceId);

authorize(_, _) ->
    false.


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
