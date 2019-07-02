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
-export([
    operation_supported/3,
    data_spec/1,
    fetch_entity/1,
    exists/2,
    authorize/2,
    validate/2
]).
-export([create/1, get/2, update/1, delete/1]).


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
%% Returns data signature for given request.
%% Returns a map with 'required', 'optional' and 'at_least_one' keys.
%% Under each of them, there is a map:
%%      Key => {type_constraint, value_constraint}
%% Which means how value of given Key should be validated.
%% @end
%%--------------------------------------------------------------------
-spec data_spec(op_logic:req()) -> undefined | op_sanitizer:data_spec().
data_spec(#op_req{operation = create, gri = #gri{aspect = instance}}) -> #{
    optional => #{
        <<"provider_id">> => {binary, non_empty},
        <<"url">> => {binary, non_empty}
    }
};

data_spec(#op_req{operation = create, gri = #gri{aspect = replicate_by_index}}) -> #{
    required => #{
        <<"space_id">> => {binary, non_empty}
    },
    optional => #{
        <<"provider_id">> => {binary, non_empty},
        <<"url">> => {binary, non_empty},
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

data_spec(#op_req{operation = get, gri = #gri{aspect = distribution}}) ->
    undefined;

data_spec(#op_req{operation = delete, gri = #gri{aspect = instance}}) -> #{
    optional => #{
        <<"provider_id">> => {binary, non_empty},
        <<"migration_provider_id">> => {binary, non_empty}
    }
};

data_spec(#op_req{operation = delete, gri = #gri{aspect = evict_by_index}}) -> #{
    required => #{
        <<"space_id">> => {binary, non_empty}
    },
    optional => #{
        <<"provider_id">> => {binary, non_empty},
        <<"migration_provider_id">> => {binary, non_empty},
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
}.


%%--------------------------------------------------------------------
%% @doc
%% Retrieves an entity from datastore based on its EntityId.
%% Should return ?ERROR_NOT_FOUND if the entity does not exist.
%% @end
%%--------------------------------------------------------------------
-spec fetch_entity(op_logic:req()) ->
    {ok, op_logic:entity()} | entity_logic:error().
fetch_entity(_) ->
    {ok, undefined}.


%%--------------------------------------------------------------------
%% @doc
%% Determines if given resource (aspect of entity) exists, based on
%% op logic request and prefetched entity.
%% @end
%%--------------------------------------------------------------------
-spec exists(op_logic:req(), entity_logic:entity()) -> boolean().
exists(_, _) ->
    true.


%%--------------------------------------------------------------------
%% @doc
%% Determines if requesting client is authorized to perform given operation,
%% based on op logic request and prefetched entity.
%% @end
%%--------------------------------------------------------------------
-spec authorize(op_logic:req(), entity_logic:entity()) -> boolean().
authorize(#op_req{client = ?NOBODY}, _) ->
    false;

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
    op_logic_utils:is_eff_space_member(Req#op_req.client, SpaceId).


%%--------------------------------------------------------------------
%% @doc
%% Determines if given request can be further processed
%% (e.g. checks whether space is supported locally).
%% Should throw custom error if not (e.g. ?ERROR_SPACE_NOT_SUPPORTED).
%% @end
%%--------------------------------------------------------------------
-spec validate(op_logic:req(), entity_logic:entity()) -> ok | no_return().
validate(#op_req{operation = create, gri = #gri{id = Guid, aspect = instance}} = Req, _) ->
    SpaceId = file_id:guid_to_space_id(Guid),
    op_logic_utils:ensure_space_supported_locally(SpaceId),

    op_logic_utils:ensure_file_exists(Req#op_req.client, Guid),

    % In case of undefined `provider_id` local provider is chosen instead
    case maps:get(<<"provider_id">>, Req#op_req.data, undefined) of
        undefined ->
            ok;
        ReplicatingProvider ->
            op_logic_utils:ensure_space_supported_by(SpaceId, ReplicatingProvider)
    end;

validate(#op_req{operation = create, gri = #gri{id = Name, aspect = replicate_by_index}} = Req, _) ->
    Data = Req#op_req.data,
    SpaceId = maps:get(<<"space_id">>, Data),
    op_logic_utils:ensure_space_supported_locally(SpaceId),

    % In case of undefined `provider_id` local provider is chosen instead
    case maps:get(<<"provider_id">>, Data, undefined) of
        undefined ->
            ensure_index_exists_on_provider(SpaceId, Name, oneprovider:get_id());
        ReplicatingProvider ->
            op_logic_utils:ensure_space_supported_by(SpaceId, ReplicatingProvider),
            ensure_index_exists_on_provider(SpaceId, Name, ReplicatingProvider)
    end;

validate(#op_req{operation = get, gri = #gri{id = FileGuid, aspect = distribution}}, _) ->
    SpaceId = file_id:guid_to_space_id(FileGuid),
    op_logic_utils:ensure_space_supported_locally(SpaceId);

validate(#op_req{operation = delete, gri = #gri{id = Guid, aspect = instance}} = Req, _) ->
    Data = Req#op_req.data,
    SpaceId = file_id:guid_to_space_id(Guid),
    op_logic_utils:ensure_space_supported_locally(SpaceId),

    op_logic_utils:ensure_file_exists(Req#op_req.client, Guid),

    % In case of undefined `provider_id` local provider is chosen instead
    case maps:get(<<"provider_id">>, Data, undefined) of
        undefined ->
            ok;
        EvictingProvider ->
            op_logic_utils:ensure_space_supported_by(SpaceId, EvictingProvider)
    end,

    case maps:get(<<"migration_provider_id">>, Data, undefined) of
        undefined ->
            ok;
        ReplicatingProvider ->
            op_logic_utils:ensure_space_supported_by(SpaceId, ReplicatingProvider)
    end;

validate(#op_req{operation = delete, gri = #gri{id = Name, aspect = evict_by_index}} = Req, _) ->
    Data = Req#op_req.data,
    SpaceId = maps:get(<<"space_id">>, Data),
    op_logic_utils:ensure_space_supported_locally(SpaceId),

    % In case of undefined `provider_id` local provider is chosen instead
    case maps:get(<<"provider_id">>, Data, undefined) of
        undefined ->
            ensure_index_exists_on_provider(SpaceId, Name, oneprovider:get_id());
        EvictingProvider ->
            op_logic_utils:ensure_space_supported_by(SpaceId, EvictingProvider),
            ensure_index_exists_on_provider(SpaceId, Name, EvictingProvider)
    end,

    case maps:get(<<"migration_provider_id">>, Data, undefined) of
        undefined ->
            ok;
        ReplicatingProvider ->
            op_logic_utils:ensure_space_supported_by(SpaceId, ReplicatingProvider),
            ensure_index_exists_on_provider(SpaceId, Name, ReplicatingProvider)
    end.


%%--------------------------------------------------------------------
%% @doc
%% Creates a resource (aspect of entity) based on op logic request.
%% @end
%%--------------------------------------------------------------------
-spec create(op_logic:req()) -> op_logic:create_result().
create(#op_req{client = Cl, data = Data, gri = #gri{id = FileGuid, aspect = instance}}) ->
    case logical_file_manager:schedule_file_replication(
        Cl#client.session_id,
        {guid, FileGuid},
        maps:get(<<"provider_id">>, Data, oneprovider:get_id()),
        maps:get(<<"url">>, Data, undefined)
    ) of
        {ok, TransferId} ->
            {ok, value, TransferId};
        {error, Errno} ->
            ?ERROR_POSIX(Errno)
    end;

create(#op_req{client = Cl, data = Data, gri = #gri{id = IndexName, aspect = replicate_by_index}}) ->
    case logical_file_manager:schedule_replication_by_index(
        Cl#client.session_id,
        maps:get(<<"provider_id">>, Data, oneprovider:get_id()),
        maps:get(<<"url">>, Data, undefined),
        maps:get(<<"space_id">>, Data),
        IndexName,
        index_utils:sanitize_query_options(Data)
    ) of
        {ok, TransferId} ->
            {ok, value, TransferId};
        {error, Errno} ->
            ?ERROR_POSIX(Errno)
    end.


%%--------------------------------------------------------------------
%% @doc
%% Retrieves a resource (aspect of entity) based on op logic request and
%% prefetched entity.
%% @end
%%--------------------------------------------------------------------
-spec get(op_logic:req(), op_logic:entity()) -> op_logic:get_result().
get(#op_req{client = Cl, gri = #gri{id = FileGuid, aspect = distribution}}, _) ->
    SessionId = Cl#client.session_id,
    case logical_file_manager:get_file_distribution(SessionId, {guid, FileGuid}) of
        {ok, _Blocks} = Res ->
            Res;
        {error, Errno} ->
            ?ERROR_POSIX(Errno)
    end.


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
    case logical_file_manager:schedule_replica_eviction(
        Cl#client.session_id,
        {guid, FileGuid},
        maps:get(<<"provider_id">>, Data, oneprovider:get_id()),
        maps:get(<<"migration_provider_id">>, Data, undefined)
    ) of
        {ok, TransferId} ->
            {ok, value, TransferId};
        {error, Errno} ->
            ?ERROR_POSIX(Errno)
    end;

delete(#op_req{client = Cl, data = Data, gri = #gri{id = IndexName, aspect = evict_by_index}}) ->
    case logical_file_manager:schedule_replica_eviction_by_index(
        Cl#client.session_id,
        maps:get(<<"provider_id">>, Data, oneprovider:get_id()),
        maps:get(<<"migration_provider_id">>, Data, undefined),
        maps:get(<<"space_id">>, Data),
        IndexName,
        index_utils:sanitize_query_options(Data)
    ) of
        {ok, TransferId} ->
            {ok, value, TransferId};
        {error, Errno} ->
            ?ERROR_POSIX(Errno)
    end.


%%%===================================================================
%%% Internal functions
%%%===================================================================


%% @private
-spec ensure_index_exists_on_provider(od_space:id(), index:name(),
    od_provider:id()) -> ok | no_return().
ensure_index_exists_on_provider(SpaceId, IndexName, ProviderId) ->
    case index:is_supported(SpaceId, IndexName, ProviderId) of
        true ->
            ok;
        false ->
            throw(?ERROR_INDEX_NOT_SUPPORTED_BY(ProviderId))
    end.
