%%%-------------------------------------------------------------------
%%% @author Bartosz Walkowicz
%%% @copyright (C) 2019 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% This module handles op logic operations corresponding to od_space model.
%%% @end
%%%-------------------------------------------------------------------
-module(op_space).
-author("Bartosz Walkowicz").

-include("op_logic.hrl").
-include("modules/datastore/transfer.hrl").
-include("modules/fslogic/fslogic_common.hrl").
-include("http/rest/rest_api/rest_errors.hrl").
-include_lib("ctool/include/logging.hrl").
%%-include_lib("ctool/include/api_errors.hrl").
-include_lib("ctool/include/posix/errors.hrl").

-export([fetch_entity/1, operation_supported/3]).
-export([create/1, get/2, update/1, delete/1]).
-export([exists/2, authorize/2, validate/1]).
-export([entity_logic_plugin/0]).

-define(MAX_LIST_LIMIT, 1000).
-define(DEFAULT_INDEX_LIST_LIMIT, 100).
-define(DEFAULT_TRANSFER_LIST_LIMIT, 100).

-define(index_check_exec(__Result),
    case __Result of
        ok ->
            ok;
        {ok, _} = __Ans ->
            __Ans;
        {error, ?EINVAL} ->
            ?ERROR_BAD_VALUE_AMBIGUOUS_ID(<<"index_name">>);
        __Error ->
            __Error
    end).

%%%===================================================================
%%% API
%%%===================================================================


%%--------------------------------------------------------------------
%% @doc
%% Returns the entity logic plugin module that handles model logic.
%% @end
%%--------------------------------------------------------------------
entity_logic_plugin() ->
    op_space.


%%--------------------------------------------------------------------
%% @doc
%% Retrieves an entity from datastore based on its EntityId.
%% Should return ?ERROR_NOT_FOUND if the entity does not exist.
%% @end
%%--------------------------------------------------------------------
-spec fetch_entity(entity_logic:entity_id()) ->
    {ok, entity_logic:entity()} | op_logic:error().
fetch_entity(_) ->
    {ok, none}.


%%--------------------------------------------------------------------
%% @doc
%% Determines if given operation is supported based on operation, aspect and
%% scope (entity type is known based on the plugin itself).
%% @end
%%--------------------------------------------------------------------
-spec operation_supported(op_logic:operation(), op_logic:aspect(),
    op_logic:scope()) -> boolean().
operation_supported(create, {index, _}, private) -> true;
operation_supported(create, {index_reduce_function, _}, private) -> true;

operation_supported(get, list, private) -> true;
operation_supported(get, instance, private) -> true;
operation_supported(get, indices, private) -> true;
operation_supported(get, {index, _}, private) -> true;
operation_supported(get, {query_index, _}, private) -> true;
operation_supported(get, transfers, private) -> true;

operation_supported(update, {index, _}, private) -> true;

operation_supported(delete, {index, _}, private) -> true;
operation_supported(delete, {index_reduce_function, _}, private) -> true;

operation_supported(_, _, _) -> false.


%%--------------------------------------------------------------------
%% @doc
%% Creates a resource (aspect of entity) based on entity logic request.
%% @end
%%--------------------------------------------------------------------
-spec create(op_logic:req()) -> op_logic:create_result().
create(#el_req{data = Data, gri = #gri{id = SpaceId, aspect = {index, IndexName}}}) ->
    Spatial = maps:get(<<"spatial">>, Data, false),

    MapFunction = maps:get(<<"application/javascript">>, Data),
    IndexOptions = prepare_index_options(Data),

    Providers = case maps:get(<<"providers[]">>, Data, undefined) of
        undefined -> [oneprovider:get_id()];
        ProviderId when is_binary(ProviderId) -> [ProviderId];
        ProvidersList -> ProvidersList
    end,
    ensure_space_supported_by_providers(SpaceId, Providers),

    index:save(
        SpaceId, IndexName, MapFunction, undefined,
        IndexOptions, Spatial, Providers
    );
create(#el_req{gri = #gri{id = SpaceId, aspect = {index_reduce_function, IndexName}}} = Req) ->
    ReduceFunction = maps:get(<<"application/javascript">>, Req#el_req.data),
    ?index_check_exec(index:update_reduce_function(SpaceId, IndexName, ReduceFunction)).


%%--------------------------------------------------------------------
%% @doc
%% Retrieves a resource (aspect of entity) based on entity logic request and
%% prefetched entity.
%% @end
%%--------------------------------------------------------------------
-spec get(op_logic:req(), entity_logic:entity()) -> op_logic:get_result().
get(#el_req{client = Cl, gri = #gri{aspect = list}}, _) ->
    SessionId = Cl#client.id,
    {ok, UserId} = session:get_user_id(SessionId),
    {ok, EffSpacesIds} = user_logic:get_eff_spaces(SessionId, UserId),
    EffSpaces = lists:map(fun(SpaceId) ->
        {ok, SpaceName} = space_logic:get_name(SessionId, SpaceId),
        #{<<"spaceId">> => SpaceId, <<"name">> => SpaceName}
    end, EffSpacesIds),
    {ok, EffSpaces};

get(#el_req{client = Cl, gri = #gri{id = SpaceId, aspect = instance}}, _) ->
    SessionId = Cl#client.id,
    case space_logic:get(SessionId, SpaceId) of
        {ok, #document{value = #od_space{name = Name, providers = ProvidersIds}}} ->
            Providers = lists:map(fun(ProviderId) ->
                {ok, ProviderName} = provider_logic:get_name(ProviderId),
                #{
                    <<"providerId">> => ProviderId,
                    <<"providerName">> => ProviderName
                }
            end, maps:keys(ProvidersIds)),
            {ok, #{
                <<"name">> => Name,
                <<"providers">> => Providers,
                <<"spaceId">> => SpaceId
            }};
        Error ->
            Error
    end;

get(#el_req{data = Data, gri = #gri{id = SpaceId, aspect = indices}}, _) ->
    PageToken = maps:get(<<"page_token">>, Data, <<"null">>),
    Limit = maps:get(<<"limit">>, Data, ?DEFAULT_INDEX_LIST_LIMIT),

    {StartId, Offset} = case PageToken of
        <<"null">> ->
            {undefined, 0};
        _ ->
            % Start after the page token (link key from last listing) if it is given
            {PageToken, 1}
    end,

    {ok, Indexes} = index:list(SpaceId, StartId, Offset, Limit),

    NextPageToken = case length(Indexes) of
        Limit ->
            #{<<"nextPageToken">> => lists:last(Indexes)};
        _ ->
            #{}
    end,

    {ok, maps:merge(#{<<"indexes">> => Indexes}, NextPageToken)};

get(#el_req{gri = #gri{id = SpaceId, aspect = {index, IndexName}}}, _) ->
    ?index_check_exec(index:get_json(SpaceId, IndexName));

get(#el_req{gri = #gri{id = SpaceId, aspect = {query_index, IndexName}}} = Req, _) ->
    Options = index_utils:sanitize_query_options(Req#el_req.data),
    ?error("~n~nOPTIONS: ~p~n~n", [Options]),
    case index:query(SpaceId, IndexName, Options) of
        {ok, {Rows}} ->
            QueryResult = lists:map(fun(Row) -> maps:from_list(Row) end, Rows),
            {ok, QueryResult};
        {error, ?EINVAL} ->
            ?ERROR_BAD_VALUE_AMBIGUOUS_ID(<<"index_name">>);
        Error ->
            Error
    end;

get(#el_req{gri = #gri{id = SpaceId, aspect = transfers}} = Req, _) ->
    PageToken = maps:get(<<"page_token">>, Req#el_req.data, <<"null">>),
    TransferState = maps:get(<<"status">>, Req#el_req.data, <<"ongoing">>),
    Limit = maps:get(<<"limit">>, Req#el_req.data, ?DEFAULT_TRANSFER_LIST_LIMIT),

    {StartId, Offset} = case PageToken of
        <<"null">> ->
            {undefined, 0};
        _ ->
            % Start after the page token (link key from last listing) if it is given
            {PageToken, 1}
    end,

    {ok, Transfers} = case TransferState of
        ?WAITING_TRANSFERS_STATE ->
            transfer:list_waiting_transfers(SpaceId, StartId, Offset, Limit);
        ?ONGOING_TRANSFERS_STATE ->
            transfer:list_ongoing_transfers(SpaceId, StartId, Offset, Limit);
        ?ENDED_TRANSFERS_STATE ->
            transfer:list_ended_transfers(SpaceId, StartId, Offset, Limit)
    end,

    NextPageToken = case length(Transfers) of
        Limit ->
            {ok, LinkKey} = transfer:get_link_key_by_state(
                lists:last(Transfers), TransferState
            ),
            #{<<"nextPageToken">> => LinkKey};
        _ ->
            #{}
    end,

    {ok, maps:merge(#{<<"transfers">> => Transfers}, NextPageToken)}.


%%--------------------------------------------------------------------
%% @doc
%% Updates a resource (aspect of entity) based on entity logic request.
%% @end
%%--------------------------------------------------------------------
-spec update(op_logic:req()) -> op_logic:update_result().
update(#el_req{data = Data, gri = #gri{id = SpaceId, aspect = {index, IndexName}}}) ->
    Spatial = maps:get(<<"spatial">>, Data, undefined),

    MapFunctionRaw = maps:get(<<"application/javascript">>, Data),
    MapFun = utils:ensure_defined(MapFunctionRaw, <<>>, undefined),
    IndexOptions = prepare_index_options(Data),

    Providers = case maps:get(<<"providers[]">>, Data, undefined) of
        undefined -> undefined;
        ProviderId when is_binary(ProviderId) -> [ProviderId];
        ProvidersList -> ProvidersList
    end,
    ensure_space_supported_by_providers(SpaceId, Providers),

    ?index_check_exec(index:update(
        SpaceId, IndexName, MapFun,
        IndexOptions, Spatial, Providers
    )).


%%--------------------------------------------------------------------
%% @doc
%% Deletes a resource (aspect of entity) based on entity logic request.
%% @end
%%--------------------------------------------------------------------
-spec delete(op_logic:req()) -> op_logic:delete_result().
delete(#el_req{gri = #gri{id = SpaceId, aspect = {index, IndexName}}}) ->
    ?index_check_exec(index:delete(SpaceId, IndexName));
delete(#el_req{gri = #gri{id = SpaceId, aspect = {index_reduce_function, IndexName}}}) ->
    ?index_check_exec(index:update_reduce_function(SpaceId, IndexName, undefined)).


%%--------------------------------------------------------------------
%% @doc
%% Determines if given resource (aspect of entity) exists, based on entity
%% logic request and prefetched entity.
%% @end
%%--------------------------------------------------------------------
-spec exists(op_logic:req(), entity_logic:entity()) -> boolean().
exists(_, _) ->
    true.


%%--------------------------------------------------------------------
%% @doc
%% Determines if requesting client is authorized to perform given operation,
%% based on entity logic request and prefetched entity.
%% @end
%%--------------------------------------------------------------------
-spec authorize(op_logic:req(), entity_logic:entity()) -> boolean().
authorize(#el_req{gri = #gri{id = undefined}}, _) ->
    true;
authorize(#el_req{client = Cl, gri = #gri{id = SpaceId}}, _) ->
    SessionId = Cl#client.id,
    {ok, UserId} = session:get_user_id(SessionId),
    user_logic:has_eff_space(SessionId, UserId, SpaceId).


%%--------------------------------------------------------------------
%% @doc
%% Returns validity verificators for given request.
%% Returns a map with 'required', 'optional' and 'at_least_one' keys.
%% Under each of them, there is a map:
%%      Key => {type_verificator, value_verificator}
%% Which means how value of given Key should be validated.
%% @end
%%--------------------------------------------------------------------
-spec validate(op_logic:req()) -> op_validator:op_logic_params_signature().
validate(#el_req{operation = create, gri = #gri{aspect = {index, _}}}) -> #{
    required => #{
        <<"application/javascript">> => {binary, any}
    },
    optional => #{
        <<"spatial">> => {boolean, any},
        <<"update_min_changes">> => {integer, {not_lower_than, 1}},
        <<"replica_update_min_changes">> => {integer, {not_lower_than, 1}},
        <<"providers[]">> => {any, any}
    }
};
validate(#el_req{operation = create, gri = #gri{aspect = {index_reduce_function, _}}}) -> #{
    required => #{<<"application/javascript">> => {binary, any}}
};

validate(#el_req{operation = get, gri = #gri{aspect = indices}}) -> #{
    optional => #{
        <<"limit">> => {integer, {between, 0, ?MAX_LIST_LIMIT}},
        <<"page_token">> => {binary, any}
    }
};
validate(#el_req{operation = get, gri = #gri{aspect = {query_index, _}}}) -> #{
    optional => #{
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
validate(#el_req{operation = get, gri = #gri{aspect = transfers}}) -> #{
    optional => #{
        <<"state">> => {binary, [<<"waiting">>, <<"ongoing">>, <<"ended">>]},
        <<"limit">> => {integer, {between, 0, ?MAX_LIST_LIMIT}},
        <<"page_token">> => {binary, any}
    }
};

validate(#el_req{operation = update, gri = #gri{aspect = {index, _}}}) -> #{
    optional => #{
        <<"application/javascript">> => {binary, any},
        <<"spatial">> => {boolean, any},
        <<"update_min_changes">> => {integer, {not_lower_than, 1}},
        <<"replica_update_min_changes">> => {integer, {not_lower_than, 1}},
        <<"providers[]">> => {any, any}
    }
};

validate(_) -> #{}.


%%%===================================================================
%%% Internal functions
%%%===================================================================


-spec ensure_space_supported_by_providers(od_space:id(), [od_provider:id()]) ->
    ok | no_return().
ensure_space_supported_by_providers(_SpaceId, undefined) ->
    ok;
ensure_space_supported_by_providers(SpaceId, Providers) ->
    lists:foreach(fun(ProviderId) ->
        case space_logic:is_supported(?ROOT_SESS_ID, SpaceId, ProviderId) of
            true ->
                ok;
            false ->
                throw(?ERROR_SPACE_NOT_SUPPORTED_BY(ProviderId))
        end
    end, Providers).


-spec prepare_index_options(maps:map()) -> list().
prepare_index_options(Data) ->
    Options = case maps:get(<<"replica_update_min_changes">>, Data, undefined) of
        undefined ->
            [];
        ReplicaUpdateMinChanges ->
            [{replica_update_min_changes, ReplicaUpdateMinChanges}]
    end,

    case maps:get(<<"update_min_changes">>, Data, undefined) of
        undefined ->
            Options;
        UpdateMinChanges ->
            [{update_min_changes, UpdateMinChanges} | Options]
    end.
