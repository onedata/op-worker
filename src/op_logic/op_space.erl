%%%-------------------------------------------------------------------
%%% @author Bartosz Walkowicz
%%% @copyright (C) 2019 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% This module handles op logic operations corresponding to op_space model.
%%% @end
%%%-------------------------------------------------------------------
-module(op_space).
-author("Bartosz Walkowicz").

-behaviour(op_logic_behaviour).

-include("op_logic.hrl").
-include("modules/datastore/transfer.hrl").
-include("http/rest/rest_api/rest_errors.hrl").
-include_lib("ctool/include/posix/errors.hrl").

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

-define(MAX_LIST_LIMIT, 1000).
-define(DEFAULT_INDEX_LIST_LIMIT, 100).
-define(DEFAULT_TRANSFER_LIST_LIMIT, 100).


%%%===================================================================
%%% API
%%%===================================================================


%%--------------------------------------------------------------------
%% @doc
%% Returns the op logic plugin module that handles model logic.
%% @end
%%--------------------------------------------------------------------
op_logic_plugin() ->
    op_space.


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
%% Returns data signature for given request.
%% Returns a map with 'required', 'optional' and 'at_least_one' keys.
%% Under each of them, there is a map:
%%      Key => {type_constraint, value_constraint}
%% Which means how value of given Key should be validated.
%% @end
%%--------------------------------------------------------------------
-spec data_spec(op_logic:req()) -> op_sanitizer:data_spec().
data_spec(#op_req{operation = create, gri = #gri{aspect = {index, _}}}) -> #{
    required => #{
        <<"application/javascript">> => {binary, non_empty}
    },
    optional => #{
        <<"spatial">> => {boolean, any},
        <<"update_min_changes">> => {integer, {not_lower_than, 1}},
        <<"replica_update_min_changes">> => {integer, {not_lower_than, 1}},
        <<"providers[]">> => {any,
            fun
                (ProviderId) when is_binary(ProviderId) ->
                    [ProviderId];
                (Providers) when is_list(Providers) ->
                    Providers
            end
        }
    }
};

data_spec(#op_req{operation = create, gri = #gri{aspect = {index_reduce_function, _}}}) -> #{
    required => #{<<"application/javascript">> => {binary, non_empty}}
};

data_spec(#op_req{operation = get, gri = #gri{aspect = As}}) when
    As =:= list;
    As =:= instance
->
    #{};

data_spec(#op_req{operation = get, gri = #gri{aspect = indices}}) -> #{
    optional => #{
        <<"limit">> => {integer, {between, 0, ?MAX_LIST_LIMIT}},
        <<"page_token">> => {binary, non_empty}
    }
};

data_spec(#op_req{operation = get, gri = #gri{aspect = {index, _}}}) ->
    #{};

data_spec(#op_req{operation = get, gri = #gri{aspect = {query_index, _}}}) -> #{
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

data_spec(#op_req{operation = get, gri = #gri{aspect = transfers}}) -> #{
    optional => #{
        <<"state">> => {binary, [<<"waiting">>, <<"ongoing">>, <<"ended">>]},
        <<"limit">> => {integer, {between, 0, ?MAX_LIST_LIMIT}},
        <<"page_token">> => {binary, non_empty}
    }
};

data_spec(#op_req{operation = update, gri = #gri{aspect = {index, _}}}) -> #{
    optional => #{
        <<"application/javascript">> => {binary, any},
        <<"spatial">> => {boolean, any},
        <<"update_min_changes">> => {integer, {not_lower_than, 1}},
        <<"replica_update_min_changes">> => {integer, {not_lower_than, 1}},
        <<"providers[]">> => {any,
            fun
                (ProviderId) when is_binary(ProviderId) ->
                    [ProviderId];
                (Providers) when is_list(Providers) ->
                    Providers
            end
        }
    }
};

data_spec(#op_req{operation = delete, gri = #gri{aspect = {index, _}}}) ->
    #{};

data_spec(#op_req{operation = delete, gri = #gri{aspect = {index_reduce_function, _}}}) ->
    #{}.


%%--------------------------------------------------------------------
%% @doc
%% Retrieves an entity from datastore based on its EntityId.
%% Should return ?ERROR_NOT_FOUND if the entity does not exist.
%% @end
%%--------------------------------------------------------------------
-spec fetch_entity(op_logic:entity_id()) ->
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

authorize(#op_req{operation = create, gri = #gri{aspect = {As, _}} = GRI} = Req, _) when
    As =:= index;
    As =:= index_reduce_function
->
    op_logic_utils:is_eff_space_member(Req#op_req.client, GRI#gri.id);

authorize(#op_req{operation = get, gri = #gri{aspect = list}}, _) ->
    true;

authorize(#op_req{operation = get, gri = #gri{id = SpaceId, aspect = As}} = Req, _) when
    As =:= instance;
    As =:= indices
->
    op_logic_utils:is_eff_space_member(Req#op_req.client, SpaceId);

authorize(#op_req{operation = get, gri = #gri{id = SpaceId, aspect = {As, _}}} = Req, _) when
    As =:= index;
    As =:= query_index
->
    op_logic_utils:is_eff_space_member(Req#op_req.client, SpaceId);

authorize(#op_req{operation = get, gri = #gri{id = SpaceId, aspect = transfers}} = Req, _) ->
    op_logic_utils:is_eff_space_member(Req#op_req.client, SpaceId);

authorize(#op_req{operation = update, gri = #gri{id = SpaceId, aspect = {index, _}}} = Req, _) ->
    op_logic_utils:is_eff_space_member(Req#op_req.client, SpaceId);

authorize(#op_req{operation = get, gri = #gri{id = SpaceId, aspect = {As, _}}} = Req, _) when
    As =:= index;
    As =:= index_reduce_function
->
    op_logic_utils:is_eff_space_member(Req#op_req.client, SpaceId).


%%--------------------------------------------------------------------
%% @doc
%% Determines if given request can be further processed
%% (e.g. checks whether space is supported locally).
%% Should throw custom error if not (e.g. ?ERROR_SPACE_NOT_SUPPORTED).
%% @end
%%--------------------------------------------------------------------
-spec validate(op_logic:req(), entity_logic:entity()) -> ok | no_return().
validate(#op_req{operation = create, gri = #gri{aspect = {index, _}} = GRI} = Req, _) ->
    SpaceId = GRI#gri.id,
    op_logic_utils:ensure_space_supported_locally(SpaceId),

    % In case of undefined `providers[]` local provider is chosen instead
    case maps:get(<<"providers[]">>, Req#op_req.data, undefined) of
        undefined ->
            ok;
        Providers ->
            lists:foreach(fun(ProviderId) ->
                op_logic_utils:ensure_space_supported_by(SpaceId, ProviderId)
            end, Providers)
    end;

validate(#op_req{operation = create, gri = #gri{aspect = {index_reduce_function, _}} = GRI}, _) ->
    op_logic_utils:ensure_space_supported_locally(GRI#gri.id);

validate(#op_req{operation = get, gri = #gri{aspect = list}}, _) ->
    ok;

validate(#op_req{operation = get, gri = #gri{id = SpaceId, aspect = As}}, _) when
    As =:= instance;
    As =:= indices
->
    op_logic_utils:ensure_space_supported_locally(SpaceId);

validate(#op_req{operation = get, gri = #gri{id = SpaceId, aspect = {As, _}}}, _) when
    As =:= index;
    As =:= query_index
->
    op_logic_utils:ensure_space_supported_locally(SpaceId);

validate(#op_req{operation = get, gri = #gri{id = SpaceId, aspect = transfers}}, _) ->
    op_logic_utils:ensure_space_supported_locally(SpaceId);

validate(#op_req{operation = update, gri = #gri{id = SpaceId, aspect = {index, _}}} = Req, _) ->
    op_logic_utils:ensure_space_supported_locally(SpaceId),

    % In case of undefined `providers[]` local provider is chosen instead
    case maps:get(<<"providers[]">>, Req#op_req.data, undefined) of
        undefined ->
            ok;
        Providers ->
            lists:foreach(fun(ProviderId) ->
                op_logic_utils:ensure_space_supported_by(SpaceId, ProviderId)
        end, Providers)
    end;

validate(#op_req{operation = get, gri = #gri{id = SpaceId, aspect = {As, _}}}, _) when
    As =:= index;
    As =:= index_reduce_function
->
    op_logic_utils:ensure_space_supported_locally(SpaceId).


%%--------------------------------------------------------------------
%% @doc
%% Creates a resource (aspect of entity) based on op logic request.
%% @end
%%--------------------------------------------------------------------
-spec create(op_logic:req()) -> op_logic:create_result().
create(#op_req{data = Data, gri = #gri{id = SpaceId, aspect = {index, IndexName}}}) ->
    index:save(
        SpaceId, IndexName,
        maps:get(<<"application/javascript">>, Data), undefined,
        prepare_index_options(Data),
        maps:get(<<"spatial">>, Data, false),
        maps:get(<<"providers[]">>, Data, [oneprovider:get_id()])
    );

create(#op_req{gri = #gri{id = SpaceId, aspect = {index_reduce_function, IndexName}}} = Req) ->
    ReduceFunction = maps:get(<<"application/javascript">>, Req#op_req.data),
    case index:update_reduce_function(SpaceId, IndexName, ReduceFunction) of
        {error, ?EINVAL} ->
            ?ERROR_BAD_VALUE_AMBIGUOUS_ID(<<"index_name">>);
        Result ->
            Result
    end.


%%--------------------------------------------------------------------
%% @doc
%% Retrieves a resource (aspect of entity) based on op logic request and
%% prefetched entity.
%% @end
%%--------------------------------------------------------------------
-spec get(op_logic:req(), op_logic:entity()) -> op_logic:get_result().
get(#op_req{client = #client{id = SessionId}, gri = #gri{aspect = list}}, _) ->
    {ok, UserId} = session:get_user_id(SessionId),
    {ok, EffSpacesIds} = user_logic:get_eff_spaces(SessionId, UserId),
    EffSpaces = lists:map(fun(SpaceId) ->
        {ok, SpaceName} = space_logic:get_name(SessionId, SpaceId),
        #{<<"spaceId">> => SpaceId, <<"name">> => SpaceName}
    end, EffSpacesIds),
    {ok, EffSpaces};

get(#op_req{client = Cl, gri = #gri{id = SpaceId, aspect = instance}}, _) ->
    case space_logic:get(Cl#client.id, SpaceId) of
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

get(#op_req{data = Data, gri = #gri{id = SpaceId, aspect = indices}}, _) ->
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

get(#op_req{gri = #gri{id = SpaceId, aspect = {index, IndexName}}}, _) ->
    case index:get_json(SpaceId, IndexName) of
        {error, ?EINVAL} ->
            ?ERROR_BAD_VALUE_AMBIGUOUS_ID(<<"index_name">>);
        Result ->
            Result
    end;

get(#op_req{gri = #gri{id = SpaceId, aspect = {query_index, IndexName}}} = Req, _) ->
    Options = index_utils:sanitize_query_options(Req#op_req.data),
    case index:query(SpaceId, IndexName, Options) of
        {ok, {Rows}} ->
            QueryResult = lists:map(fun(Row) -> maps:from_list(Row) end, Rows),
            {ok, QueryResult};
        {error, ?EINVAL} ->
            ?ERROR_BAD_VALUE_AMBIGUOUS_ID(<<"index_name">>);
        Error ->
            Error
    end;

get(#op_req{data = Data, gri = #gri{id = SpaceId, aspect = transfers}}, _) ->
    op_logic_utils:ensure_space_supported_locally(SpaceId),

    PageToken = maps:get(<<"page_token">>, Data, <<"null">>),
    TransferState = maps:get(<<"state">>, Data, <<"ongoing">>),
    Limit = maps:get(<<"limit">>, Data, ?DEFAULT_TRANSFER_LIST_LIMIT),

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
%% Updates a resource (aspect of entity) based on op logic request.
%% @end
%%--------------------------------------------------------------------
-spec update(op_logic:req()) -> op_logic:update_result().
update(#op_req{data = Data, gri = #gri{id = SpaceId, aspect = {index, IndexName}}}) ->
    MapFunctionRaw = maps:get(<<"application/javascript">>, Data),
    MapFun = utils:ensure_defined(MapFunctionRaw, <<>>, undefined),

    case index:update(
        SpaceId, IndexName,
        MapFun,
        prepare_index_options(Data),
        maps:get(<<"spatial">>, Data, undefined),
        maps:get(<<"providers[]">>, Data, undefined)
    ) of
        {error, ?EINVAL} ->
            ?ERROR_BAD_VALUE_AMBIGUOUS_ID(<<"index_name">>);
        Result ->
            Result
    end.


%%--------------------------------------------------------------------
%% @doc
%% Deletes a resource (aspect of entity) based on op logic request.
%% @end
%%--------------------------------------------------------------------
-spec delete(op_logic:req()) -> op_logic:delete_result().
delete(#op_req{gri = #gri{id = SpaceId, aspect = {index, IndexName}}}) ->
    case index:delete(SpaceId, IndexName) of
        {error, ?EINVAL} ->
            ?ERROR_BAD_VALUE_AMBIGUOUS_ID(<<"index_name">>);
        Result ->
            Result
    end;

delete(#op_req{gri = #gri{id = SpaceId, aspect = {index_reduce_function, IndexName}}}) ->
    case index:update_reduce_function(SpaceId, IndexName, undefined) of
        {error, ?EINVAL} ->
            ?ERROR_BAD_VALUE_AMBIGUOUS_ID(<<"index_name">>);
        Result ->
            Result
    end.


%%%===================================================================
%%% Internal functions
%%%===================================================================


%% @private
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
