%%%-------------------------------------------------------------------
%%% @author Bartosz Walkowicz
%%% @copyright (C) 2019 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% This module implements entity logic plugin behaviour and handles
%%% entity logic operations corresponding to od_space model.
%%% @end
%%%-------------------------------------------------------------------
-module(op_space).
-author("Bartosz Walkowicz").

-include("op_logic.hrl").
-include("http/rest/rest_api/rest_errors.hrl").
-include("modules/datastore/transfer.hrl").
-include("modules/datastore/datastore_models.hrl").
-include_lib("ctool/include/logging.hrl").
-include_lib("ctool/include/privileges.hrl").
-include_lib("ctool/include/api_errors.hrl").
-include_lib("ctool/include/posix/errors.hrl").

-export([fetch_entity/1, operation_supported/3]).
-export([create/1, get/2, update/1, delete/1]).
-export([exists/2, authorize/2, validate/1]).
-export([entity_logic_plugin/0]).

-define(DEFAULT_TRANSFERS_LIST_LIMIT, 100).

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
create(#el_req{client = Cl, gri = #gri{}} = Req) ->
    ok.


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
    Spaces = lists:map(fun(SpaceId) ->
        {ok, SpaceName} = space_logic:get_name(SessionId, SpaceId),
        #{<<"spaceId">> => SpaceId, <<"name">> => SpaceName}
    end, EffSpacesIds),
    {ok, Spaces};

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

get(#el_req{client = Cl, gri = #gri{id = SpaceId, aspect = transfers}} = Req, _) ->
    space_membership:check_with_auth(Cl#client.id, SpaceId),

    PageToken = maps:get(<<"page_token">>, Req#el_req.data, <<"null">>),
    TransferState = maps:get(<<"status">>, Req#el_req.data, <<"ongoing">>),
    Limit = maps:get(<<"limit">>, Req#el_req.data, ?DEFAULT_TRANSFERS_LIST_LIMIT),

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
update(#el_req{client = Cl, gri = #gri{}} = Req) ->
    ok.


%%--------------------------------------------------------------------
%% @doc
%% Deletes a resource (aspect of entity) based on entity logic request.
%% @end
%%--------------------------------------------------------------------
-spec delete(op_logic:req()) -> op_logic:delete_result().
delete(#el_req{client = Cl, gri = #gri{}} = Req) ->
    ok.


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
authorize(_, _) ->
    true.


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
validate(#el_req{operation = get, gri = #gri{aspect = transfers}}) -> #{
    optional => #{
        <<"state">> => {binary, [<<"waiting">>, <<"ongoing">>, <<"ended">>]},
        <<"limit">> => {integer, {not_lower_than, 0}},
        <<"page_token">> => {binary, any}
    }
};
validate(_) -> #{}.


%%%===================================================================
%%% Internal functions
%%%===================================================================


-spec ensure_space_supported(od_space:id()) -> ok | no_return().
ensure_space_supported(SpaceId) when is_binary(SpaceId) ->
    case provider_logic:supports_space(SpaceId) of
        true -> ok;
        false -> throw(?ERROR_SPACE_NOT_SUPPORTED)
    end.
