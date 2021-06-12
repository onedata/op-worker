%%%-------------------------------------------------------------------
%%% @author Bartosz Walkowicz
%%% @copyright (C) 2021 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% This module handles middleware operations (create, get, update, delete)
%%% corresponding to space qos aspects.
%%% @end
%%%-------------------------------------------------------------------
-module(space_qos_middleware_handler).
-author("Bartosz Walkowicz").

-behaviour(middleware_handler).

-include("middleware/middleware.hrl").
-include_lib("ctool/include/errors.hrl").
-include_lib("ctool/include/privileges.hrl").

%% middleware_handler callbacks
-export([data_spec/1, fetch_entity/1, authorize/2, validate/2]).
-export([create/1, get/2, update/1, delete/1]).


-define(MAX_LIST_LIMIT, 1000).
-define(DEFAULT_LIST_LIMIT, 1000).


%%%===================================================================
%%% middleware_handler callbacks
%%%===================================================================


%%--------------------------------------------------------------------
%% @doc
%% {@link middleware_handler} callback data_spec/1.
%% @end
%%--------------------------------------------------------------------
-spec data_spec(middleware:req()) -> undefined | middleware_sanitizer:data_spec().
data_spec(#op_req{operation = create, gri = #gri{aspect = evaluate_qos_expression}}) -> #{
    required => #{
        <<"expression">> => {binary, non_empty}
    }
};

data_spec(#op_req{operation = get, gri = #gri{aspect = available_qos_parameters}}) ->
    undefined.


%%--------------------------------------------------------------------
%% @doc
%% {@link middleware_handler} callback fetch_entity/1.
%% @end
%%--------------------------------------------------------------------
-spec fetch_entity(middleware:req()) ->
    {ok, middleware:versioned_entity()} | errors:error().
fetch_entity(_) ->
    {ok, {undefined, 1}}.


%%--------------------------------------------------------------------
%% @doc
%% {@link middleware_handler} callback authorize/2.
%% @end
%%--------------------------------------------------------------------
-spec authorize(middleware:req(), middleware:entity()) -> boolean().
authorize(#op_req{auth = ?GUEST}, _) ->
    false;

authorize(#op_req{operation = create, auth = ?USER(UserId), gri = #gri{
    id = SpaceId,
    aspect = evaluate_qos_expression
}}, _) ->
    space_logic:has_eff_privilege(SpaceId, UserId, ?SPACE_MANAGE_QOS);

authorize(#op_req{operation = get, auth = ?USER(UserId, SessionId), gri = #gri{
    id = SpaceId,
    aspect = available_qos_parameters
}}, _) ->
    space_logic:has_eff_user(SessionId, SpaceId, UserId).


%%--------------------------------------------------------------------
%% @doc
%% {@link middleware_handler} callback validate/2.
%% @end
%%--------------------------------------------------------------------
-spec validate(middleware:req(), middleware:entity()) -> ok | no_return().
validate(#op_req{operation = create, gri = #gri{
    id = SpaceId,
    aspect = evaluate_qos_expression
}}, _) ->
    middleware_utils:assert_space_supported_locally(SpaceId);

validate(#op_req{operation = get, gri = #gri{
    id = SpaceId,
    aspect = available_qos_parameters
}}, _QosEntry) ->
    middleware_utils:assert_space_supported_locally(SpaceId).


%%--------------------------------------------------------------------
%% @doc
%% {@link middleware_handler} callback create/1.
%% @end
%%--------------------------------------------------------------------
-spec create(middleware:req()) -> middleware:create_result().
create(#op_req{gri = #gri{id = SpaceId, aspect = evaluate_qos_expression}} = Req) ->
    QosExpressionInfix = maps:get(<<"expression">>, Req#op_req.data),
    QosExpression = qos_expression:parse(QosExpressionInfix),
    StorageIds = qos_expression:get_matching_storages_in_space(SpaceId, QosExpression),
    StoragesList = lists:map(fun(StorageId) ->
        StorageName = storage:fetch_name_of_remote_storage(StorageId, SpaceId),
        ProviderId = storage:fetch_provider_id_of_remote_storage(StorageId, SpaceId),
        #{<<"id">> => StorageId, <<"name">> => StorageName, <<"providerId">> => ProviderId}
    end, StorageIds),
    {ok, value, #{
        <<"expressionRpn">> => qos_expression:to_rpn(QosExpression),
        %% @TODO VFS-6520 not needed when storage api is implemented
        <<"matchingStorages">> => StoragesList 
    }}.


%%--------------------------------------------------------------------
%% @doc
%% {@link middleware_handler} callback get/2.
%% @end
%%--------------------------------------------------------------------
-spec get(middleware:req(), middleware:entity()) -> middleware:get_result().
get(#op_req{gri = #gri{id = SpaceId, aspect = available_qos_parameters}}, _) ->
    {ok, Storages} = space_logic:get_all_storage_ids(SpaceId),
    Res = lists:foldl(fun(StorageId, OuterAcc) ->
        QosParameters = storage:fetch_qos_parameters_of_remote_storage(StorageId, SpaceId),
        maps:fold(fun(ParameterKey, Value, InnerAcc) ->
            Key = case is_number(Value) of
                true -> <<"numberValues">>;
                false -> <<"stringValues">>
            end,
            InnerAcc#{ParameterKey => maps:update_with(
                Key,
                fun(Values) -> lists:usort([Value | Values]) end,
                [Value],
                maps:get(ParameterKey, InnerAcc, #{
                    <<"stringValues">> => [],
                    <<"numberValues">> => []
                })
            )}
        end, OuterAcc, QosParameters)
    end, #{}, Storages),
    {ok, Res}.


%%--------------------------------------------------------------------
%% @doc
%% {@link middleware_handler} callback update/1.
%% @end
%%--------------------------------------------------------------------
-spec update(middleware:req()) -> middleware:update_result().
update(_) ->
    ?ERROR_NOT_SUPPORTED.


%%--------------------------------------------------------------------
%% @doc
%% {@link middleware_handler} callback delete/1.
%% @end
%%--------------------------------------------------------------------
-spec delete(middleware:req()) -> middleware:delete_result().
delete(_) ->
    ?ERROR_NOT_SUPPORTED.
