%%%-------------------------------------------------------------------
%%% @author Michal Stanisz
%%% @copyright (C) 2022 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% This module handles middleware operations (create, get, update, delete) 
%%% corresponding to space statistics aspects.
%%% @end
%%%-------------------------------------------------------------------
-module(space_stats_middleware_handler).
-author("Michal Stanisz").

-behaviour(middleware_handler).

-include("middleware/middleware.hrl").
-include_lib("ctool/include/errors.hrl").
-include_lib("ctool/include/privileges.hrl").

%% middleware_handler callbacks
-export([data_spec/1, fetch_entity/1, authorize/2, validate/2]).
-export([create/1, get/2, update/1, delete/1]).


%%%===================================================================
%%% middleware_handler callbacks
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% {@link middleware_handler} callback data_spec/1.
%% @end
%%--------------------------------------------------------------------
-spec data_spec(middleware:req()) -> undefined | middleware_sanitizer:data_spec().
data_spec(#op_req{operation = get, gri = #gri{aspect = dir_stats_service_state}}) ->
    undefined;

data_spec(#op_req{operation = update, gri = #gri{aspect = dir_stats_service_state}}) -> #{
    required => #{
        <<"dirStatsServiceEnabled">> => {boolean, any}
    }
}.


%%--------------------------------------------------------------------
%% @doc
%% {@link middleware_handler} callback fetch_entity/1.
%% @end
%%--------------------------------------------------------------------
-spec fetch_entity(middleware:req()) -> {ok, middleware:versioned_entity()}.
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

authorize(#op_req{operation = get, auth = ?USER(UserId, SessionId), gri = #gri{
    id = SpaceId,
    aspect = dir_stats_service_state
}}, _) ->
    space_logic:has_eff_user(SessionId, SpaceId, UserId);

authorize(#op_req{operation = update, auth = ?USER(UserId), gri = #gri{
    id = SpaceId,
    aspect = dir_stats_service_state
}}, _) ->
    space_logic:has_eff_privilege(SpaceId, UserId, ?SPACE_UPDATE).


%%--------------------------------------------------------------------
%% @doc
%% {@link middleware_handler} callback validate/2.
%% @end
%%--------------------------------------------------------------------
-spec validate(middleware:req(), middleware:entity()) -> ok | no_return().
validate(#op_req{operation = get, gri = #gri{
    id = SpaceId,
    aspect = dir_stats_service_state
}}, _QosEntry) ->
    middleware_utils:assert_space_supported_locally(SpaceId);

validate(#op_req{operation = update, gri = #gri{
    id = SpaceId,
    aspect = dir_stats_service_state
}}, _) ->
    middleware_utils:assert_space_supported_locally(SpaceId).


%%--------------------------------------------------------------------
%% @doc
%% {@link middleware_handler} callback create/1.
%% @end
%%--------------------------------------------------------------------
-spec create(middleware:req()) -> middleware:create_result().
create(_) ->
    ?ERROR_NOT_SUPPORTED.


%%--------------------------------------------------------------------
%% @doc
%% {@link middleware_handler} callback get/2.
%% @end
%%--------------------------------------------------------------------
-spec get(middleware:req(), middleware:entity()) -> middleware:get_result().
get(#op_req{gri = #gri{id = SpaceId, aspect = dir_stats_service_state}}, _) ->
    {ok, SpaceSupportState} = space_support_state_api:get_support_state(SpaceId),
    {ok, SpaceSupportOpts} = space_support_state_api:get_support_opts(SpaceSupportState),
    {Status, Since} = case dir_stats_service_state:get_last_status_change_timestamp_if_in_enabled_status(
        SpaceSupportState#space_support_state.dir_stats_service_state
    ) of
        {ok, Timestamp} -> {<<"enabled">>, Timestamp};
        ?ERROR_DIR_STATS_DISABLED_FOR_SPACE -> {<<"disabled">>, undefined};
        ?ERROR_DIR_STATS_NOT_READY-> {<<"initializing">>, undefined}
    end,
    {ok, value, maps_utils:remove_undefined(#{
        <<"accountingEnabled">> => maps:get(accounting_enabled, SpaceSupportOpts),
        <<"dirStatsServiceStatus">> => Status,
        <<"since">> => Since
    })}.


%%--------------------------------------------------------------------
%% @doc
%% {@link middleware_handler} callback update/1.
%% @end
%%--------------------------------------------------------------------
-spec update(middleware:req()) -> middleware:update_result().
update(#op_req{gri = #gri{id = SpaceId, aspect = dir_stats_service_state}, data = Data}) ->
    case maps:get(<<"dirStatsServiceEnabled">>, Data) of
        true -> dir_stats_service_state:enable(SpaceId);
        false -> dir_stats_service_state:disable(SpaceId)
    end.


%%--------------------------------------------------------------------
%% @doc
%% {@link middleware_handler} callback delete/1.
%% @end
%%--------------------------------------------------------------------
-spec delete(middleware:req()) -> middleware:delete_result().
delete(_) ->
    ?ERROR_NOT_SUPPORTED.
