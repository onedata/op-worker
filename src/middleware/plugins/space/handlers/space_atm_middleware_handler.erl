%%%-------------------------------------------------------------------
%%% @author Bartosz Walkowicz
%%% @copyright (C) 2021 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% This module handles middleware operations (create, get, update, delete)
%%% corresponding to space automation aspects.
%%% @end
%%%-------------------------------------------------------------------
-module(space_atm_middleware_handler).
-author("Bartosz Walkowicz").

-behaviour(middleware_handler).

-include("middleware/middleware.hrl").
-include("modules/automation/atm_execution.hrl").
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
data_spec(#op_req{operation = get, gri = #gri{aspect = atm_workflow_execution_summaries}}) -> #{
    optional => #{
        <<"phase">> => {atom, [?WAITING_PHASE, ?ONGOING_PHASE, ?ENDED_PHASE]},
        <<"index">> => {binary, any},
        <<"token">> => {binary, non_empty},
        <<"offset">> => {integer, any},
        <<"limit">> => {integer, {between, 1, ?MAX_LIST_LIMIT}}
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

authorize(#op_req{operation = get, auth = ?USER(UserId), gri = #gri{
    id = SpaceId,
    aspect = atm_workflow_execution_summaries
}}, _) ->
    space_logic:has_eff_privilege(SpaceId, UserId, ?SPACE_VIEW_ATM_WORKFLOW_EXECUTIONS).


%%--------------------------------------------------------------------
%% @doc
%% {@link middleware_handler} callback validate/2.
%% @end
%%--------------------------------------------------------------------
-spec validate(middleware:req(), middleware:entity()) -> ok | no_return().
validate(#op_req{operation = get, gri = #gri{
    id = SpaceId,
    aspect = atm_workflow_execution_summaries
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
get(#op_req{data = Data, gri = #gri{id = SpaceId, aspect = atm_workflow_execution_summaries}}, _) ->
    Phase = maps:get(<<"phase">>, Data, ?ONGOING_PHASE),

    Offset = maps:get(<<"offset">>, Data, 0),
    Limit = maps:get(<<"limit">>, Data, ?DEFAULT_LIST_LIMIT),

    ListingOpts = case maps:get(<<"token">>, Data, undefined) of
        undefined ->
            Index = maps:get(<<"index">>, Data, undefined),
            maps_utils:put_if_defined(#{offset => Offset, limit => Limit}, start_index, Index);
        Token when is_binary(Token) ->
            % if token is passed, offset has to be increased by 1
            % to ensure that listing using token is exclusive
            #{
                start_index => http_utils:base64url_decode(Token),
                offset => Offset + 1,
                limit => Limit
            }
    end,

    {ok, Entries, IsLast} = atm_workflow_execution_api:list(SpaceId, Phase, summary, ListingOpts),
    {_, AtmWorkflowExecutionSummaries} = lists:unzip(Entries),

    {ok, value, {AtmWorkflowExecutionSummaries, IsLast}}.


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
