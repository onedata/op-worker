%%%-------------------------------------------------------------------
%%% @author Bartosz Walkowicz
%%% @copyright (C) 2021 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% This module handles middleware operations (create, get, update, delete)
%%% corresponding to automation stores.
%%% @end
%%%-------------------------------------------------------------------
-module(atm_store_middleware_plugin).
-author("Bartosz Walkowicz").

-behaviour(middleware_router).
-behaviour(middleware_handler).

-include("middleware/middleware.hrl").
-include("modules/automation/atm_execution.hrl").
-include_lib("ctool/include/errors.hrl").
-include_lib("ctool/include/logging.hrl").
-include_lib("ctool/include/privileges.hrl").

%% middleware_router callbacks
-export([resolve_handler/3]).

%% middleware_handler callbacks
-export([data_spec/1, fetch_entity/1, authorize/2, validate/2]).
-export([create/1, get/2, update/1, delete/1]).


-record(atm_store_ctx, {
    store :: atm_store:record(),
    workflow_execution :: atm_workflow_execution:record()
}).

-define(MAX_LIST_LIMIT, 1000).
-define(DEFAULT_LIST_LIMIT, 1000).


%%%===================================================================
%%% middleware_router callbacks
%%%===================================================================


%%--------------------------------------------------------------------
%% @doc
%% {@link middleware_router} callback resolve_handler/3.
%% @end
%%--------------------------------------------------------------------
-spec resolve_handler(middleware:operation(), gri:aspect(), middleware:scope()) ->
    module() | no_return().
resolve_handler(get, instance, private) -> ?MODULE;
resolve_handler(get, content, private) -> ?MODULE;

resolve_handler(_, _, _) -> throw(?ERROR_NOT_SUPPORTED).


%%%===================================================================
%%% middleware_handler callbacks
%%%===================================================================


%%--------------------------------------------------------------------
%% @doc
%% {@link middleware_handler} callback data_spec/1.
%% @end
%%--------------------------------------------------------------------
-spec data_spec(middleware:req()) -> undefined | middleware_sanitizer:data_spec().
data_spec(#op_req{operation = get, gri = #gri{aspect = instance}}) ->
    undefined;

data_spec(#op_req{operation = get, gri = #gri{aspect = content}}) -> #{
    optional => #{
        <<"index">> => {binary, any},
        <<"token">> => {binary, non_empty},
        <<"offset">> => {integer, any},
        <<"limit">> => {integer, {between, 1, ?MAX_LIST_LIMIT}}
    }
}.


%%--------------------------------------------------------------------
%% @doc
%% {@link middleware_handler} callback fetch_entity/1.
%%
%% For now fetches only records for authorized users.
%% @end
%%--------------------------------------------------------------------
-spec fetch_entity(middleware:req()) ->
    {ok, middleware:versioned_entity()} | errors:error().
fetch_entity(#op_req{auth = ?NOBODY}) ->
    ?ERROR_UNAUTHORIZED;

fetch_entity(#op_req{gri = #gri{id = AtmStoreId, scope = private}}) ->
    case atm_store_api:get(AtmStoreId) of
        {ok, #atm_store{workflow_execution_id = AtmWorkflowExecutionId} = AtmStore} ->
            case atm_workflow_execution_api:get(AtmWorkflowExecutionId) of
                {ok, AtmWorkflowExecution} ->
                    AtmStoreCtx = #atm_store_ctx{
                        store = AtmStore,
                        workflow_execution = AtmWorkflowExecution
                    },
                    {ok, {AtmStoreCtx, 1}};
                {error, _} = Error ->
                    Error
            end;
        {error, _} = Error ->
            Error
    end.


%%--------------------------------------------------------------------
%% @doc
%% {@link middleware_handler} callback authorize/2.
%% @end
%%--------------------------------------------------------------------
-spec authorize(middleware:req(), middleware:entity()) -> boolean().
authorize(#op_req{auth = ?GUEST}, _) ->
    false;

authorize(#op_req{operation = get, auth = Auth, gri = #gri{aspect = As}}, #atm_store_ctx{
    workflow_execution = AtmWorkflowExecution
}) when
    As =:= instance;
    As =:= content
->
    atm_workflow_execution_middleware_plugin:has_access_to_workflow_execution_details(
        Auth, AtmWorkflowExecution
    ).


%%--------------------------------------------------------------------
%% @doc
%% {@link middleware_handler} callback validate/2.
%% @end
%%--------------------------------------------------------------------
-spec validate(middleware:req(), middleware:entity()) -> ok | no_return().
validate(#op_req{operation = get, gri = #gri{aspect = As}}, _) when
    As =:= instance;
    As =:= content
->
    % Doc was already fetched in 'fetch_entity' so space must be supported locally
    ok.


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
get(#op_req{gri = #gri{aspect = instance, scope = private}}, #atm_store_ctx{store = AtmStore}) ->
    {ok, AtmStore};

get(#op_req{auth = Auth, data = Data, gri = #gri{aspect = content, scope = private}}, #atm_store_ctx{
    store = #atm_store{workflow_execution_id = AtmWorkflowExecutionId} = AtmStore,
    workflow_execution = #atm_workflow_execution{space_id = SpaceId}
}) ->
    AtmWorkflowExecutionAuth = atm_workflow_execution_auth:build(
        SpaceId, AtmWorkflowExecutionId, Auth#auth.session_id
    ),

    Offset = maps:get(<<"offset">>, Data, 0),
    Limit = maps:get(<<"limit">>, Data, ?DEFAULT_LIST_LIMIT),

    BrowseOpts = case maps:get(<<"token">>, Data, undefined) of
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

    {ok, value, atm_store_api:browse_content(AtmWorkflowExecutionAuth, BrowseOpts, AtmStore)}.


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
