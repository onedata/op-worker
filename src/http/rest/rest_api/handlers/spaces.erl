%%%--------------------------------------------------------------------
%%% @author Tomasz Lichon
%%% @copyright (C) 2016 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%--------------------------------------------------------------------
%%% @doc
%%% Handler for listing spaces.
%%% @end
%%%--------------------------------------------------------------------
-module(spaces).
-author("Tomasz Lichon").

-include("global_definitions.hrl").
-include("http/http_common.hrl").
-include_lib("cluster_worker/include/modules/datastore/datastore.hrl").
-include("modules/datastore/datastore_specific_models_def.hrl").
-include_lib("ctool/include/logging.hrl").
-include("http/rest/http_status.hrl").
-include("http/rest/rest_api/rest_errors.hrl").


%% API
-export([rest_init/2, terminate/3, allowed_methods/2, is_authorized/2,
    content_types_provided/2]).

%% resource functions
-export([list_spaces/2]).

%%%===================================================================
%%% API
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc @equiv pre_handler:rest_init/2
%%--------------------------------------------------------------------
-spec rest_init(req(), term()) -> {ok, req(), term()} | {shutdown, req()}.
rest_init(Req, _Opts) ->
    {ok, Req, #{}}.

%%--------------------------------------------------------------------
%% @doc @equiv pre_handler:terminate/3
%%--------------------------------------------------------------------
-spec terminate(Reason :: term(), req(), #{}) -> ok.
terminate(_, _, _) ->
    ok.

%%--------------------------------------------------------------------
%% @doc @equiv pre_handler:allowed_methods/2
%%--------------------------------------------------------------------
-spec allowed_methods(req(), #{} | {error, term()}) -> {[binary()], req(), #{}}.
allowed_methods(Req, State) ->
    {[<<"GET">>], Req, State}.

%%--------------------------------------------------------------------
%% @doc @equiv pre_handler:is_authorized/2
%%--------------------------------------------------------------------
-spec is_authorized(req(), #{}) -> {true | {false, binary()} | halt, req(), #{}}.
is_authorized(Req, State) ->
    onedata_auth_api:is_authorized(Req, State).

%%--------------------------------------------------------------------
%% @doc @equiv pre_handler:content_types_provided/2
%%--------------------------------------------------------------------
-spec content_types_provided(req(), #{}) -> {[{binary(), atom()}], req(), #{}}.
content_types_provided(Req, State) ->
    {[
        {<<"application/json">>, list_spaces}
    ], Req, State}.

%%%===================================================================
%%% Content type handler functions
%%%===================================================================

%%--------------------------------------------------------------------
%% '/api/v3/oneprovider/spaces'
%% @doc Returns the list of all user spaces.
%%
%% HTTP method: GET
%%
%%--------------------------------------------------------------------
-spec list_spaces(req(), #{}) -> {term(), req(), #{}}.
list_spaces(Req, State = #{auth := Auth}) ->
    {ok, UserId} = session:get_user_id(Auth),

    Client = fslogic_utils:session_to_rest_client(Auth),
    {ok, #document{value = #onedata_user{spaces = Spaces}}} = onedata_user:get_or_fetch(Client, UserId),
    RawResponse =
        lists:map(fun({SpaceId, SpaceName}) ->
            [{<<"name">>, SpaceName}, {<<"spaceId">>, SpaceId}]
        end, Spaces),
    Response = json_utils:encode(RawResponse),
    {Response, Req, State}.