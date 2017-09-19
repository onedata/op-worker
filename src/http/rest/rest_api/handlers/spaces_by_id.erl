%%%--------------------------------------------------------------------
%%% @author Tomasz Lichon
%%% @copyright (C) 2016 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%--------------------------------------------------------------------
%%% @doc
%%% Handler for listing spaces by id.
%%% @end
%%%--------------------------------------------------------------------
-module(spaces_by_id).
-author("Tomasz Lichon").

-include("global_definitions.hrl").
-include("http/http_common.hrl").
-include("modules/datastore/datastore_models.hrl").
-include_lib("ctool/include/logging.hrl").
-include("http/rest/http_status.hrl").
-include("http/rest/rest_api/rest_errors.hrl").


%% API
-export([rest_init/2, terminate/3, allowed_methods/2, is_authorized/2,
    content_types_provided/2]).

%% resource functions
-export([get_space/2]).

%%%===================================================================
%%% API
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc @equiv pre_handler:rest_init/2
%%--------------------------------------------------------------------
-spec rest_init(req(), term()) -> {ok, req(), term()} | {shutdown, req()}.
rest_init(Req, State) ->
    {ok, Req, State}.

%%--------------------------------------------------------------------
%% @doc @equiv pre_handler:terminate/3
%%--------------------------------------------------------------------
-spec terminate(Reason :: term(), req(), maps:map()) -> ok.
terminate(_, _, _) ->
    ok.

%%--------------------------------------------------------------------
%% @doc @equiv pre_handler:allowed_methods/2
%%--------------------------------------------------------------------
-spec allowed_methods(req(), maps:map() | {error, term()}) -> {[binary()], req(), maps:map()}.
allowed_methods(Req, State) ->
    {[<<"GET">>], Req, State}.

%%--------------------------------------------------------------------
%% @doc @equiv pre_handler:is_authorized/2
%%--------------------------------------------------------------------
-spec is_authorized(req(), maps:map()) -> {true | {false, binary()} | halt, req(), maps:map()}.
is_authorized(Req, State) ->
    onedata_auth_api:is_authorized(Req, State).

%%--------------------------------------------------------------------
%% @doc @equiv pre_handler:content_types_provided/2
%%--------------------------------------------------------------------
-spec content_types_provided(req(), maps:map()) -> {[{binary(), atom()}], req(), maps:map()}.
content_types_provided(Req, State) ->
    {[
        {<<"application/json">>, get_space}
    ], Req, State}.

%%%===================================================================
%%% Content type handler functions
%%%===================================================================

%%--------------------------------------------------------------------
%% '/api/v3/oneprovider/spaces/{sid}'
%% @doc Returns the basic information about space with given ID.\n
%%
%% HTTP method: GET
%%
%% @param sid Space ID.
%%--------------------------------------------------------------------
-spec get_space(req(), maps:map()) -> {term(), req(), maps:map()}.
get_space(Req, State) ->
    {State2, Req2} = validator:parse_space_id(Req, State),

    #{auth := Auth, space_id := SpaceId} = State2,

    space_membership:check_with_auth(Auth, SpaceId),
    {ok, #document{value = #od_space{name = Name, providers = Providers}}} =
        od_space:get_or_fetch(Auth, SpaceId),
    ProvidersRawResponse = lists:map(fun(ProviderId) ->
        {ok, #document{value = #od_provider{client_name = ProviderName}}} =
            od_provider:get_or_fetch(ProviderId),
        #{
            <<"providerId">> => ProviderId,
            <<"providerName">> => ProviderName
        }
    end, Providers),
    RawResponse = #{
        <<"name">> => Name,
        <<"providers">> => ProvidersRawResponse,
        <<"spaceId">> => SpaceId
    },
    Response = json_utils:encode_map(RawResponse),
    {Response, Req2, State2}.