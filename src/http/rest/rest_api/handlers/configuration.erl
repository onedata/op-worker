%%%--------------------------------------------------------------------
%%% @author Wojciech Geisler
%%% @copyright (C) 2018 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%--------------------------------------------------------------------
%%% @doc
%%% handler for the configuration endpoint.
%%% @end
%%%--------------------------------------------------------------------
-module(configuration).
-author("Wojciech Geisler").

-include("http/http_common.hrl").
-include("global_definitions.hrl").
-include("modules/rtransfer/rtransfer.hrl").

-include_lib("ctool/include/logging.hrl").

-define(to_binaries(__List), [list_to_binary(V) || V <- __List]).

%% API
-export([terminate/3, allowed_methods/2, is_authorized/2,
    content_types_provided/2]).
-export([gather_configuration/0]).

%% resource functions
-export([get_configuration/2]).

%%%===================================================================
%%% API
%%%===================================================================

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
-spec is_authorized(req(), maps:map()) -> {true | {false, binary()} | stop, req(), maps:map()}.
is_authorized(Req, State) ->
    {true, Req, State}.

%%--------------------------------------------------------------------
%% @doc @equiv pre_handler:content_types_provided/2
%%--------------------------------------------------------------------
-spec content_types_provided(req(), maps:map()) -> {[{binary(), atom()}], req(), maps:map()}.
content_types_provided(Req, State) ->
    {[
        {<<"application/json">>, get_configuration}
    ], Req, State}.


%%--------------------------------------------------------------------
%% @doc Returns contents of the configuration object.
%% @end
%%--------------------------------------------------------------------
-spec gather_configuration() -> #{binary() := term()}.
gather_configuration() ->
    ProviderId = oneprovider:get_id_or_undefined(),
    Name = case provider_logic:get_name() of
        {ok, N} -> N;
        _ -> undefined
    end,
    Domain = case provider_logic:get_domain() of
        {ok, D} -> D;
        _ -> undefined
    end,
    OnezoneDomain = case ProviderId of
        undefined -> undefined;
        _ -> list_to_binary(oneprovider:get_oz_domain())
    end,
    CompOzVersions = application:get_env(?APP_NAME, compatible_oz_versions, []),
    CompOpVersions = application:get_env(?APP_NAME, compatible_op_versions, []),
    CompOcVersions = application:get_env(?APP_NAME, compatible_oc_versions, []),

    #{
        <<"providerId">> => gs_protocol:undefined_to_null(ProviderId),
        <<"name">> => gs_protocol:undefined_to_null(Name),
        <<"domain">> => gs_protocol:undefined_to_null(Domain),
        <<"onezoneDomain">> => gs_protocol:undefined_to_null(OnezoneDomain),
        <<"version">> => oneprovider:get_version(),
        <<"build">> => oneprovider:get_build(),
        <<"rtransferPort">> => ?RTRANSFER_PORT,
        <<"compatibleOnezoneVersions">> => ?to_binaries(CompOzVersions),
        <<"compatibleOneproviderVersions">> => ?to_binaries(CompOpVersions),
        <<"compatibleOneclientVersions">> => ?to_binaries(CompOcVersions)
    }.

%%%===================================================================
%%% Content type handler functions
%%%===================================================================

%%--------------------------------------------------------------------
%% '/api/v3/oneprovider/configuration'
%% @doc This method returns public Oneprovider onfiguration info.
%%
%% HTTP method: GET
%%--------------------------------------------------------------------
-spec get_configuration(req(), map()) -> {term(), req(), map()}.
get_configuration(Req, State) ->
    {json_utils:encode(gather_configuration()), Req, State}.
