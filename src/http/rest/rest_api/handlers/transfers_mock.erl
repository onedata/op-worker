%%%--------------------------------------------------------------------
%%% @author Michal Stanisz
%%% @copyright (C) 2018 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%--------------------------------------------------------------------
%%% @doc
%%% Handler for managing transfers mock.
%%% @end
%%%--------------------------------------------------------------------
-module(transfers_mock).
-author("Michal Stanisz").

-include("global_definitions.hrl").
-include("http/http_common.hrl").

%% API
-export([
    terminate/3, allowed_methods/2, 
    content_types_provided/2, delete_resource/2, content_types_accepted/2
]).

%% resource functions
-export([get_value/2, set_mock/2]).

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
-spec allowed_methods(req(), maps:map() | {error, term()}) ->
    {[binary()], req(), maps:map()}.
allowed_methods(Req, State) ->
    {[<<"GET">>, <<"DELETE">>, <<"POST">>], Req, State}.

%%--------------------------------------------------------------------
%% @doc @equiv pre_handler:content_types_provided/2
%%--------------------------------------------------------------------
-spec content_types_provided(req(), maps:map()) ->
    {[{binary(), atom()}], req(), maps:map()}.
content_types_provided(Req, State) ->
    {[
        {<<"application/json">>, get_value}
    ], Req, State}.

%%--------------------------------------------------------------------
%% '/api/v3/oneprovider/debug/transfers_mock'
%% @doc Sets rtransfer_mock to false.
%%
%% HTTP method: DELETE
%% @end
%%--------------------------------------------------------------------
-spec delete_resource(req(), maps:map()) -> {term(), req(), maps:map()}.
delete_resource(Req, State) ->
    application:set_env(?APP_NAME, rtransfer_mock, false),
    {true, Req, State}.


%%--------------------------------------------------------------------
%% @doc @equiv pre_handler:content_types_accepted/2
%%--------------------------------------------------------------------
-spec content_types_accepted(req(), maps:map()) ->
    {[{atom() | binary(), atom()}], req(), maps:map()}.
content_types_accepted(Req, State) ->
    {[
        {'*', set_mock}
    ], Req, State}.

%%%===================================================================
%%% Content type handler functions
%%%===================================================================

%%--------------------------------------------------------------------
%% '/api/v3/oneprovider/debug/transfers_mock'
%% @doc Returns current rtransfer_mock value.
%%
%% HTTP method: GET
%% @end
%%--------------------------------------------------------------------
-spec get_value(req(), maps:map()) -> {term(), req(), maps:map()}.
get_value(Req, State) ->
    Response = json_utils:encode(#{rtransfer_mock => application:get_env(?APP_NAME, rtransfer_mock, false)}),
    {Response, Req, State}.

%%-------------------------------------------------------------------
%% '/api/v3/oneprovider/debug/transfers_mock'
%% @doc Sets rtransfer_mock to true.
%%
%% HTTP method: POST
%% @end
%%-------------------------------------------------------------------
-spec set_mock(req(), maps:map()) -> {term(), req(), maps:map()}.
set_mock(Req, State) ->
    application:set_env(?APP_NAME, rtransfer_mock, true),
    {true, Req, State}.
