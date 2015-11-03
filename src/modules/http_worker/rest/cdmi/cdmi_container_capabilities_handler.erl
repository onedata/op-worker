%% ===================================================================
%% @author Piotr Ociepka
%% @copyright (C): 2015 ACK CYFRONET AGH
%% This software is released under the MIT license
%% cited in 'LICENSE.txt'.
%% @end
%% ===================================================================
%% @doc This is a cdmi handler module providing access to cdmi_capabilities
%% which are used to discover operation that can be performed.
%% @end
%% ===================================================================
-module(cdmi_container_capabilities_handler).

-include("modules/http_worker/http_common.hrl").

%% API
-export([rest_init/2, terminate/3, allowed_methods/2, malformed_request/2, content_types_provided/2]).
-export([get_cdmi_capability/2]).

%%--------------------------------------------------------------------
%% @doc @equiv pre_handler:rest_init/2
%%--------------------------------------------------------------------
-spec rest_init(req(), term()) -> {ok, req(), #{}} | {shutdown, req()}.
rest_init(Req, _Opts) ->
  {ok, Req, #{}}.

%%--------------------------------------------------------------------
%% @doc @equiv pre_handler:terminate/3
%%--------------------------------------------------------------------
-spec terminate(Reason :: term(), req(), #{}) -> ok.
terminate(_, _, _) ->
  ok.

%% ====================================================================
%% @doc @equiv pre_handler:allowed_methods/2
%% ====================================================================
-spec allowed_methods(req(), #{}) -> {[binary()], req(), #{}}.
allowed_methods(Req, State) ->
  {[<<"GET">>], Req, State}.

%% ====================================================================
%% @doc @equiv pre_handler:malformed_request/2
%% ====================================================================
-spec malformed_request(req(), #{}) -> {boolean(), req(), #{}} | no_return().
malformed_request(Req, State) ->
  cdmi_arg_parser:malformed_request(Req, State).

%% ====================================================================
%% @doc @equiv pre_handler:content_types_provided/2
%% ====================================================================
-spec content_types_provided(req(), #{}) -> {[{ContentType, Method}], req(), #{}} when
  ContentType :: binary(),
  Method :: atom().
content_types_provided(Req, State) ->
  {[
    {<<"application/cdmi-capability">>, get_cdmi_capability}
  ], Req, State}.

%% ====================================================================
%% @doc Cowboy callback function
%% Handles GET requests for cdmi capability.
%% @end
%% ====================================================================
-spec get_cdmi_capability(req(), #{}) -> {term(), req(), #{}}.
get_cdmi_capability(Req, #{opts => Opts} = State) ->
  {[],Req,State}.
