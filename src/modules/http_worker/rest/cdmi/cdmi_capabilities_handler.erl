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
-module(cdmi_capabilities_handler).

-include("modules/http_worker/http_common.hrl").

%% API
-export([allowed_methods/2, malformed_request/2, resource_exists/2, content_types_provided/2, get_cdmi_capability/2]).

%% ====================================================================
%% @doc Returns binary list of methods that are allowed (i.e GET, PUT, DELETE).
-spec allowed_methods(req(), dict()) -> {[binary()], req(), dict()}.
%% ====================================================================
allowed_methods(Req, State) ->
  {[<<"GET">>], Req, State}.

%% ====================================================================
%% @doc
%% Checks if request contains all mandatory fields and their values are set properly
%% depending on requested operation
%% @end
-spec malformed_request(req(), dict()) -> {boolean(), req(), dict()} | no_return().
%% ====================================================================
malformed_request(Req, State) ->
  cdmi_arg_parser:malformed_request(Req, State).

%% ====================================================================
%% @doc Determines if resource, that can be obtained from state, exists.
-spec resource_exists(req(), dict()) -> {boolean(), req(), dict()}.
%% ====================================================================
resource_exists(Req, State) ->
  {false, Req, State}.

%% ====================================================================
%% @doc
%% Returns content types that can be provided and what functions should be used to process the request.
%% Before adding new content type make sure that adequate routing function
%% exists in cdmi_handler
%% @end
-spec content_types_provided(req(), dict()) -> {[{ContentType, Method}], req(), dict()} when
  ContentType :: binary(),
  Method :: atom().
%% ====================================================================
content_types_provided(Req, State) ->
  {[
    {<<"application/cdmi-capability">>, get_cdmi_capability}
  ], Req, State}.

%% ====================================================================
%% @doc Cowboy callback function
%% Handles GET requests for cdmi capability.
%% @end
-spec get_cdmi_capability(req(), dict()) -> {term(), req(), dict()}.
%% ====================================================================
get_cdmi_capability(Req, #{opts = Opts} = State) ->
  {[],Req,State}.
