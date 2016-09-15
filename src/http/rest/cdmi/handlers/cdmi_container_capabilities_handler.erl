%%%--------------------------------------------------------------------
%%% @author Piotr Ociepka
%%% @copyright (C) 2015 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%--------------------------------------------------------------------
%%% @doc This is a cdmi handler module providing access to cdmi_capabilities
%%% which are used to discover operation that can be performed with containers.
%%% @end
%%%--------------------------------------------------------------------
-module(cdmi_container_capabilities_handler).

-include("http/http_common.hrl").

-include("http/rest/cdmi/cdmi_capabilities.hrl").

%% API
-export([rest_init/2, terminate/3, allowed_methods/2, malformed_request/2, content_types_provided/2]).
-export([get_cdmi_capability/2]).

%%--------------------------------------------------------------------
%% @doc @equiv pre_handler:rest_init/2
%%--------------------------------------------------------------------
-spec rest_init(req(), term()) -> {ok, req(), maps:map()} | {shutdown, req()}.
rest_init(Req, _Opts) ->
    {ok, Req, #{}}.

%%--------------------------------------------------------------------
%% @doc @equiv pre_handler:terminate/3
%%--------------------------------------------------------------------
-spec terminate(Reason :: term(), req(), maps:map()) -> ok.
terminate(_, _, _) ->
    ok.

%% ====================================================================
%% @doc @equiv pre_handler:allowed_methods/2
%% ====================================================================
-spec allowed_methods(req(), maps:map()) -> {[binary()], req(), maps:map()}.
allowed_methods(Req, State) ->
    {[<<"GET">>], Req, State}.

%% ====================================================================
%% @doc @equiv pre_handler:malformed_request/2
%% ====================================================================
-spec malformed_request(req(), maps:map()) -> {boolean(), req(), maps:map()} | no_return().
malformed_request(Req, State) ->
    cdmi_arg_parser:malformed_capability_request(Req, State).

%% ====================================================================
%% @doc @equiv pre_handler:content_types_provided/2
%% ====================================================================
-spec content_types_provided(req(), maps:map()) -> {[{ContentType, Method}], req(), maps:map()} when
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
-spec get_cdmi_capability(req(), maps:map()) -> {binary(), req(), maps:map()}.
get_cdmi_capability(Req, #{options := Opts} = State) ->
    NonEmptyOpts = utils:ensure_defined(Opts, [], ?default_get_capability_opts),
    RawCapabilities = prepare_capability_ans(NonEmptyOpts),
    Capabilities = json_utils:encode_map(RawCapabilities),
    {Capabilities, Req, State}.

%% ====================================================================
%% Internal functions
%% ====================================================================

%% ====================================================================
%% @doc Return proplist contains CDMI answer
%% ====================================================================
-spec prepare_capability_ans([Opt :: binary()]) -> [{Capability :: binary(), Value :: term()}].
prepare_capability_ans([]) ->
    #{};
prepare_capability_ans([<<"objectType">> | Tail]) ->
    (prepare_capability_ans(Tail))#{<<"objectType">> => <<"application/cdmi-capability">>};
prepare_capability_ans([<<"objectID">> | Tail]) ->
    (prepare_capability_ans(Tail))#{<<"objectID">> => ?container_capability_id};
prepare_capability_ans([<<"objectName">> | Tail]) ->
    (prepare_capability_ans(Tail))#{<<"objectName">> => filepath_utils:ensure_ends_with_slash(filename:basename(?container_capability_path))};
prepare_capability_ans([<<"parentURI">> | Tail]) ->
    (prepare_capability_ans(Tail))#{<<"parentURI">> => ?root_capability_path};
prepare_capability_ans([<<"parentID">> | Tail]) ->
    (prepare_capability_ans(Tail))#{<<"parentID">> => ?root_capability_id};
prepare_capability_ans([<<"capabilities">> | Tail]) ->
    (prepare_capability_ans(Tail))#{<<"capabilities">> => ?container_capability_list};
prepare_capability_ans([<<"childrenrange">> | Tail]) ->
    prepare_capability_ans(Tail);
prepare_capability_ans([<<"children">> | Tail]) ->
    (prepare_capability_ans(Tail))#{<<"children">> => []}.

