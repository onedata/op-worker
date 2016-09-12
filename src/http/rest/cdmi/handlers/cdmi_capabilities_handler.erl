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

-include("http/http_common.hrl").
-include("http/rest/cdmi/cdmi_errors.hrl").

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
-spec get_cdmi_capability(req(), maps:map()) -> {term(), req(), maps:map()}.
get_cdmi_capability(Req, #{options := Opts} = State) ->
    RawCapabilities = case Opts of
                          [] ->
                              prepare_capability_ans(?default_get_capability_opts);
                          X -> prepare_capability_ans(X)
                      end,
    Capabilities = json_utils:encode(RawCapabilities),
    {Capabilities, Req, State}.

%% ====================================================================
%% Internal functions
%% ====================================================================

%% ====================================================================
%% @doc Return proplist contains CDMI answer
%% ====================================================================
-spec prepare_capability_ans([Opt :: binary()]) ->
    [{Capability :: binary(), Value :: term()}].
prepare_capability_ans([]) ->
    [];
prepare_capability_ans([<<"objectType">> | Tail]) ->
    [{<<"objectType">>, <<"application/cdmi-capability">>} | prepare_capability_ans(Tail)];
prepare_capability_ans([<<"objectID">> | Tail]) ->
    [{<<"objectID">>, ?root_capability_id} | prepare_capability_ans(Tail)];
prepare_capability_ans([<<"objectName">> | Tail]) ->
    [{<<"objectName">>, ?root_capability_path} | prepare_capability_ans(Tail)];
prepare_capability_ans([<<"parentURI">> | Tail]) ->
    prepare_capability_ans(Tail);
prepare_capability_ans([<<"parentID">> | Tail]) ->
    prepare_capability_ans(Tail);
prepare_capability_ans([<<"capabilities">> | Tail]) ->
    [{<<"capabilities">>, ?root_capability_list} | prepare_capability_ans(Tail)];
prepare_capability_ans([<<"childrenrange">> | Tail]) ->
    [{<<"childrenrange">>, <<"0-1">>} | prepare_capability_ans(Tail)]; %todo hardcoded childrens, when adding childrenranges or new capabilities, this has to be changed
prepare_capability_ans([<<"children">> | Tail]) ->
    [{<<"children">>, [
        filepath_utils:ensure_ends_with_slash(filename:basename(?container_capability_path)),
        filepath_utils:ensure_ends_with_slash(filename:basename(?dataobject_capability_path))
    ]} | prepare_capability_ans(Tail)].
