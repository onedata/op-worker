%% ===================================================================
%% @author Tomasz Lichon
%% @copyright (C): 2014 ACK CYFRONET AGH
%% This software is released under the MIT license
%% cited in 'LICENSE.txt'.
%% @end
%% ===================================================================
%% @doc This is a cdmi handler module providing access to cdmi_capabilities
%% which are used to discover capabilities of objects (operation that can be perfored)
%% @end
%% ===================================================================
-module(cdmi_capabilities).

-include("veil_modules/control_panel/cdmi.hrl").
-include("veil_modules/control_panel/cdmi_capabilities.hrl").

%% API
-export([allowed_methods/2,malformed_request/2,content_types_provided/2,resource_exists/2]).
-export([get_cdmi_capability/2]).

%% allowed_methods/2
%% ====================================================================
%% @doc Returns binary list of methods that are allowed (i.e GET, PUT, DELETE).
-spec allowed_methods(req(), #state{}) -> {[binary()], req(), #state{}}.
%% ====================================================================
allowed_methods(Req, State) ->
    {[<<"GET">>], Req, State}.

%% malformed_request/2
%% ====================================================================
%% @doc
%% Checks if request contains all mandatory fields and their values are set properly
%% depending on requested operation
%% @end
-spec malformed_request(req(), #state{}) -> {boolean(), req(), #state{}} | no_return().
%% ====================================================================
malformed_request(Req, #state{cdmi_version = undefined } = State) ->
    ?error_stacktrace("No cdmi version secified in cdmi_capabilities request"),
    {true,Req,State};
malformed_request(Req,  State) ->
    {false,Req,State}.

%% resource_exists/2
%% ====================================================================
%% @doc Determines if resource, that can be obtained from state, exists.
-spec resource_exists(req(), #state{}) -> {boolean(), req(), #state{}}.
%% ====================================================================
resource_exists(Req, State = #state{filepath = Filepath}) ->
    case proplists:get_value(Filepath++"/",?CapabilityNameByPath) of
        undefined -> {false, Req, State};
        CapabilityName -> {true, Req, State#state{capability = CapabilityName}}
    end.

%% content_types_provided/2
%% ====================================================================
%% @doc
%% Returns content types that can be provided and what functions should be used to process the request.
%% Before adding new content type make sure that adequate routing function
%% exists in cdmi_handler
%% @end
-spec content_types_provided(req(), #state{}) -> {[{ContentType, Method}], req(), #state{}} when
    ContentType :: binary(),
    Method :: atom().
%% ====================================================================
content_types_provided(Req, State) ->
    {[
        {<<"application/cdmi-capability">>, get_cdmi_capability}
    ], Req, State}.

%% ====================================================================
%% User callbacks registered in content_types_provided/content_types_accepted and present
%% in main cdmi_handler. They can handle get/put requests depending on content type.
%% ====================================================================

%% get_cdmi_capability/2
%% ====================================================================
%% @doc Cowboy callback function
%% Handles GET requests for cdmi capability.
%% @end
-spec get_cdmi_capability(req(), #state{}) -> {term(), req(), #state{}}.
%% ====================================================================
get_cdmi_capability(Req, #state{opts = Opts} = State) ->
    Capability = prepare_capability_ans(case Opts of [] -> ?default_get_capability_opts; _ -> Opts end, State),
    Response = rest_utils:encode_to_json({struct, Capability}),
    {Response,Req,State}.

%% ====================================================================
%% Internal functions
%% ====================================================================

%% prepare_capability_ans/2
%% ====================================================================
%% @doc Prepares proplist formatted answer with field names from given list of binaries
%% (selected opts that someone wants to extract)
%% @end
-spec prepare_capability_ans([FieldName :: binary()], #state{}) -> [{FieldName :: binary(), Value :: term()}].
%% ====================================================================
prepare_capability_ans([], _State) ->
    [];
prepare_capability_ans([<<"objectType">> | Tail], State) ->
    [{<<"objectType">>, <<"application/cdmi-capability">>} | prepare_capability_ans(Tail, State)];

% root capabilities
prepare_capability_ans([<<"objectID">> | Tail], State = #state{capability = root}) ->
    [{<<"objectID">>, ?root_capability_id} | prepare_capability_ans(Tail, State)];
prepare_capability_ans([<<"objectName">> | Tail], State = #state{capability = root}) ->
    [{<<"objectName">>, list_to_binary(?root_capability_path)} | prepare_capability_ans(Tail, State)];
prepare_capability_ans([<<"parentURI">> | Tail], State = #state{capability = root}) ->
    prepare_capability_ans(Tail, State);
prepare_capability_ans([<<"parentID">> | Tail], State = #state{capability = root}) ->
    prepare_capability_ans(Tail, State);
prepare_capability_ans([<<"capabilities">> | Tail], State = #state{capability = root}) ->
    [{<<"capabilities">>, ?root_capability_list} | prepare_capability_ans(Tail, State)];
prepare_capability_ans([<<"childrenrange">> | Tail], State = #state{capability = root}) ->
    [{<<"childrenrange">>, <<"0-1">>} | prepare_capability_ans(Tail, State)]; %todo hardcoded childrens, when adding childrenranges or new capabilities, this has to be changed
prepare_capability_ans([<<"children">> | Tail], State = #state{capability = root}) ->
    [{<<"children">>, [list_to_binary(?container_capability_path), list_to_binary(?dataobject_capability_path)]} | prepare_capability_ans(Tail, State)];

% container capabilities
prepare_capability_ans([<<"objectID">> | Tail], State = #state{capability = container}) ->
    [{<<"objectID">>, ?container_capability_id} | prepare_capability_ans(Tail, State)];
prepare_capability_ans([<<"objectName">> | Tail], State = #state{capability = container}) ->
    [{<<"objectName">>, list_to_binary(?container_capability_path)} | prepare_capability_ans(Tail, State)];
prepare_capability_ans([<<"parentURI">> | Tail], State = #state{capability = container}) ->
    [{<<"parentURI">>, list_to_binary(?root_capability_path)} | prepare_capability_ans(Tail, State)];
prepare_capability_ans([<<"parentID">> | Tail], State = #state{capability = container}) ->
    [{<<"parentID">>, ?root_capability_id} | prepare_capability_ans(Tail, State)];
prepare_capability_ans([<<"capabilities">> | Tail], State = #state{capability = container}) ->
    [{<<"capabilities">>, ?container_capability_list} | prepare_capability_ans(Tail, State)];
prepare_capability_ans([<<"childrenrange">> | Tail], State = #state{capability = container}) ->
    prepare_capability_ans(Tail, State);
prepare_capability_ans([<<"children">> | Tail], State = #state{capability = container}) ->
    [{<<"children">>, []} | prepare_capability_ans(Tail, State)];

% dataobject capabilities
prepare_capability_ans([<<"objectID">> | Tail], State = #state{capability = dataobject}) ->
    [{<<"objectID">>, ?dataobject_capability_id} | prepare_capability_ans(Tail, State)];
prepare_capability_ans([<<"objectName">> | Tail], State = #state{capability = dataobject}) ->
    [{<<"objectName">>, list_to_binary(?dataobject_capability_path)} | prepare_capability_ans(Tail, State)];
prepare_capability_ans([<<"parentURI">> | Tail], State = #state{capability = dataobject}) ->
    [{<<"parentURI">>, list_to_binary(?root_capability_path)} | prepare_capability_ans(Tail, State)];
prepare_capability_ans([<<"parentID">> | Tail], State = #state{capability = dataobject}) ->
    [{<<"parentID">>, ?root_capability_id} | prepare_capability_ans(Tail, State)];
prepare_capability_ans([<<"capabilities">> | Tail], State = #state{capability = dataobject}) ->
    [{<<"capabilities">>, ?dataobject_capability_list} | prepare_capability_ans(Tail, State)];
prepare_capability_ans([<<"childrenrange">> | Tail], State = #state{capability = dataobject}) ->
    prepare_capability_ans(Tail, State);
prepare_capability_ans([<<"children">> | Tail], State = #state{capability = dataobject}) ->
    [{<<"children">>, []} | prepare_capability_ans(Tail, State)].
