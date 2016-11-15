%%%--------------------------------------------------------------------
%%% @author Tomasz Lichon
%%% @copyright (C) 2016 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%--------------------------------------------------------------------
%%% @doc
%%% Handler for listing indexes.
%%% @end
%%%--------------------------------------------------------------------
-module(index_collection).
-author("Tomasz Lichon").

-include("http/http_common.hrl").
-include("http/rest/rest_api/rest_errors.hrl").


%% API
-export([rest_init/2, terminate/3, allowed_methods/2, is_authorized/2,
    content_types_provided/2, content_types_accepted/2]).

%% resource functions
-export([list_indexes/2, create_index/2]).

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
    {[<<"GET">>, <<"POST">>], Req, State}.

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
        {<<"application/json">>, list_indexes}
    ], Req, State}.

%%--------------------------------------------------------------------
%% @doc @equiv pre_handler:content_types_accepted/2
%%--------------------------------------------------------------------
-spec content_types_accepted(req(), maps:map()) ->
    {[{binary(), atom()}], req(), maps:map()}.
content_types_accepted(Req, State) ->
    {[
        {<<"application/javascript">>, create_index}
    ], Req, State}.

%%%===================================================================
%%% Content type handler functions
%%%===================================================================

%%--------------------------------------------------------------------
%% '/api/v3/oneprovider/index'
%% @doc This method returns the list of user defined index functions.
%%
%% The result can be limited to specific space using query parameter &#x60;space_id&#x60;.
%%
%%
%% ***Example cURL requests***
%%
%% **Get list of indexes for space**
%% &#x60;&#x60;&#x60;bash
%% curl --tlsv1.2 -H \&quot;X-Auth-Token: $TOKEN\&quot; -X GET \\
%% https://$HOST:8443/api/v1/oneprovider/index?space_id&#x3D;2e462492-a4d7-46b9-8641-abfdf50f06af
%%
%% [
%% {
%% \&quot;spaceId\&quot;: \&quot;2e462492-a4d7-46b9-8641-abfdf50f06af\&quot;,
%% \&quot;name\&quot;: \&quot;My index\&quot;,
%% \&quot;indexId\&quot;: \&quot;fdecdf35-5e18-4a9b-a01a-1702acd4d274\&quot;
%% }
%% ]
%% &#x60;&#x60;&#x60;
%%
%% HTTP method: GET
%%
%% @param space_id Id of the space to query.
%%--------------------------------------------------------------------
-spec list_indexes(req(), maps:map()) -> {term(), req(), maps:map()}.
list_indexes(Req, State) ->
    {State1, Req1} = validator:parse_query_space_id(Req, State),

    #{auth := Auth, space_id := SpaceId} = State1,

    {ok, UserId} = session:get_user_id(Auth),
    {ok, Indexes} = indexes:get_all_indexes(UserId),
    IndexList = maps:values(maps:map(fun(K, V) -> V#{id => K} end, Indexes)),
    RawResponse =
        lists:filtermap(fun
            (#{id := Id, space_id := SID, name := undefined}) when SpaceId =:= undefined orelse SID =:= SpaceId ->
                {true, #{<<"spaceId">> => SID, <<"indexId">> => Id}};
            (#{id := Id, space_id := SID, name := Name}) when SpaceId =:= undefined orelse SID =:= SpaceId ->
                {true, #{<<"spaceId">> => SID, <<"name">> => Name, <<"indexId">> => Id}};
            (_) ->
                false
        end, IndexList),

    Response = json_utils:encode_map(RawResponse),
    {Response, Req1, State1}.

%%--------------------------------------------------------------------
%% '/api/v3/oneprovider/index'
%% @doc This method allows to create a new index for space.
%%
%% Indexes allow creating custom views on data which enable efficient searching through data.
%%
%% Currently indexes are created per space, i.e. the &#x60;space_id&#x60; query parameter is required.
%%
%% The operation returns the created index ID in the response &#x60;Location&#x60; header.
%%
%% ***Example cURL requests***
%%
%% **Set JSON metadata for file**
%% &#x60;&#x60;&#x60;bash
%% curl --tlsv1.2 -H \&quot;X-Auth-Token: $TOKEN\&quot; -X POST \\
%% -H \&quot;Content-type: application/json\&quot; \\
%% -d \&quot;@./my_index_1.js\&quot;
%% https://$HOST:8443/api/v1/oneprovider/index?space_id&#x3D;7f85c115-8631-4602-b7d5-47cd969280a2&amp;name&#x3D;MyIndex1
%% &#x60;&#x60;&#x60;
%%
%% HTTP method: POST
%%
%% @param space_id File or folder path or space id.
%% @param name The user friendly name of the index (can be used to assign names to &#39;smart folders&#39; in the GUI).
%% If not provider an auto generated name will be assigned.
%%
%%--------------------------------------------------------------------
-spec create_index(req(), maps:map()) -> term().
create_index(Req, State) ->
    {State1, Req1} = validator:parse_query_space_id(Req, State),
    {State2, Req2} = validator:parse_name(Req1, State1),
    {State3, Req3} = validator:parse_function(Req2, State2),
    {State4, Req4} = validator:parse_spatial(Req3, State3),

    #{auth := Auth, name := Name, space_id := SpaceId, function := Function, spatial := Spatial} = State4,
    {ok, UserId} = session:get_user_id(Auth),
    case SpaceId of
        undefined ->
            throw(?ERROR_SPACE_NOT_PROVIDED);
        _ ->
            ok
    end,
    space_membership:check_with_user(UserId, SpaceId),
    {ok, Id} = indexes:add_index(UserId, Name, Function, SpaceId, Spatial),

    {{true, <<"/api/v3/oneprovider/index/", Id/binary>>}, Req4, State4}.



