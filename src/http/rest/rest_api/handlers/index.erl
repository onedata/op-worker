%%%--------------------------------------------------------------------
%%% @author Tomasz Lichon
%%% @copyright (C) 2016 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%--------------------------------------------------------------------
%%% @doc
%%% Handler for getting and modifying indexes.
%%% @end
%%%--------------------------------------------------------------------
-module(index).
-author("Tomasz Lichon").

-include("http/http_common.hrl").
-include("http/rest/rest_api/rest_errors.hrl").

%% API
-export([rest_init/2, terminate/3, allowed_methods/2, is_authorized/2,
    content_types_provided/2, content_types_accepted/2]).

%% resource functions
-export([get_index/2, modify_index/2]).

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
    {[<<"GET">>, <<"PUT">>], Req, State}.

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
        {<<"text/javascript">>, get_index}
    ], Req, State}.

%%--------------------------------------------------------------------
%% @doc @equiv pre_handler:content_types_accepted/2
%%--------------------------------------------------------------------
-spec content_types_accepted(req(), #{}) ->
    {[{binary(), atom()}], req(), #{}}.
content_types_accepted(Req, State) ->
    {[
        {<<"text/javascript">>, modify_index}
    ], Req, State}.

%%%===================================================================
%%% Content type handler functions
%%%===================================================================

%%--------------------------------------------------------------------
%% '/api/v3/oneprovider/index/{iid}'
%% @doc This method returns a specific index source code.
%%
%% The indexes are defined as JavaScript functions which are executed
%% on the database backend.
%%
%% ***Example cURL requests***
%%
%% **Get list of indexes for space**
%% &#x60;&#x60;&#x60;bash
%% curl --tlsv1.2 -H \&quot;X-Auth-Token: $TOKEN\&quot; -X GET \\
%% https://$HOST:8443/api/v1/oneprovider/index/f209c965-e212-4149-af72-860faea4187a
%%
%%
%% function(x) {
%% ...
%% }
%% &#x60;&#x60;&#x60;
%%
%% HTTP method: GET
%%
%% @param iid Id of the index to return.
%%--------------------------------------------------------------------
-spec get_index(req(), #{}) -> {term(), req(), #{}}.
get_index(Req, State) ->
    {State1, Req1} = validator:parse_id(Req, State),

    #{auth := Auth, id := Id} = State1,

    {ok, UserId} = session:get_user_id(Auth),
    {ok, Index} = indexes:get_index(UserId, Id),

    {maps:get(function, Index), Req1, State1}.

%%--------------------------------------------------------------------
%% '/api/v3/oneprovider/index/{iid}'
%% @doc This method replaces an existing index code with request body content.
%%
%% The indexes are defined as JavaScript functions which are executed
%% on the database backend.
%%
%% ***Example cURL requests***
%%
%% **Get list of indexes for space**
%% &#x60;&#x60;&#x60;bash
%% curl --tlsv1.2 -H \&quot;X-Auth-Token: $TOKEN\&quot; -X PUT \\
%% -H \&quot;Content-type: text/javascript\&quot; \\
%% -d \&quot;@./my_improved_index1.js\&quot; \\
%% https://$HOST:8443/api/v1/oneprovider/index/f209c965-e212-4149-af72-860faea4187a
%% &#x60;&#x60;&#x60;
%%
%% HTTP method: PUT
%%
%% @param iid Id of the index to update.
%%--------------------------------------------------------------------
-spec modify_index(req(), #{}) -> term().
modify_index(Req, State) ->
    {State1, Req1} = validator:parse_id(Req, State),
    {State2, Req2} = validator:parse_function(Req1, State1),

    #{auth := Auth, id := Id, function := Function} = State2,

    {ok, UserId} = session:get_user_id(Auth),
    {ok, Id} = indexes:change_index_function(UserId, Id, Function),

    {true, Req2, State2}.



