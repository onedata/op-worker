%%%--------------------------------------------------------------------
%%% @author Bartosz Walkowicz
%%% @copyright (C) 2018 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%--------------------------------------------------------------------
%%% @doc
%%% Handler for creating, getting, modifying and deleting indexes.
%%% @end
%%%--------------------------------------------------------------------
-module(index_reduce).
-author("Bartosz Walkowicz").

-include("http/http_common.hrl").
-include("http/rest/rest_api/rest_errors.hrl").
-include("modules/fslogic/fslogic_common.hrl").

%% API
-export([
    terminate/3, allowed_methods/2, is_authorized/2, content_types_accepted/2,
    delete_resource/2
]).

%% resource functions
-export([add_or_modify_index_reduce_fun/2]).

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
    {[<<"PUT">>, <<"DELETE">>], Req, State}.

%%--------------------------------------------------------------------
%% @doc @equiv pre_handler:is_authorized/2
%%--------------------------------------------------------------------
-spec is_authorized(req(), maps:map()) ->
    {true | {false, binary()} | stop, req(), maps:map()}.
is_authorized(Req, State) ->
    onedata_auth_api:is_authorized(Req, State).

%%--------------------------------------------------------------------
%% @doc @equiv pre_handler:content_types_accepted/2
%%--------------------------------------------------------------------
-spec content_types_accepted(req(), maps:map()) ->
    {[{binary(), atom()}], req(), maps:map()}.
content_types_accepted(Req, State) ->
    {[
        {<<"application/javascript">>, add_or_modify_index_reduce_fun}
    ], Req, State}.

%%--------------------------------------------------------------------
%% '/api/v3/oneprovider/spaces/{sid}/indexes/{name}/reduce'
%% @doc This method removes index
%%
%% HTTP method: DELETE
%%
%% @param sid Id of the space within which index exist.
%% @param name Name of the index.
%%--------------------------------------------------------------------
-spec delete_resource(req(), maps:map()) -> {term(), req(), maps:map()}.
delete_resource(Req, State) ->
    {State2, Req2} = validator:parse_space_id(Req, State),
    {State3, Req3} = validator:parse_index_name(Req2, State2),

    #{space_id := SpaceId, index_name := IndexName} = State3,

    % TODO delete reduce fun
    {true, Req3, State3}.

%%%===================================================================
%%% Content type handler functions
%%%===================================================================

%%--------------------------------------------------------------------
%% '/api/v3/oneprovider/spaces/{sid}/indexes/{name}/reduce'
%% @doc This method creates or replaces an existing index reduce function
%% code with request body content.
%%
%% HTTP method: PUT
%%
%% @param sid Id of the space within which index exist.
%% @param name Name of the index.
%%--------------------------------------------------------------------
-spec add_or_modify_index_reduce_fun(req(), maps:map()) -> term().
add_or_modify_index_reduce_fun(Req, State) ->
    {State2, Req2} = validator:parse_space_id(Req, State),
    {State3, Req3} = validator:parse_index_name(Req2, State2),
    {State4, Req4} = validator:parse_function(Req3, State3),

    #{
        space_id := SpaceId,
        index_name := IndexName,
        function := ReduceFunction
    } = State4,

    % TODO add reduce fun to index
    {stop, cowboy_req:reply(?HTTP_OK, Req4), State4}.
