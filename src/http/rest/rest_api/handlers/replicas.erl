%%%--------------------------------------------------------------------
%%% @author Tomasz Lichon
%%% @copyright (C) 2016 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%--------------------------------------------------------------------
%%% @doc
%%% Handler allowing for managing file replicas
%%% @end
%%%--------------------------------------------------------------------
-module(replicas).
-author("Tomasz Lichon").

-include("global_definitions.hrl").
-include("http/http_common.hrl").
-include("http/rest/http_status.hrl").
-include("modules/datastore/datastore_specific_models_def.hrl").
-include_lib("ctool/include/logging.hrl").

%% API
-export([rest_init/2, terminate/3, allowed_methods/2, is_authorized/2,
    content_types_accepted/2, content_types_provided/2]).

%% resource functions
-export([replicate_file/2, get_file_replicas/2]).

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
%% @doc @equiv pre_handler:content_types_accepted/2
%%--------------------------------------------------------------------
-spec content_types_accepted(req(), maps:map()) ->
    {[{atom() | binary(), atom()}], req(), maps:map()}.
content_types_accepted(Req, State) ->
    {[
        {'*', replicate_file}
    ], Req, State}.

%%--------------------------------------------------------------------
%% @doc @equiv pre_handler:content_types_provided/2
%%--------------------------------------------------------------------
-spec content_types_provided(req(), maps:map()) -> {[{binary(), atom()}], req(), maps:map()}.
content_types_provided(Req, State) ->
    {[
        {<<"application/json">>, get_file_replicas}
    ], Req, State}.


%%%===================================================================
%%% Content type handler functions
%%%===================================================================

%%--------------------------------------------------------------------
%% '/api/v3/oneprovider/replicas/{path}'
%% @doc Replicates a file to a specified provider. This operation is asynchronous
%% as it can  take a long time depending on the size of the data to move.
%% If the &#x60;path&#x60; parameter specifies a folder, entire folder is
%% replicated to  requested provider.
%%
%% HTTP method: POST
%%
%% @param path File path (e.g. &#39;/My Private Space/testfiles/file1.txt&#39;)
%% @param provider_id The ID of the provider to which the file should be replicated.
%%    By default the file will be replicated to the provided handling this REST call.\n
%% @param callback This parameter allows the user to specify a REST callback URL,
%%    which will be called when the transfer is complete\n
%%--------------------------------------------------------------------
-spec replicate_file(req(), maps:map()) -> {term(), req(), maps:map()}.
replicate_file(Req, State = #{resource_type := id}) ->
    {State2, Req2} = validator:parse_id(Req, State),
    {State3, Req3} = validator:parse_provider_id(Req2, State2),
    {State4, Req4} = validator:parse_callback(Req3, State3),

    replicate_file_internal(Req4, State4);
replicate_file(Req, State) ->
    {State2, Req2} = validator:parse_path(Req, State),
    {State3, Req3} = validator:parse_provider_id(Req2, State2),
    {State4, Req4} = validator:parse_callback(Req3, State3),

    replicate_file_internal(Req4, State4).

%%--------------------------------------------------------------------
%% '/api/v3/oneprovider/replicas/{path}'
%% @doc Returns file distribution information about a specific file replicated at this provider.\n
%%
%% HTTP method: GET
%%
%% @param path File path (e.g. &#39;/My Private Space/testfiles/file1.txt&#39;)
%%--------------------------------------------------------------------
-spec get_file_replicas(req(), maps:map()) -> {term(), req(), maps:map()}.
get_file_replicas(Req, State = #{resource_type := id}) ->
    {State2, Req2} = validator:parse_id(Req, State),

    get_file_replicas_internal(Req2, State2);
get_file_replicas(Req, State) ->
    {State2, Req2} = validator:parse_path(Req, State),

    get_file_replicas_internal(Req2, State2).

%%%===================================================================
%%% Internal functions
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc internal version of replicate_file/2
%%--------------------------------------------------------------------
-spec replicate_file_internal(req(), maps:map()) -> {term(), req(), maps:map()}.
replicate_file_internal(Req, #{auth := Auth, provider_id := ProviderId, callback := Callback} = State) ->
    File = get_file(State),

    {ok, _} = onedata_file_api:stat(Auth, File),
    {ok, TransferId} = transfer:start(Auth, File, ProviderId, Callback),

    Response = json_utils:encode([{<<"transferId">>, TransferId}]),
    {ok, Req2} = cowboy_req:reply(?HTTP_OK, [], Response, Req),
    {halt, Req2, State}.

%%--------------------------------------------------------------------
%% @doc internal version of get_file_replicas/2
%%--------------------------------------------------------------------
-spec get_file_replicas_internal(req(), maps:map()) -> {term(), req(), maps:map()}.
get_file_replicas_internal(Req, #{auth := Auth} = State) ->
    File = get_file(State),
    {ok, Distribution} = onedata_file_api:get_file_distribution(Auth, File),
    Response = json_utils:encode(Distribution),
    {Response, Req, State}.

%%--------------------------------------------------------------------
%% @doc
%% Get file entry from state
%% @end
%%--------------------------------------------------------------------
-spec get_file(maps:map()) -> {guid, binary()} | {path, binary()}.
get_file(#{id := Id}) ->
    {guid, Id};
get_file(#{path := Path}) ->
    {path, Path}.
