%%%--------------------------------------------------------------------
%%% @author Lukasz Opiola
%%% @copyright (C) 2019 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%--------------------------------------------------------------------
%%% @doc
%%% Handler for creating shares by directory id.
%%% @end
%%%--------------------------------------------------------------------
-module(shares_by_id).
-author("Lukasz Opiola").

-include("global_definitions.hrl").
-include("http/http_common.hrl").
-include("modules/datastore/datastore_models.hrl").
-include("http/rest/http_status.hrl").
-include("http/rest/rest_api/rest_errors.hrl").
-include_lib("ctool/include/logging.hrl").
-include_lib("ctool/include/posix/errors.hrl").

%% API
-export([terminate/3, allowed_methods/2, is_authorized/2,
    content_types_accepted/2]).

%% resource functions
-export([create_share/2]).

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
-spec allowed_methods(req(), maps:map() | {error, term()}) -> {[binary()], req(), maps:map()}.
allowed_methods(Req, State) ->
    {[<<"POST">>], Req, State}.

%%--------------------------------------------------------------------
%% @doc @equiv pre_handler:is_authorized/2
%%--------------------------------------------------------------------
-spec is_authorized(req(), maps:map()) -> {true | {false, binary()} | stop, req(), maps:map()}.
is_authorized(Req, State) ->
    onedata_auth_api:is_authorized(Req, State).

%%--------------------------------------------------------------------
%% @doc @equiv pre_handler:content_types_accepted/2
%%--------------------------------------------------------------------
-spec content_types_accepted(req(), maps:map()) ->
    {[{atom() | binary(), atom()}], req(), maps:map()}.
content_types_accepted(Req, State) ->
    {[
        {<<"application/json">>, create_share}
    ], Req, State}.

%%%===================================================================
%%% Content type handler functions
%%%===================================================================

%%--------------------------------------------------------------------
%% '/api/v3/oneprovider/shares-id/{id}'
%% @doc Creates a new share of a directory specified by its id.\n
%%
%% HTTP method: POST
%%
%% @param id The fileId of the directory to be shared.\n
%% @param name The human readable name of the share.\n
%%--------------------------------------------------------------------
-spec create_share(req(), maps:map()) -> {term(), req(), maps:map()}.
create_share(Req, State) ->
    {State2, Req2} = validator:parse_objectid(Req, State),
    {State3, Req3} = validator:parse_name(Req2, State2),
    #{auth := SessionId, id := RootFileId, name := Name} = State3,

    Name == undefined andalso throw(?ERROR_INVALID_NAME),

    case logical_file_manager:create_share(SessionId, {guid, RootFileId}, Name) of
        {error, Error} ->
            error({error, Error});
        {ok, {ShareId, _}} ->
            Response = json_utils:encode(#{<<"shareId">> => ShareId}),
            Req4 = cowboy_req:reply(?HTTP_200_OK, #{}, Response, Req3),
            {stop, Req4, State3}
    end.


