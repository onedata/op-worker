%%%--------------------------------------------------------------------
%%% @author Tomasz Lichon
%%% @copyright (C) 2017 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%--------------------------------------------------------------------
%%% @doc
%%% Handler for enabling space cleanup.
%%% @end
%%%--------------------------------------------------------------------
-module(space_cleanup).
-author("Tomasz Lichon").

-include("global_definitions.hrl").
-include("http/http_common.hrl").
-include_lib("cluster_worker/include/modules/datastore/datastore.hrl").
-include_lib("ctool/include/logging.hrl").
-include("http/rest/http_status.hrl").
-include("http/rest/rest_api/rest_errors.hrl").

-define(MAX_ENTRIES, 1000).

%% API
-export([rest_init/2, terminate/3, allowed_methods/2, is_authorized/2,
    content_types_accepted/2, delete_resource/2]).

%% resource functions
-export([enable_cleanup/2, disable_cleanup/2]).

%%%===================================================================
%%% API
%%%===================================================================

%%--------------------------------------------------------------------
%% @equiv pre_handler:rest_init/2
%% @end
%%--------------------------------------------------------------------
-spec rest_init(req(), term()) -> {ok, req(), term()} | {shutdown, req()}.
rest_init(Req, State) ->
    {ok, Req, State}.

%%--------------------------------------------------------------------
%% @equiv pre_handler:terminate/3
%% @end
%%--------------------------------------------------------------------
-spec terminate(Reason :: term(), req(), maps:map()) -> ok.
terminate(_, _, _) ->
    ok.

%%--------------------------------------------------------------------
%% @equiv pre_handler:allowed_methods/2
%% @end
%%--------------------------------------------------------------------
-spec allowed_methods(req(), maps:map() | {error, term()}) -> {[binary()], req(), maps:map()}.
allowed_methods(Req, State) ->
    {[<<"POST">>, <<"DELETE">>], Req, State}.

%%--------------------------------------------------------------------
%% @equiv pre_handler:is_authorized/2
%% @end
%%--------------------------------------------------------------------
-spec is_authorized(req(), maps:map()) -> {true | {false, binary()} | halt, req(), maps:map()}.
is_authorized(Req, State) ->
    onedata_auth_api:is_authorized(Req, State).

%%--------------------------------------------------------------------
%% @equiv pre_handler:content_types_accepted/2
%% @end
%%--------------------------------------------------------------------
-spec content_types_accepted(req(), maps:map()) -> {[{atom(), atom()}], req(), maps:map()}.
content_types_accepted(Req, State) ->
    {[
        {'*', enable_cleanup}
    ], Req, State}.

%%--------------------------------------------------------------------
%% @equiv pre_handler:delete_resource/2
%% @end
%%--------------------------------------------------------------------
-spec delete_resource(req(), maps:map()) -> {term(), req(), maps:map()}.
delete_resource(Req, State) ->
    disable_cleanup(Req, State).

%%%===================================================================
%%% Content type handler functions
%%%===================================================================

%%--------------------------------------------------------------------
%% '/api/v3/oneprovider/space-cleanup/{sid}'
%% @doc
%% Enable file popularity and space cleanup
%% @end
%%--------------------------------------------------------------------
-spec enable_cleanup(req(), maps:map()) -> {term(), req(), maps:map()}.
enable_cleanup(Req, State) ->
    {State2, Req2} = validator:parse_space_id(Req, State),

    #{space_id := SpaceId} = State2,

    {ok, _} = space_cleanup_api:enable_cleanup(SpaceId),
    {true, Req2, State2}.

%%--------------------------------------------------------------------
%% '/api/v3/oneprovider/space-cleanup/{sid}'
%% @doc
%% Disable file popularity and space cleanup
%% @end
%%--------------------------------------------------------------------
-spec disable_cleanup(req(), maps:map()) -> {term(), req(), maps:map()}.
disable_cleanup(Req, State) ->
    {State2, Req2} = validator:parse_space_id(Req, State),

    #{space_id := SpaceId} = State2,

    {ok, _} = space_cleanup_api:disable_cleanup(SpaceId),
    {true, Req2, State2}.