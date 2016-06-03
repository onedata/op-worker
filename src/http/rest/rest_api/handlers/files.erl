%%%--------------------------------------------------------------------
%%% @author Tomasz Lichon
%%% @copyright (C) 2016 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%--------------------------------------------------------------------
%%% @doc
%%% Handler for listing files.
%%% @end
%%%--------------------------------------------------------------------
-module(files).
-author("Tomasz Lichon").

-include("global_definitions.hrl").
-include("http/http_common.hrl").
-include_lib("cluster_worker/include/modules/datastore/datastore.hrl").
-include("modules/datastore/datastore_specific_models_def.hrl").
-include_lib("ctool/include/logging.hrl").
-include("http/rest/http_status.hrl").
-include("http/rest/rest_api/rest_errors.hrl").

-define(MAX_ENTRIES, 1000).

%% API
-export([rest_init/2, terminate/3, allowed_methods/2, is_authorized/2,
    content_types_provided/2]).

%% resource functions
-export([list_files/2]).

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
    {[<<"GET">>], Req, State}.

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
        {<<"application/json">>, list_files}
    ], Req, State}.

%%%===================================================================
%%% Content type handler functions
%%%===================================================================

%%--------------------------------------------------------------------
%% '/api/v3/oneprovider/files/{path}'
%% @doc Returns the list of folders and files directly under specified path.
%% If the path points to a file, the result array will consist only of the
%% single item with the path to the file requested, confirming it exists.\n
%%
%% HTTP method: GET
%%
%% @param path Directory path (e.g. &#39;/My Private Space/testfiles&#39;)
%%--------------------------------------------------------------------
-spec list_files(req(), #{}) -> {term(), req(), #{}}.
list_files(Req, State) ->
    {State2, Req2} = validator:parse_path(Req, State),

    #{auth := Auth, path := Path} = State2,

    Response =
        case onedata_file_api:stat(Auth, {path, Path}) of
            {ok, #file_attr{type = ?DIRECTORY_TYPE, uuid = Guid}} ->
                case onedata_file_api:get_children_count(Auth, {guid, Guid}) of
                    {ok, ChildNum} when ChildNum > ?MAX_ENTRIES ->
                        throw(?ERROR_TOO_MANY_ENTRIES);
                    {ok, ChildNum} ->
                        {ok, Children} = onedata_file_api:ls(Auth, {path, Path}, 0, ?MAX_ENTRIES),
                        json_utils:encode(
                            lists:map(fun({Guid, ChildPath}) ->
                                [{<<"id">>, Guid}, {<<"path">>, filename:join(Path, ChildPath)}]
                            end, Children))
                end;
            {ok, #file_attr{uuid = Guid}} ->
                json_utils:encode([[{<<"id">>, Guid}, {<<"path">>, Path}]])
        end,
    {Response, Req2, State2}.