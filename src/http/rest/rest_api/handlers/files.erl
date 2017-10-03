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
-include("modules/datastore/datastore_models.hrl").
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
    {[<<"GET">>], Req, State}.

%%--------------------------------------------------------------------
%% @equiv pre_handler:is_authorized/2
%% @end
%%--------------------------------------------------------------------
-spec is_authorized(req(), maps:map()) -> {true | {false, binary()} | halt, req(), maps:map()}.
is_authorized(Req, State) ->
    onedata_auth_api:is_authorized(Req, State).

%%--------------------------------------------------------------------
%% @equiv pre_handler:content_types_provided/2
%% @end
%%--------------------------------------------------------------------
-spec content_types_provided(req(), maps:map()) -> {[{binary(), atom()}], req(), maps:map()}.
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
%% @end
%%--------------------------------------------------------------------
-spec list_files(req(), maps:map()) -> {term(), req(), maps:map()}.
list_files(Req, State) ->
    {State2, Req2} = validator:parse_path(Req, State),
    {State3, Req3} = validator:parse_offset(Req2, State2),
    {State4, Req4} = validator:parse_dir_limit(Req3, State3),

    #{auth := Auth, path := Path, offset := Offset, limit := Limit} = State4,

    Response =
        case onedata_file_api:stat(Auth, {path, Path}) of
            {ok, #file_attr{type = ?DIRECTORY_TYPE, guid = Guid}} ->
                case onedata_file_api:get_children_count(Auth, {guid, Guid}) of
                    {ok, ChildNum} when Limit =:= undefined andalso ChildNum > ?MAX_ENTRIES ->
                        throw(?ERROR_TOO_MANY_ENTRIES);
                    {ok, _ChildNum} ->
                        DefinedLimit = utils:ensure_defined(Limit, undefined, ?MAX_ENTRIES),
                        {ok, Children} = onedata_file_api:ls(Auth, {path, Path}, Offset, DefinedLimit),
                        json_utils:encode_map(
                            lists:map(fun({ChildGuid, ChildPath}) ->
                                {ok, ObjectId} = cdmi_id:guid_to_objectid(ChildGuid),
                                #{<<"id">> => ObjectId, <<"path">> => filename:join(Path, ChildPath)}
                            end, Children))
                end;
            {ok, #file_attr{guid = Guid}} ->
                {ok, ObjectId} = cdmi_id:guid_to_objectid(Guid),
                json_utils:encode_map([#{<<"id">> => ObjectId, <<"path">> => Path}])
        end,
    {Response, Req4, State4}.
