%%%--------------------------------------------------------------------
%%% @author Tomasz Lichon
%%% @copyright (C) 2015 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%--------------------------------------------------------------------
%%% @doc
%%% Common cowboy callback's implementation for cdmi handlers.
%%% @end
%%%--------------------------------------------------------------------
-module(cdmi_existence_checker).
-author("Tomasz Lichon").

-include("global_definitions.hrl").
-include_lib("ctool/include/posix/file_attr.hrl").
-include_lib("ctool/include/posix/errors.hrl").
-include("http/rest/http_status.hrl").

%% API
-export([container_resource_exists/2, object_resource_exists/2]).

%%%===================================================================
%%% API
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc @equiv pre_handler:resource_exists/2
%%--------------------------------------------------------------------
-spec container_resource_exists(cowboy_req:req(), maps:map()) -> {boolean(), cowboy_req:req(), maps:map()}.
container_resource_exists(Req, State) ->
    resource_exists(Req, State, container).


%%--------------------------------------------------------------------
%% @doc @equiv pre_handler:resource_exists/2
%%--------------------------------------------------------------------
-spec object_resource_exists(cowboy_req:req(), maps:map()) -> {boolean(), cowboy_req:req(), maps:map()}.
object_resource_exists(Req, State) ->
    resource_exists(Req, State, object).


%%%===================================================================
%%% Internal functions
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Redirect this request to the same url but without trailing '/'.
%% @end
%%--------------------------------------------------------------------
-spec redirect_to_object(cowboy_req:req(), maps:map()) -> {halt, cowboy_req:req(), maps:map()}.
redirect_to_object(Req, #{path := Path} = State) ->
    redirect_to(Req, State, binary_part(Path, {0, byte_size(Path) - 1})).

%%--------------------------------------------------------------------
%% @doc
%% Redirect this request to the same url but with trailing '/'.
%% @end
%%--------------------------------------------------------------------
-spec redirect_to_container(cowboy_req:req(), maps:map()) -> {halt, cowboy_req:req(), maps:map()}.
redirect_to_container(Req, #{path := Path} = State) ->
    redirect_to(Req, State, <<Path/binary, "/">>).

%%--------------------------------------------------------------------
%% @doc
%% Redirect a request to the given path.
%% @end
%%--------------------------------------------------------------------
-spec redirect_to(cowboy_req:req(), maps:map(), binary()) -> {halt, cowboy_req:req(), maps:map()}.
redirect_to(Req, State, Path) ->
    {Hostname, _} = cowboy_req:header(<<"host">>, Req),

    {QS, _} = cowboy_req:qs(Req),
    Location = case QS of
                   <<"">> -> <<"https://", Hostname/binary, "/cdmi", Path/binary>>;
                   _ -> <<"https://", Hostname/binary, "/cdmi", Path/binary, "?", QS/binary>>
               end,
    {ok, Req2} = cowboy_req:reply(?MOVED_PERMANENTLY, [{<<"Location">>, Location}], Req),
    {halt, Req2, State}.


%%--------------------------------------------------------------------
%% @doc @equiv pre_handler:resource_exists/2
%%--------------------------------------------------------------------
resource_exists(Req, State = #{path := Path, auth := Auth}, Type) ->
    case logical_file_manager:stat(Auth, {path, Path}) of
        {ok, Attr = #file_attr{type = ?DIRECTORY_TYPE, guid = Guid}} when Type == container ->
            {true, Req, State#{attributes => Attr, guid => Guid}};
        {ok, Attr = #file_attr{type = ?REGULAR_FILE_TYPE, guid = Guid}} when Type == object ->
            {true, Req, State#{attributes => Attr, guid => Guid}};
        {ok, #file_attr{type = ?DIRECTORY_TYPE}} when Type == object ->
            redirect_to_container(Req, State);
        {ok, #file_attr{type = ?REGULAR_FILE_TYPE}} when Type == container ->
            redirect_to_object(Req, State);
        {ok, #file_attr{type = ?SYMLINK_TYPE}} ->
            {false, Req, State};
        {error, ?ENOENT} ->
            {false, Req, State}
    end.
