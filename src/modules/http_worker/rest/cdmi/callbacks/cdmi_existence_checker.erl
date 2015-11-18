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

-include_lib("ctool/include/posix/file_attr.hrl").
-include_lib("ctool/include/posix/errors.hrl").
-include("modules/http_worker/rest/http_status.hrl").

%% API
-export([container_resource_exists/2, object_resource_exists/2]).

%%%===================================================================
%%% API
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc @equiv pre_handler:resource_exists/2
%%--------------------------------------------------------------------
-spec container_resource_exists(cowboy_req:req(), #{}) -> {boolean(), cowboy_req:req(), #{}}.
container_resource_exists(Req, State) ->
    resource_exists(Req, State, container).


%%--------------------------------------------------------------------
%% @doc @equiv pre_handler:resource_exists/2
%%--------------------------------------------------------------------
-spec object_resource_exists(cowboy_req:req(), #{}) -> {boolean(), cowboy_req:req(), #{}}.
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
-spec redirect_to_object(cowboy_req:req(), #{}) -> {halt, cowboy_req:req(), #{}}.
redirect_to_object(Req, State) ->
    Location = <<"location">>, %todo prepare
    Req2 = cowboy_req:set_resp_header(<<"Location">>, Location, Req),
    {ok, Req3} = cowboy_req:reply(?MOVED_PERMANENTLY, [], [], Req2),
    {halt, Req3, State}.

%%--------------------------------------------------------------------
%% @doc
%% Redirect this request to the same url but with trailing '/'.
%% @end
%%--------------------------------------------------------------------
-spec redirect_to_container(cowboy_req:req(), #{}) -> {halt, cowboy_req:req(), #{}}.
redirect_to_container(Req, State) ->
    Location = <<"location">>, %todo prepare
    Req2 = cowboy_req:set_resp_header(<<"Location">>, Location, Req),
    {ok, Req3} = cowboy_req:reply(?MOVED_PERMANENTLY, [], [], Req2),
    {halt, Req3, State}.

%%--------------------------------------------------------------------
%% @doc @equiv pre_handler:resource_exists/2
%%--------------------------------------------------------------------
resource_exists(Req, State = #{path := Path, identity := Identity}, Type) ->
    case logical_file_manager:stat(Identity, {path, Path}) of
        {ok, #file_attr{type = ?DIRECTORY_TYPE}} when Type == container ->
            {true, Req, State};
        {ok, #file_attr{type = ?REGULAR_FILE_TYPE}} when Type == object ->
            {true, Req, State};
        {ok, #file_attr{type = ?DIRECTORY_TYPE}} when Type == object ->
            redirect_to_object(Req, State);
        {ok, #file_attr{type = ?REGULAR_FILE_TYPE}} when Type == container ->
            redirect_to_container(Req, State);
        {ok, #file_attr{type = ?LINK_TYPE}} ->
            {false, Req, State};
        {error, ?ENOENT} ->
            {false, Req, State}
    end.