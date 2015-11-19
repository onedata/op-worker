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
-export([resource_exists/2]).

%%%===================================================================
%%% API
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc @equiv pre_handler:resource_exists/2
%%--------------------------------------------------------------------
-spec resource_exists(cowboy_req:req(), #{}) -> {boolean(), cowboy_req:req(), #{}}.
resource_exists(Req, State = #{path := Path, identity := Identity}) ->
    case logical_file_manager:stat(Identity, {path, Path}) of
        {ok, Attr = #file_attr{type = ?DIRECTORY_TYPE}} ->
            {true, Req, State#{attributes := Attr}};
        {ok, #file_attr{}} ->
            redirect_to_object(Req, State);
        {error, ?ENOENT} ->
            {false, Req, State#{attributes := undefined}}
    end.


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
