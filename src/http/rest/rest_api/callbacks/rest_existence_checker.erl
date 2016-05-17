%%%--------------------------------------------------------------------
%%% @author Tomasz Lichon
%%% @copyright (C) 2016 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%--------------------------------------------------------------------
%%% @doc
%%% Cowboy callback for checking existence of file
%%% @end
%%%--------------------------------------------------------------------
-module(rest_existence_checker).
-author("Tomasz Lichon").

-include("http/http_common.hrl").
-include("global_definitions.hrl").
-include_lib("ctool/include/posix/file_attr.hrl").
-include_lib("ctool/include/posix/errors.hrl").
-include("http/rest/http_status.hrl").

%% API
-export([resource_exists/2]).

%%%===================================================================
%%% API
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc Checks if resource exists, adds its attributes to state.
%%--------------------------------------------------------------------
-spec resource_exists(req(), #{}) -> {boolean(), req(), #{}}.
resource_exists(Req, State = #{path := Path, auth := Auth}) ->
    case logical_file_manager:stat(Auth, {path, Path}) of
        {ok, Attr = #file_attr{}} ->
            {true, Req, State#{attributes => Attr}};
        {error, ?ENOENT} ->
            {false, Req, State}
    end.

%%%===================================================================
%%% Internal functions
%%%===================================================================