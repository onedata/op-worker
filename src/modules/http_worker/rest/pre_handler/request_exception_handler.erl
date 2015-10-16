%%%--------------------------------------------------------------------
%%% @author Tomasz Lichon
%%% @copyright (C) 2015 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%--------------------------------------------------------------------
%%% @doc
%%% Default exception handler for rest operations. Returns internal server
%%% error on each fail.
%%% @end
%%%--------------------------------------------------------------------
-module(request_exception_handler).
-author("Tomasz Lichon").

-include_lib("ctool/include/logging.hrl").

%% Function that translates handler exception to cowboy format
-type exception_handler() ::
fun((Req :: cowboy_req:req(), State :: term(), Type :: atom(), Error :: term()) -> term()).

-define(INTERNAL_SERVER_ERROR, 500).

%% API
-export([handle/4]).

%%%===================================================================
%%% API
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Exception handler for rest modules. I should return appropriate cowboy
%% status.
%% @end
%%--------------------------------------------------------------------
-spec handle(cowboy_req:req(), term, atom(), term()) -> no_return().
handle(Req, State, Type, Error) ->
    ?error_stacktrace("Unhandled exception in rest request ~p:~p", [Type, Error]),
    {ok, Req2} = cowboy_req:reply(?INTERNAL_SERVER_ERROR, [], [], Req),
    {halt, Req2, State}.

%%%===================================================================
%%% Internal functions
%%%===================================================================