%% ===================================================================
%% @author Lukasz Opiola
%% @copyright (C): 2013 ACK CYFRONET AGH
%% This software is released under the MIT license 
%% cited in 'LICENSE.txt'.
%% @end
%% ===================================================================
%% @doc: This module handles Nagios monitoring requests.
%% @end
%% ===================================================================
-module(nagios_handler).

-export([init/3, handle/2, terminate/3]).


%% init/3
%% ====================================================================
%% @doc Cowboy handler callback, no state is required
-spec init(any(), term(), any()) -> {ok, term(), []}.
%% ====================================================================
init(_Type, Req, _Opts) ->
	{ok, Req, []}.


%% handle/2
%% ====================================================================
%% @doc Handles a request producing an XML response
%% @end
-spec handle(term(), term()) -> {ok, term(), term()}.
%% ====================================================================
handle(Req, State) ->	
	{{YY, MM, DD}, {Hour, Min, Sec}} = calendar:now_to_local_time(now()),
	DateString = io_lib:format("~4..0w/~2..0w/~2..0w ~2..0w:~2..0w:~2..0w",
	              [YY, MM, DD, Hour, Min, Sec]),

	DateBin = list_to_binary(DateString),
	Reply = <<"<healthdata date=\"", DateBin/binary, "\" status=\"ok\" message=\"\">
    <veil_cluster status=\"ok\" />
</healthdata>">>,

    {ok, Req2} = cowboy_req:reply(200, [{<<"content-type">>, <<"application/xml">>}], Reply, Req),

    {ok, Req2, State}.


%% terminate/3
%% ====================================================================
%% @doc Cowboy handler callback, no cleanup needed
-spec terminate(term(), term(), term()) -> ok.
%% ====================================================================
terminate(_Reason, _Req, _State) ->
	ok.
