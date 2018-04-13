%%%-------------------------------------------------------------------
%%% @author Lukasz Opiola
%%% @copyright (C) 2017 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% This handler serves as a public endpoint to check OP's connectivity to OZ.
%%% @end
%%%-------------------------------------------------------------------
-module(oz_connectivity_handler).
-author("Lukasz Opiola").

-behaviour(cowboy_handler).

-include_lib("ctool/include/logging.hrl").
-include_lib("ctool/include/api_errors.hrl").

%% API
-export([init/2]).


%%--------------------------------------------------------------------
%% @doc Cowboy handler callback.
%% Handles a request returning status of connection to Onezone.
%% @end
%%--------------------------------------------------------------------
-spec init(cowboy_req:req(), term()) -> {ok, cowboy_req:req(), term()}.
init(#{method := <<"GET">>} = Req, State) ->
    %% If connection is established, OP should be able to retrieve its own
    %% data. Any other result implies connection error.
    Status = case oneprovider:is_connected_to_oz() of
        true -> <<"ok">>;
        false -> <<"error">>
    end,
    NewReq = cowboy_req:reply(200,
        #{<<"content-type">> => <<"application/json">>},
        json_utils:encode(#{<<"status">> => Status}),
        Req
    ),
    {ok, NewReq, State};
init(Req, State) ->
    NewReq = cowboy_req:reply(405, #{<<"allow">> => <<"GET">>}, Req),
    {ok, NewReq, State}.
