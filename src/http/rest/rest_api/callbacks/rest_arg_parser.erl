%%%--------------------------------------------------------------------
%%% @author Tomasz Lichon
%%% @copyright (C) 2016 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%--------------------------------------------------------------------
%%% @doc This module implements some functions for parsing
%%% and processing parameters of request.
%%% @end
%%%--------------------------------------------------------------------
-module(rest_arg_parser).
-author("Tomasz Lichon").

-include("http/http_common.hrl").

%% API
-export([malformed_request/2]).

%%%===================================================================
%%% API
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc Extract the CDMI version and options and put it in State.
%%--------------------------------------------------------------------
-spec malformed_request(req(), #{}) -> {false, req(), #{}}.
malformed_request(Req, State) ->
    {State2, Req2} = add_version_to_state(Req, State),
    {State3, Req3} = add_path_to_state(Req2, State2),
    {false, Req3, State3}.

%%%===================================================================
%%% Internal functions
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Parses request's version adds it to State.
%% @end
%%--------------------------------------------------------------------
-spec add_version_to_state(cowboy_req:req(), #{}) ->
    {#{cdmi_version => binary()}, cowboy_req:req()}.
add_version_to_state(Req, State) ->
    {Version, NewReq} = cowboy_req:binding(version, Req),
    {State#{version => Version}, NewReq}.

%%--------------------------------------------------------------------
%% @doc
%% Retrieves file path from req and adds it to state.
%% @end
%%--------------------------------------------------------------------
-spec add_path_to_state(cowboy_req:req(), #{}) ->
    {#{path => onedata_file_api:file_path()}, cowboy_req:req()}.
add_path_to_state(Req, State) ->
    {Path, NewReq} = cowboy_req:path_info(Req),
    {State#{path => filename:join([<<"/">> | Path])}, NewReq}.