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
-export([malformed_request/2, malformed_metrics_request/2]).

%%%===================================================================
%%% API
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc Extract the REST api version and put it in State.
%%--------------------------------------------------------------------
-spec malformed_metrics_request(req(), #{}) -> {false, req(), #{}}.
malformed_metrics_request(Req, State) ->
    {State2, Req2} = add_version_to_state(Req, State),
    {State3, Req3} = add_id_to_state(Req2, State2),
    {false, Req3, State3}.

%%--------------------------------------------------------------------
%% @doc Extract the REST api version and path and put it in State.
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
%% Retrieves request's version and adds it to State.
%% @end
%%--------------------------------------------------------------------
-spec add_version_to_state(cowboy_req:req(), #{}) ->
    {#{cdmi_version => binary()}, cowboy_req:req()}.
add_version_to_state(Req, State) ->
    {Version, NewReq} = cowboy_req:binding(version, Req),
    {State#{version => Version}, NewReq}.

%%--------------------------------------------------------------------
%% @doc
%% Retrieves request's file path and adds it to state.
%% @end
%%--------------------------------------------------------------------
-spec add_path_to_state(cowboy_req:req(), #{}) ->
    {#{path => onedata_file_api:file_path()}, cowboy_req:req()}.
add_path_to_state(Req, State) ->
    {Path, NewReq} = cowboy_req:path_info(Req),
    {State#{path => filename:join([<<"/">> | Path])}, NewReq}.

%%--------------------------------------------------------------------
%% @doc
%% Retrieves request's id and adds it to State.
%% @end
%%--------------------------------------------------------------------
-spec add_id_to_state(cowboy_req:req(), #{}) ->
    {#{cdmi_version => binary()}, cowboy_req:req()}.
add_id_to_state(Req, State) ->
    {Version, NewReq} = cowboy_req:binding(id, Req),
    {State#{id => Version}, NewReq}.