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
-module(validator).
-author("Tomasz Lichon").

-include("http/http_common.hrl").
-include("http/rest/rest_api/rest_errors.hrl").

-define(ALLOWED_ATTRIBUTES, [<<"mode">>]).
-define(DEFAULT_ATTRIBUTE, <<"mode">>).

-define(DEFAULT_TIMEOUT, infinity).

-define(DEFAULT_LAST_SEQ, now).

%% API
-export([malformed_metrics_request/2, malformed_request/2, parse_path/2,
    parse_id/2, parse_attribute/2, parse_attribute_body/2, parse_provider_id/2,
    parse_callback/2, parse_space_id/2, parse_timeout/2, parse_last_seq/2]).

%%%===================================================================
%%% API
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc Extract the REST api version and put it in State.
%%--------------------------------------------------------------------
-spec malformed_metrics_request(req(), #{}) -> {false, req(), #{}}.
malformed_metrics_request(Req, State) ->
    {State2, Req2} = parse_id(Req, State),
    {false, Req2, State2}.

%%--------------------------------------------------------------------
%% @doc Extract the REST api version and path and put it in State.
%%--------------------------------------------------------------------
-spec malformed_request(req(), #{}) -> {false, req(), #{}}.
malformed_request(Req, State) ->
    {State2, Req2} = parse_path(Req, State),
    {false, Req2, State2}.

%%--------------------------------------------------------------------
%% @doc
%% Retrieves request's file path and adds it to state.
%% @end
%%--------------------------------------------------------------------
-spec parse_path(cowboy_req:req(), #{}) ->
    {#{path => onedata_file_api:file_path()}, cowboy_req:req()}.
parse_path(Req, State) ->
    {Path, NewReq} = cowboy_req:path_info(Req),
    case Path of
        undefined ->
            throw(?ERROR_NOT_FOUND);
        _ ->
            {State#{path => filename:join([<<"/">> | Path])}, NewReq}
    end.

%%--------------------------------------------------------------------
%% @doc
%% Retrieves request's id and adds it to State.
%% @end
%%--------------------------------------------------------------------
-spec parse_id(cowboy_req:req(), #{}) ->
    {#{id => binary()}, cowboy_req:req()}.
parse_id(Req, State) ->
    {Id, NewReq} = cowboy_req:binding(id, Req),
    {State#{id => Id}, NewReq}.

%%--------------------------------------------------------------------
%% @doc
%% Retrieves request's attribute and adds it to State.
%% @end
%%--------------------------------------------------------------------
-spec parse_attribute(cowboy_req:req(), #{}) ->
    {#{attribute => binary()}, cowboy_req:req()}.
parse_attribute(Req, State) ->
    {Attribute, NewReq} = cowboy_req:qs_val(<<"attribute">>, Req, ?DEFAULT_ATTRIBUTE),
    case lists:member(Attribute, ?ALLOWED_ATTRIBUTES) of
        true ->
            {State#{attribute => Attribute}, NewReq};
        false ->
            throw(?ERROR_INVALID_ATTRIBUTE)
    end.


%%--------------------------------------------------------------------
%% @doc
%% Retrieves request's attribute from body and adds it to State.
%% @end
%%--------------------------------------------------------------------
-spec parse_attribute_body(cowboy_req:req(), #{}) ->
    {#{attribute_body => {binary(), binary()}}, cowboy_req:req()}.
parse_attribute_body(Req, State) ->
    {ok, Body, Req2} = cowboy_req:body(Req),

    Json = json_utils:decode(Body),
    case {
        proplists:get_value(<<"name">>, Json),
        proplists:get_value(<<"value">>, Json)
    } of
        {<<"mode">>, Value} ->
            try binary_to_integer(Value, 8) of
                Mode ->
                    {State#{attribute_body => {<<"mode">>, Mode}}, Req2}
            catch
               _:_ ->
                   throw(?ERROR_INVALID_MODE)
            end;
        {undefined, _} ->
            throw(?ERROR_INVALID_ATTRIBUTE_BODY);
        {_, undefined} ->
            throw(?ERROR_INVALID_ATTRIBUTE_BODY);
        {_Attr, _Value} ->
            throw(?ERROR_INVALID_ATTRIBUTE)
    end.

%%--------------------------------------------------------------------
%% @doc
%% Retrieves request's provider_id parameter and adds it to State.
%% @end
%%--------------------------------------------------------------------
-spec parse_provider_id(cowboy_req:req(), #{}) ->
    {#{provider_id => binary()}, cowboy_req:req()}.
parse_provider_id(Req, State) ->
    {ProviderId, NewReq} = cowboy_req:qs_val(<<"provider_id">>, Req, oneprovider:get_provider_id()),
    {State#{provider_id => ProviderId}, NewReq}.

%%--------------------------------------------------------------------
%% @doc
%% Retrieves request's callback body and adds it to State.
%% @end
%%--------------------------------------------------------------------
-spec parse_callback(cowboy_req:req(), #{}) ->
    {#{callback => binary()}, cowboy_req:req()}.
parse_callback(Req, State) ->
    {ok, Body, NewReq} = cowboy_req:body(Req),

    Json = json_utils:decode(Body),
    Callback = proplists:get_value(<<"url">>, Json),
    {State#{callback => Callback}, NewReq}.

%%--------------------------------------------------------------------
%% @doc
%% Retrieves request's space id and adds it to State.
%% @end
%%--------------------------------------------------------------------
-spec parse_space_id(cowboy_req:req(), #{}) ->
    {#{id => binary()}, cowboy_req:req()}.
parse_space_id(Req, State = #{auth := Auth}) ->
    {Id, NewReq} = cowboy_req:binding(sid, Req),
    {ok, UserId} = session:get_user_id(Auth),
    case space_info:get(Id, UserId) of
        {ok, _} ->
            {State#{space_id => Id}, NewReq};
        {error, {not_found, space_info}} ->
            throw(?ERROR_SPACE_NOT_FOUND)
    end.

%%--------------------------------------------------------------------
%% @doc
%% Retrieves request's timeout and adds it to State.
%% @end
%%--------------------------------------------------------------------
-spec parse_timeout(cowboy_req:req(), #{}) ->
    {#{id => binary()}, cowboy_req:req()}.
parse_timeout(Req, State) ->
    {RawTimeout, NewReq} = cowboy_req:qs_val(<<"timeout">>, Req, ?DEFAULT_TIMEOUT),
    case RawTimeout of
        infinity ->
            {State#{timeout => infinity}, NewReq};
        Number ->
            try binary_to_integer(Number) of
                Timeout ->
                    {State#{timeout => Timeout}, NewReq}
            catch
                _:_ ->
                    throw(?ERROR_INVALID_TIMEOUT)
            end
    end.

%%--------------------------------------------------------------------
%% @doc
%% Retrieves request's last sequence number and adds it to State.
%% @end
%%--------------------------------------------------------------------
-spec parse_last_seq(cowboy_req:req(), #{}) ->
    {#{id => binary()}, cowboy_req:req()}.
parse_last_seq(Req, State) ->
    {RawLastSeq, NewReq} = cowboy_req:qs_val(<<"last_seq">>, Req, ?DEFAULT_LAST_SEQ),
    case RawLastSeq of
        now ->
            {State#{last_seq => dbsync_worker:state_get(global_resume_seq)}, NewReq};
        Number ->
            try binary_to_integer(Number) of
                LastSeq ->
                    {State#{last_seq => LastSeq}, NewReq}
            catch
                _:_ ->
                    throw(?ERROR_INVALID_LAST_SEQ)
            end
    end.

%%%===================================================================
%%% Internal functions
%%%===================================================================
