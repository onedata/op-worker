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
-include("modules/fslogic/fslogic_common.hrl").
-include_lib("ctool/include/logging.hrl").


-define(DEFAULT_TIMEOUT, <<"infinity">>).
-define(DEFAULT_LAST_SEQ, <<"now">>).

%% API
-export([
    parse_space_id/2,
    parse_timeout/2, parse_last_seq/2
]).

%% TODO VFS-2574 Make validation of result map
-type parse_result() :: maps:map().

%%%===================================================================
%%% API
%%%===================================================================


%%--------------------------------------------------------------------
%% @doc
%% Retrieves request's space id and adds it to State.
%% @end
%%--------------------------------------------------------------------
-spec parse_space_id(cowboy_req:req(), maps:map()) ->
    {parse_result(), cowboy_req:req()}.
parse_space_id(Req, State = #{auth := SessionId}) ->
    Id = cowboy_req:binding(sid, Req),
    {ok, UserId} = session:get_user_id(SessionId),
    case user_logic:has_eff_space(SessionId, UserId, Id) of
        true ->
            {State#{space_id => Id}, Req};
        false ->
            throw(?ERROR_SPACE_NOT_FOUND)
    end.


%%--------------------------------------------------------------------
%% @doc
%% Retrieves request's timeout and adds it to State.
%% @end
%%--------------------------------------------------------------------
-spec parse_timeout(cowboy_req:req(), maps:map()) ->
    {parse_result(), cowboy_req:req()}.
parse_timeout(Req, State) ->
    {RawTimeout, NewReq} = qs_val(<<"timeout">>, Req, ?DEFAULT_TIMEOUT),
    case RawTimeout of
        <<"infinity">> ->
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
-spec parse_last_seq(cowboy_req:req(), maps:map()) ->
    {parse_result(), cowboy_req:req()}.
parse_last_seq(Req, #{space_id := SpaceId} = State) ->
    {RawLastSeq, NewReq} = qs_val(<<"last_seq">>, Req, ?DEFAULT_LAST_SEQ),
    case RawLastSeq of
        <<"now">> ->
            LastSeq = dbsync_state:get_seq(SpaceId, oneprovider:get_id()),
            {State#{last_seq => LastSeq}, NewReq};
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


%%--------------------------------------------------------------------
%% @doc
%% Retrieves qs param and cache parsed params.
%% @end
%%--------------------------------------------------------------------
-spec qs_val(Name :: binary(), Req :: cowboy_req:req(), Default :: any()) ->
    {any(), cowboy_req:req()}.
qs_val(Name, Req, Default) ->
    case maps:get('_params', Req, undefined) of
        undefined ->
            Params = parse_qs(Req),
            {maps:get(Name, Params, Default), Req#{'_params' => Params}};
        Map ->
            {maps:get(Name, Map, Default), Req}
    end.


%%--------------------------------------------------------------------
%% @doc
%% Parse query string.
%% @end
%%--------------------------------------------------------------------
-spec parse_qs(cowboy_req:req()) -> any().
parse_qs(Req) ->
    Params = lists:foldl(fun({Key, Val}, AccMap) ->
        case maps:get(Key, AccMap, undefined) of
            undefined ->
                AccMap#{Key => Val};
            OldVal when is_list(OldVal) ->
                AccMap#{Key => [Val | OldVal]};
            OldVal ->
                AccMap#{Key => [Val, OldVal]}
        end
    end, #{}, cowboy_req:parse_qs(Req)),
    maps:fold(fun
        (K, V, AccIn) when is_list(V) ->
            AccIn#{K => lists:reverse(V)};
        (_K, _V, AccIn) ->
            AccIn
    end, Params, Params).
