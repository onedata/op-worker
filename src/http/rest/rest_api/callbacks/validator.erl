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


-define(ALLOWED_METADATA_TYPES, [<<"json">>, <<"rdf">>, undefined]).

-define(DEFAULT_INHERITED, <<"false">>).
-define(DEFAULT_TIMEOUT, <<"infinity">>).
-define(DEFAULT_LAST_SEQ, <<"now">>).

%% API
-export([
    malformed_request/2, parse_path/2,
    parse_id/2, parse_objectid/2,
    parse_space_id/2, parse_user_id/2,
    parse_timeout/2, parse_last_seq/2,
    parse_metadata_type/2,

    parse_filter/2, parse_filter_type/2, parse_inherited/2
]).

%% TODO VFS-2574 Make validation of result map
-type parse_result() :: maps:map().

%%%===================================================================
%%% API
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc Extract the REST api version and path and put it in State.
%%--------------------------------------------------------------------
-spec malformed_request(req(), maps:map()) -> {false, req(), maps:map()}.
malformed_request(Req, State) ->
    {State2, Req2} = parse_path(Req, State),
    {false, Req2, State2}.

%%--------------------------------------------------------------------
%% @doc
%% Retrieves request's file path and adds it to state.
%% @end
%%--------------------------------------------------------------------
-spec parse_path(cowboy_req:req(), maps:map()) ->
    {parse_result(), cowboy_req:req()}.
parse_path(Req, State) ->
    case cowboy_req:path_info(Req) of
        undefined ->
            throw(?ERROR_NOT_FOUND_REST);
        Path ->
            {State#{path => filename:join([<<"/">> | Path])}, Req}
    end.

%%--------------------------------------------------------------------
%% @doc
%% Retrieves request's id and adds it to State.
%% @end
%%--------------------------------------------------------------------
-spec parse_id(cowboy_req:req(), maps:map()) ->
    {parse_result(), cowboy_req:req()}.
parse_id(Req, State) ->
    {State#{id => cowboy_req:binding(id, Req)}, Req}.

%%--------------------------------------------------------------------
%% @doc
%% Retrieves request's object id, converts it to uuid and adds it to State.
%% @end
%%--------------------------------------------------------------------
-spec parse_objectid(cowboy_req:req(), maps:map()) ->
    {parse_result(), cowboy_req:req()}.
parse_objectid(Req, State) ->
    case catch file_id:objectid_to_guid(cowboy_req:binding(id, Req)) of
        {ok, Guid} ->
            {State#{id => Guid}, Req};
        _Error ->
            throw(?ERROR_INVALID_OBJECTID)
    end.

%%--------------------------------------------------------------------
%% @doc
%% Retrieves request's user_id and adds it to State.
%% @end
%%--------------------------------------------------------------------
-spec parse_user_id(cowboy_req:req(), maps:map()) ->
    {parse_result(), cowboy_req:req()}.
parse_user_id(Req, State) ->
    {State#{user_id => cowboy_req:binding(uid, Req)}, Req}.

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

%%--------------------------------------------------------------------
%% @doc
%% Retrieves request's metadata type and adds it to State.
%% @end
%%--------------------------------------------------------------------
-spec parse_metadata_type(cowboy_req:req(), maps:map()) ->
    {parse_result(), cowboy_req:req()}.
parse_metadata_type(Req, State) ->
    {MetadataType, NewReq} = qs_val(<<"metadata_type">>, Req),
    case lists:member(MetadataType, ?ALLOWED_METADATA_TYPES) of
        true ->
            case MetadataType of
                undefined ->
                    {State#{metadata_type => undefined}, NewReq};
                _ ->
                    {State#{metadata_type => binary_to_existing_atom(MetadataType, utf8)}, NewReq}
            end;
        false ->
            throw(?ERROR_INVALID_METADATA_TYPE)
    end.

%%--------------------------------------------------------------------
%% @doc
%% Retrieves request's filter param and adds it to State.
%% @end
%%--------------------------------------------------------------------
-spec parse_filter(cowboy_req:req(), maps:map()) ->
    {parse_result(), cowboy_req:req()}.
parse_filter(Req, State) ->
    {Val, NewReq} = qs_val(<<"filter">>, Req),
    {State#{filter => Val}, NewReq}.

%%--------------------------------------------------------------------
%% @doc
%% Retrieves request's filter_type param and adds it to State.
%% @end
%%--------------------------------------------------------------------
-spec parse_filter_type(cowboy_req:req(), maps:map()) ->
    {parse_result(), cowboy_req:req()}.
parse_filter_type(Req, State) ->
    {Val, NewReq} = qs_val(<<"filter_type">>, Req),
    {State#{filter_type => Val}, NewReq}.

%%--------------------------------------------------------------------
%% @doc
%% Retrieves request's inherited param and adds it to State.
%% @end
%%--------------------------------------------------------------------
-spec parse_inherited(cowboy_req:req(), maps:map()) ->
    {parse_result(), cowboy_req:req()}.
parse_inherited(Req, State) ->
    {Inherited, NewReq} = qs_val(<<"inherited">>, Req, ?DEFAULT_INHERITED),
    case Inherited of
        <<"true">> ->
            {State#{inherited => true}, NewReq};
        <<"false">> ->
            {State#{inherited => false}, NewReq};
        _ ->
            throw(?ERROR_INVALID_INHERITED_FLAG)
    end.


%%%===================================================================
%%% Internal functions
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Retrieves qs param.
%% @end
%%--------------------------------------------------------------------
-spec qs_val(Name :: binary(), Req :: cowboy_req:req()) ->
    {binary() | undefined, cowboy_req:req()}.
qs_val(Name, Req) ->
    qs_val(Name, Req, undefined).

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
