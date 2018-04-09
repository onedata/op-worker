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

-define(ALLOWED_METADATA_TYPES, [<<"json">>, <<"rdf">>, undefined]).

-define(DEFAULT_EXTENDED, <<"false">>).
-define(DEFAULT_INHERITED, <<"false">>).
-define(DEFAULT_TIMEOUT, <<"infinity">>).
-define(DEFAULT_LAST_SEQ, <<"now">>).
-define(DEFAULT_OFFSET, <<"0">>).
-define(DEFAULT_SPATIAL, <<"false">>).

-define(MAX_LIMIT, 1000).

%% API
-export([malformed_request/2, parse_path/2,
    parse_id/2, parse_objectid/2, parse_attribute/2, parse_extended/2, parse_attribute_body/2,
    parse_provider_id/2, parse_migration_provider_id/2, parse_callback/2, parse_space_id/2, parse_user_id/2,
    parse_timeout/2, parse_last_seq/2, parse_offset/2, parse_dir_limit/2,
    parse_status/2, parse_metadata_type/2, parse_name/2, parse_query_space_id/2,
    parse_function/2, parse_bbox/2, parse_descending/2, parse_endkey/2, parse_key/2,
    parse_keys/2, parse_skip/2, parse_stale/2, parse_limit/2, parse_inclusive_end/2,
    parse_startkey/2, parse_filter/2, parse_filter_type/2, parse_inherited/2,
    parse_spatial/2, parse_start_range/2, parse_end_range/2]).

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
            throw(?ERROR_NOT_FOUND);
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
    case catch cdmi_id:objectid_to_guid(cowboy_req:binding(id, Req)) of
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
%% Retrieves request's xattr flag and adds it to State.
%% @end
%%--------------------------------------------------------------------
-spec parse_extended(cowboy_req:req(), maps:map()) ->
    {parse_result(), cowboy_req:req()}.
parse_extended(Req, State) ->
    {Extended, NewReq} = qs_val(<<"extended">>, Req, ?DEFAULT_EXTENDED),
    case Extended of
        <<"true">> ->
            {State#{extended => true}, NewReq};
        <<"false">> ->
            {State#{extended => false}, NewReq};
        _ ->
            throw(?ERROR_INVALID_EXTENDED_FLAG)
    end.

%%--------------------------------------------------------------------
%% @doc
%% Retrieves request's attribute and adds it to State.
%% @end
%%--------------------------------------------------------------------
-spec parse_attribute(cowboy_req:req(), maps:map()) ->
    {parse_result(), cowboy_req:req()}.
parse_attribute(Req, State = #{extended := true}) ->
    {Attribute, NewReq} = qs_val(<<"attribute">>, Req),
    case Attribute =:= undefined orelse is_binary(Attribute) of
        true ->
            {State#{attribute => Attribute}, NewReq};
        false ->
            throw(?ERROR_INVALID_ATTRIBUTE_NAME)
    end;
parse_attribute(Req, State) ->
    {Attribute, NewReq} = qs_val(<<"attribute">>, Req),
    {State#{attribute => Attribute}, NewReq}.

%%--------------------------------------------------------------------
%% @doc
%% Retrieves request's attribute from body and adds it to State.
%% @end
%%--------------------------------------------------------------------
-spec parse_attribute_body(cowboy_req:req(), maps:map()) ->
    {parse_result(), cowboy_req:req()}.
parse_attribute_body(Req, State = #{extended := Extended}) ->
    {ok, Body, Req2} = cowboy_req:read_body(Req),

    Json = json_utils:decode(Body),
    case {
        maps:to_list(Json),
        Extended
    } of
        {[{<<"mode">>, Value}], false} ->
            try binary_to_integer(Value, 8) of
                Mode ->
                    {State#{attribute_body => {<<"mode">>, Mode}}, Req2}
            catch
               _:_ ->
                   throw(?ERROR_INVALID_MODE)
            end;
        {[{_Attr, _Value}], false} ->
            throw(?ERROR_INVALID_ATTRIBUTE);
        {[{Attr, _Value}], true} when not is_binary(Attr) ->
            throw(?ERROR_INVALID_ATTRIBUTE_NAME);
        {[{Attr, Value}], true} ->
            {State#{attribute_body => {Attr, Value}}, Req2};
        {_, _} ->
            throw(?ERROR_INVALID_ATTRIBUTE_BODY)
    end.

%%--------------------------------------------------------------------
%% @doc
%% Retrieves request's provider_id parameter and adds it to State.
%% @end
%%--------------------------------------------------------------------
-spec parse_provider_id(cowboy_req:req(), maps:map()) ->
    {parse_result(), cowboy_req:req()}.
parse_provider_id(Req, State) ->
    {ProviderId, NewReq} = qs_val(<<"provider_id">>, Req, oneprovider:get_id()),
    {State#{provider_id => ProviderId}, NewReq}.

%%--------------------------------------------------------------------
%% @doc
%% Retrieves request's migration_provider_id parameter and adds it to State.
%% @end
%%--------------------------------------------------------------------
-spec parse_migration_provider_id(cowboy_req:req(), maps:map()) ->
    {parse_result(), cowboy_req:req()}.
parse_migration_provider_id(Req, State) ->
    {ProviderId, NewReq} = qs_val(<<"migration_provider_id">>, Req, undefined),
    {State#{migration_provider_id => ProviderId}, NewReq}.

%%--------------------------------------------------------------------
%% @doc
%% Retrieves request's callback body and adds it to State.
%% @end
%%--------------------------------------------------------------------
-spec parse_callback(cowboy_req:req(), maps:map()) ->
    {parse_result(), cowboy_req:req()}.
parse_callback(Req, State) ->
    {ok, Body, NewReq} = cowboy_req:read_body(Req),

    Callback =
        case Body of
            <<"">> ->
                undefined;
            _ ->
                Json = json_utils:decode(Body),
                maps:get(<<"url">>, Json, undefined)
        end,
    {State#{callback => Callback}, NewReq}.

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
%% Retrieves request's offset and adds it to State.
%% @end
%%--------------------------------------------------------------------
-spec parse_offset(cowboy_req:req(), maps:map()) ->
    {parse_result(), cowboy_req:req()}.
parse_offset(Req, State) ->
    {RawOffset, NewReq} = qs_val(<<"offset">>, Req, ?DEFAULT_OFFSET),
    try binary_to_integer(RawOffset) of
        Offset ->
            {State#{offset => Offset}, NewReq}
    catch
        _:_ ->
            throw(?ERROR_INVALID_OFFSET)
    end.

%%--------------------------------------------------------------------
%% @doc
%% Retrieves request's limit and adds it to State.
%% @end
%%--------------------------------------------------------------------
-spec parse_dir_limit(cowboy_req:req(), maps:map()) ->
    {parse_result(), cowboy_req:req()}.
parse_dir_limit(Req, State) ->
    {RawLimit, NewReq} = qs_val(<<"limit">>, Req),
    case RawLimit of
        undefined ->
            {State#{limit => undefined}, NewReq};
        _ ->
            try binary_to_integer(RawLimit) of
                Limit ->
                    case Limit > ?MAX_LIMIT of
                        true ->
                            throw(?ERROR_LIMIT_TOO_LARGE(?MAX_LIMIT));
                        false ->
                            {State#{limit => Limit}, NewReq}
                    end
            catch
                _:_ ->
                    throw(?ERROR_INVALID_LIMIT)
            end
    end.

%%--------------------------------------------------------------------
%% @doc
%% Retrieves request's limit and adds it to State.
%% @end
%%--------------------------------------------------------------------
-spec parse_status(cowboy_req:req(), maps:map()) ->
    {parse_result(), cowboy_req:req()}.
parse_status(Req, State) ->
    {Status, NewReq} = qs_val(<<"status">>, Req),
    {State#{status => Status}, NewReq}.

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
%% Retrieves request's name and adds it to State.
%% @end
%%--------------------------------------------------------------------
-spec parse_name(cowboy_req:req(), maps:map()) ->
    {parse_result(), cowboy_req:req()}.
parse_name(Req, State) ->
    {Name, NewReq} = qs_val(<<"name">>, Req),
    {State#{name => Name}, NewReq}.

%%--------------------------------------------------------------------
%% @doc
%% Retrieves request's space_id and adds it to State.
%% @end
%%--------------------------------------------------------------------
-spec parse_query_space_id(cowboy_req:req(), maps:map()) ->
    {parse_result(), cowboy_req:req()}.
parse_query_space_id(Req, State) ->
    {SpaceId, NewReq} = qs_val(<<"space_id">>, Req),
    {State#{space_id => SpaceId}, NewReq}.

%%--------------------------------------------------------------------
%% @doc
%% Retrieves request's function body and adds it to State.
%% @end
%%--------------------------------------------------------------------
-spec parse_function(cowboy_req:req(), maps:map()) ->
    {parse_result(), cowboy_req:req()}.
parse_function(Req, State) ->
    {ok, Body, NewReq} = cowboy_req:read_body(Req),
    {State#{function => Body}, NewReq}.

%%--------------------------------------------------------------------
%% @doc
%% Retrieves request's spatial param and adds it to State.
%% @end
%%--------------------------------------------------------------------
-spec parse_spatial(cowboy_req:req(), maps:map()) ->
    {parse_result(), cowboy_req:req()}.
parse_spatial(Req, State) ->
    {Spatial, NewReq} = qs_val(<<"spatial">>, Req, ?DEFAULT_SPATIAL),
    case Spatial of
        <<"true">> ->
            {State#{spatial => true}, NewReq};
        <<"false">> ->
            {State#{spatial => false}, NewReq};
        _ ->
            throw(?ERROR_INVALID_SPATIAL_FLAG)
    end.

%%--------------------------------------------------------------------
%% @doc
%% Retrieves request's bbox param and adds it to State.
%% @end
%%--------------------------------------------------------------------
-spec parse_bbox(cowboy_req:req(), maps:map()) ->
    {parse_result(), cowboy_req:req()}.
parse_bbox(Req, State) ->
    {Val, NewReq} = qs_val(<<"bbox">>, Req),
    try
        case Val of
            undefined ->
                ok;
            _ ->
                [W, S, E, N] = binary:split(Val, <<",">>, [global]),
                true = is_float(catch binary_to_float(W)) orelse is_integer(catch binary_to_integer(W)),
                true = is_float(catch binary_to_float(S)) orelse is_integer(catch binary_to_integer(S)),
                true = is_float(catch binary_to_float(E)) orelse is_integer(catch binary_to_integer(E)),
                true = is_float(catch binary_to_float(N)) orelse is_integer(catch binary_to_integer(N))
        end
    catch
        _:_  ->
            throw(?ERROR_INVALID_BBOX)
    end,
    {State#{bbox => Val}, NewReq}.

%%--------------------------------------------------------------------
%% @doc
%% Retrieves request's descending param and adds it to State.
%% @end
%%--------------------------------------------------------------------
-spec parse_descending(cowboy_req:req(), maps:map()) ->
    {parse_result(), cowboy_req:req()}.
parse_descending(Req, State) ->
    {Val, NewReq} = qs_val(<<"descending">>, Req),
    {State#{descending => Val}, NewReq}.

%%--------------------------------------------------------------------
%% @doc
%% Retrieves request's endkey param and adds it to State.
%% @end
%%--------------------------------------------------------------------
-spec parse_endkey(cowboy_req:req(), maps:map()) ->
    {parse_result(), cowboy_req:req()}.
parse_endkey(Req, State) ->
    {Val, NewReq} = qs_val(<<"endkey">>, Req),
    {State#{endkey => Val}, NewReq}.


%%--------------------------------------------------------------------
%% @doc
%% Retrieves request's inclusive_end param and adds it to State.
%% @end
%%--------------------------------------------------------------------
-spec parse_inclusive_end(cowboy_req:req(), maps:map()) ->
    {parse_result(), cowboy_req:req()}.
parse_inclusive_end(Req, State) ->
    {Val, NewReq} = qs_val(<<"inclusive_end">>, Req),
    {State#{inclusive_end => Val}, NewReq}.

%%--------------------------------------------------------------------
%% @doc
%% Retrieves request's key param and adds it to State.
%% @end
%%--------------------------------------------------------------------
-spec parse_key(cowboy_req:req(), maps:map()) ->
    {parse_result(), cowboy_req:req()}.
parse_key(Req, State) ->
    {Val, NewReq} = qs_val(<<"key">>, Req),
    {State#{key => Val}, NewReq}.

%%--------------------------------------------------------------------
%% @doc
%% Retrieves request's keys param and adds it to State.
%% @end
%%--------------------------------------------------------------------
-spec parse_keys(cowboy_req:req(), maps:map()) ->
    {parse_result(), cowboy_req:req()}.
parse_keys(Req, State) ->
    {Val, NewReq} = qs_val(<<"keys">>, Req),
    {State#{keys => Val}, NewReq}.

%%--------------------------------------------------------------------
%% @doc
%% Retrieves request's limit param and adds it to State.
%% @end
%%--------------------------------------------------------------------
-spec parse_limit(cowboy_req:req(), maps:map()) ->
    {parse_result(), cowboy_req:req()}.
parse_limit(Req, State) ->
    {Val, NewReq} = qs_val(<<"limit">>, Req),
    {State#{limit => Val}, NewReq}.

%%--------------------------------------------------------------------
%% @doc
%% Retrieves request's skip param and adds it to State.
%% @end
%%--------------------------------------------------------------------
-spec parse_skip(cowboy_req:req(), maps:map()) ->
    {parse_result(), cowboy_req:req()}.
parse_skip(Req, State) ->
    {Val, NewReq} = qs_val(<<"skip">>, Req),
    {State#{skip => Val}, NewReq}.

%%--------------------------------------------------------------------
%% @doc
%% Retrieves request's stale param and adds it to State.
%% @end
%%--------------------------------------------------------------------
-spec parse_stale(cowboy_req:req(), maps:map()) ->
    {parse_result(), cowboy_req:req()}.
parse_stale(Req, State) ->
    {Val, NewReq} = qs_val(<<"stale">>, Req),
    {State#{stale => Val}, NewReq}.

%%--------------------------------------------------------------------
%% @doc
%% Retrieves request's startkey param and adds it to State.
%% @end
%%--------------------------------------------------------------------
-spec parse_startkey(cowboy_req:req(), maps:map()) ->
    {parse_result(), cowboy_req:req()}.
parse_startkey(Req, State) ->
    {Val, NewReq} = qs_val(<<"startkey">>, Req),
    {State#{startkey => Val}, NewReq}.

%%--------------------------------------------------------------------
%% @doc
%% Retrieves request's start_range param and adds it to State.
%% @end
%%--------------------------------------------------------------------
-spec parse_start_range(cowboy_req:req(), maps:map()) ->
    {parse_result(), cowboy_req:req()}.
parse_start_range(Req, State) ->
    {Val, NewReq} = qs_val(<<"start_range">>, Req),
    {State#{start_range => Val}, NewReq}.

%%--------------------------------------------------------------------
%% @doc
%% Retrieves request's end_range param and adds it to State.
%% @end
%%--------------------------------------------------------------------
-spec parse_end_range(cowboy_req:req(), maps:map()) ->
    {parse_result(), cowboy_req:req()}.
parse_end_range(Req, State) ->
    {Val, NewReq} = qs_val(<<"end_range">>, Req),
    {State#{end_range => Val}, NewReq}.

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
    {Inherited, NewReq} = qs_val(<<"inherited">>, Req, ?DEFAULT_EXTENDED),
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
            Params = maps:from_list(cowboy_req:parse_qs(Req)),
            {maps:get(Name, Params, Default), Req#{'_params' => Params}};
        Map ->
            {maps:get(Name, Map, Default), Req}
    end.
