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

-define(ALLOWED_ATTRIBUTES, [<<"mode">>, undefined]).
-define(DEFAULT_EXTENDED, <<"false">>).
-define(DEFAULT_INHERITED, <<"false">>).

-define(DEFAULT_TIMEOUT, <<"infinity">>).

-define(DEFAULT_LAST_SEQ, <<"now">>).

-define(MAX_LIMIT, 1000).

-define(DEFAULT_OFFSET, <<"0">>).

%% API
-export([malformed_request/2, parse_path/2,
    parse_id/2, parse_objectid/2, parse_attribute/2, parse_extended/2, parse_attribute_body/2,
    parse_provider_id/2, parse_callback/2, parse_space_id/2, parse_user_id/2,
    parse_timeout/2, parse_last_seq/2, parse_offset/2, parse_dir_limit/2,
    parse_status/2, parse_metadata_type/2, parse_name/2, parse_query_space_id/2,
    parse_function/2, parse_bbox/2, parse_descending/2, parse_endkey/2, parse_key/2,
    parse_keys/2, parse_skip/2, parse_stale/2, parse_limit/2, parse_inclusive_end/2,
    parse_startkey/2, parse_filter/2, parse_filter_type/2, parse_inherited/2]).

%%%===================================================================
%%% API
%%%===================================================================

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
%% Retrieves request's object id, converts it to uuid and adds it to State.
%% @end
%%--------------------------------------------------------------------
-spec parse_objectid(cowboy_req:req(), #{}) ->
    {#{id => binary()}, cowboy_req:req()}.
parse_objectid(Req, State) ->
    {Id, NewReq} = cowboy_req:binding(id, Req),
    case catch cdmi_id:objectid_to_uuid(Id) of
        {ok, Guid} ->
            {State#{id => Guid}, NewReq};
        Error ->
            throw(?ERROR_INVALID_OBJECTID)
    end.

%%--------------------------------------------------------------------
%% @doc
%% Retrieves request's user_id and adds it to State.
%% @end
%%--------------------------------------------------------------------
-spec parse_user_id(cowboy_req:req(), #{}) ->
    {#{user_id => binary()}, cowboy_req:req()}.
parse_user_id(Req, State) ->
    {Id, NewReq} = cowboy_req:binding(uid, Req),
    {State#{user_id => Id}, NewReq}.

%%--------------------------------------------------------------------
%% @doc
%% Retrieves request's xattr flag and adds it to State.
%% @end
%%--------------------------------------------------------------------
-spec parse_extended(cowboy_req:req(), #{}) ->
    {#{extended => binary()}, cowboy_req:req()}.
parse_extended(Req, State) ->
    {Extended, NewReq} = cowboy_req:qs_val(<<"extended">>, Req, ?DEFAULT_EXTENDED),
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
-spec parse_attribute(cowboy_req:req(), #{}) ->
    {#{attribute => binary()}, cowboy_req:req()}.
parse_attribute(Req, State = #{extended := true}) ->
    {Attribute, NewReq} = cowboy_req:qs_val(<<"attribute">>, Req),
    {State#{attribute => Attribute}, NewReq};
parse_attribute(Req, State) ->
    {Attribute, NewReq} = cowboy_req:qs_val(<<"attribute">>, Req),
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
parse_attribute_body(Req, State = #{extended := Extended}) ->
    {ok, Body, Req2} = cowboy_req:body(Req),

    Json = json_utils:decode_map(Body),
    case {
        proplists:get_value(<<"name">>, Json),
        proplists:get_value(<<"value">>, Json),
        Extended
    } of
        {undefined, _, _} ->
            throw(?ERROR_INVALID_ATTRIBUTE_BODY);
        {_, undefined, _} ->
            throw(?ERROR_INVALID_ATTRIBUTE_BODY);
        {<<"mode">>, Value, false} ->
            try binary_to_integer(Value, 8) of
                Mode ->
                    {State#{attribute_body => {<<"mode">>, Mode}}, Req2}
            catch
               _:_ ->
                   throw(?ERROR_INVALID_MODE)
            end;
        {_Attr, _Value, false} ->
            throw(?ERROR_INVALID_ATTRIBUTE);
        {Attr, Value, true} ->
            {State#{attribute_body => {Attr, Value}}, Req2}
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

    Json = json_utils:decode_map(Body),
    Callback = maps:get(<<"url">>, Json),
    {State#{callback => Callback}, NewReq}.

%%--------------------------------------------------------------------
%% @doc
%% Retrieves request's space id and adds it to State.
%% @end
%%--------------------------------------------------------------------
-spec parse_space_id(cowboy_req:req(), #{}) ->
    {#{space_id => binary()}, cowboy_req:req()}.
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
-spec parse_last_seq(cowboy_req:req(), #{}) ->
    {#{id => binary()}, cowboy_req:req()}.
parse_last_seq(Req, State) ->
    {RawLastSeq, NewReq} = cowboy_req:qs_val(<<"last_seq">>, Req, ?DEFAULT_LAST_SEQ),
    case RawLastSeq of
        <<"now">> ->
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

%%--------------------------------------------------------------------
%% @doc
%% Retrieves request's offset and adds it to State.
%% @end
%%--------------------------------------------------------------------
-spec parse_offset(cowboy_req:req(), #{}) ->
    {#{id => binary()}, cowboy_req:req()}.
parse_offset(Req, State) ->
    {RawOffset, NewReq} = cowboy_req:qs_val(<<"offset">>, Req, ?DEFAULT_OFFSET),
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
-spec parse_dir_limit(cowboy_req:req(), #{}) ->
    {#{id => binary()}, cowboy_req:req()}.
parse_dir_limit(Req, State) ->
    {RawLimit, NewReq} = cowboy_req:qs_val(<<"limit">>, Req),
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
-spec parse_status(cowboy_req:req(), #{}) ->
    {#{id => binary()}, cowboy_req:req()}.
parse_status(Req, State) ->
    {Status, NewReq} = cowboy_req:qs_val(<<"status">>, Req),
    {State#{status => Status}, NewReq}.

%%--------------------------------------------------------------------
%% @doc
%% Retrieves request's metadata type and adds it to State.
%% @end
%%--------------------------------------------------------------------
-spec parse_metadata_type(cowboy_req:req(), #{}) ->
    {#{metadata_type => binary()}, cowboy_req:req()}.
parse_metadata_type(Req, State) ->
    {MetadataType, NewReq} = cowboy_req:qs_val(<<"metadata_type">>, Req),
    {State#{metadata_type => MetadataType}, NewReq}.

%%--------------------------------------------------------------------
%% @doc
%% Retrieves request's name and adds it to State.
%% @end
%%--------------------------------------------------------------------
-spec parse_name(cowboy_req:req(), #{}) ->
    {#{name => binary()}, cowboy_req:req()}.
parse_name(Req, State) ->
    {Name, NewReq} = cowboy_req:qs_val(<<"name">>, Req),
    {State#{name => Name}, NewReq}.

%%--------------------------------------------------------------------
%% @doc
%% Retrieves request's space_id and adds it to State.
%% @end
%%--------------------------------------------------------------------
-spec parse_query_space_id(cowboy_req:req(), #{}) ->
    {#{space_id => binary()}, cowboy_req:req()}.
parse_query_space_id(Req, State) ->
    {SpaceId, NewReq} = cowboy_req:qs_val(<<"space_id">>, Req),
    {State#{space_id => SpaceId}, NewReq}.

%%--------------------------------------------------------------------
%% @doc
%% Retrieves request's function body and adds it to State.
%% @end
%%--------------------------------------------------------------------
-spec parse_function(cowboy_req:req(), #{}) ->
    {#{function => binary()}, cowboy_req:req()}.
parse_function(Req, State) ->
    {ok, Body, NewReq} = cowboy_req:body(Req),
    {State#{function => Body}, NewReq}.

%%--------------------------------------------------------------------
%% @doc
%% Retrieves request's bbox param and adds it to State.
%% @end
%%--------------------------------------------------------------------
-spec parse_bbox(cowboy_req:req(), #{}) ->
    {#{bbox => binary()}, cowboy_req:req()}.
parse_bbox(Req, State) ->
    {Val, NewReq} = cowboy_req:qs_val(<<"bbox">>, Req),
    {State#{bbox => Val}, NewReq}.

%%--------------------------------------------------------------------
%% @doc
%% Retrieves request's descending param and adds it to State.
%% @end
%%--------------------------------------------------------------------
-spec parse_descending(cowboy_req:req(), #{}) ->
    {#{descending => binary()}, cowboy_req:req()}.
parse_descending(Req, State) ->
    {Val, NewReq} = cowboy_req:qs_val(<<"descending">>, Req),
    {State#{descending => Val}, NewReq}.

%%--------------------------------------------------------------------
%% @doc
%% Retrieves request's endkey param and adds it to State.
%% @end
%%--------------------------------------------------------------------
-spec parse_endkey(cowboy_req:req(), #{}) ->
    {#{endkey => binary()}, cowboy_req:req()}.
parse_endkey(Req, State) ->
    {Val, NewReq} = cowboy_req:qs_val(<<"endkey">>, Req),
    {State#{endkey => Val}, NewReq}.


%%--------------------------------------------------------------------
%% @doc
%% Retrieves request's inclusive_end param and adds it to State.
%% @end
%%--------------------------------------------------------------------
-spec parse_inclusive_end(cowboy_req:req(), #{}) ->
    {#{inclusive_end => binary()}, cowboy_req:req()}.
parse_inclusive_end(Req, State) ->
    {Val, NewReq} = cowboy_req:qs_val(<<"inclusive_end">>, Req),
    {State#{inclusive_end => Val}, NewReq}.

%%--------------------------------------------------------------------
%% @doc
%% Retrieves request's key param and adds it to State.
%% @end
%%--------------------------------------------------------------------
-spec parse_key(cowboy_req:req(), #{}) ->
    {#{key => binary()}, cowboy_req:req()}.
parse_key(Req, State) ->
    {Val, NewReq} = cowboy_req:qs_val(<<"key">>, Req),
    {State#{key => Val}, NewReq}.

%%--------------------------------------------------------------------
%% @doc
%% Retrieves request's keys param and adds it to State.
%% @end
%%--------------------------------------------------------------------
-spec parse_keys(cowboy_req:req(), #{}) ->
    {#{keys => binary()}, cowboy_req:req()}.
parse_keys(Req, State) ->
    {Val, NewReq} = cowboy_req:qs_val(<<"keys">>, Req),
    {State#{keys => Val}, NewReq}.

%%--------------------------------------------------------------------
%% @doc
%% Retrieves request's limit param and adds it to State.
%% @end
%%--------------------------------------------------------------------
-spec parse_limit(cowboy_req:req(), #{}) ->
    {#{limit => binary()}, cowboy_req:req()}.
parse_limit(Req, State) ->
    {Val, NewReq} = cowboy_req:qs_val(<<"limit">>, Req),
    {State#{limit => Val}, NewReq}.

%%--------------------------------------------------------------------
%% @doc
%% Retrieves request's skip param and adds it to State.
%% @end
%%--------------------------------------------------------------------
-spec parse_skip(cowboy_req:req(), #{}) ->
    {#{skip => binary()}, cowboy_req:req()}.
parse_skip(Req, State) ->
    {Val, NewReq} = cowboy_req:qs_val(<<"skip">>, Req),
    {State#{skip => Val}, NewReq}.

%%--------------------------------------------------------------------
%% @doc
%% Retrieves request's stale param and adds it to State.
%% @end
%%--------------------------------------------------------------------
-spec parse_stale(cowboy_req:req(), #{}) ->
    {#{stale => binary()}, cowboy_req:req()}.
parse_stale(Req, State) ->
    {Val, NewReq} = cowboy_req:qs_val(<<"stale">>, Req),
    {State#{stale => Val}, NewReq}.

%%--------------------------------------------------------------------
%% @doc
%% Retrieves request's startkey param and adds it to State.
%% @end
%%--------------------------------------------------------------------
-spec parse_startkey(cowboy_req:req(), #{}) ->
    {#{startkey => binary()}, cowboy_req:req()}.
parse_startkey(Req, State) ->
    {Val, NewReq} = cowboy_req:qs_val(<<"startkey">>, Req),
    {State#{startkey => Val}, NewReq}.

%%--------------------------------------------------------------------
%% @doc
%% Retrieves request's filter param and adds it to State.
%% @end
%%--------------------------------------------------------------------
-spec parse_filter(cowboy_req:req(), #{}) ->
    {#{filter => binary()}, cowboy_req:req()}.
parse_filter(Req, State) ->
    {Val, NewReq} = cowboy_req:qs_val(<<"filter">>, Req),
    {State#{filter => Val}, NewReq}.

%%--------------------------------------------------------------------
%% @doc
%% Retrieves request's filter_type param and adds it to State.
%% @end
%%--------------------------------------------------------------------
-spec parse_filter_type(cowboy_req:req(), #{}) ->
    {#{filter_type => binary()}, cowboy_req:req()}.
parse_filter_type(Req, State) ->
    {Val, NewReq} = cowboy_req:qs_val(<<"filter_type">>, Req),
    {State#{filter_type => Val}, NewReq}.

%%--------------------------------------------------------------------
%% @doc
%% Retrieves request's inherited param and adds it to State.
%% @end
%%--------------------------------------------------------------------
-spec parse_inherited(cowboy_req:req(), #{}) ->
    {#{inherited => binary()}, cowboy_req:req()}.
parse_inherited(Req, State) ->
    {Inherited, NewReq} = cowboy_req:qs_val(<<"inherited">>, Req, ?DEFAULT_EXTENDED),
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
