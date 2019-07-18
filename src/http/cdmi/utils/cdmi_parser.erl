%%%--------------------------------------------------------------------
%%% @author Piotr Ociepka
%%% @author Tomasz Lichon
%%% @author Bartosz Walkowicz
%%% @copyright (C) 2015-2019 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%--------------------------------------------------------------------
%%% @doc
%%% This module implements some functions for parsing
%%% and processing parameters of request.
%%% @end
%%%--------------------------------------------------------------------
-module(cdmi_parser).
-author("Piotr Ociepka").
-author("Tomasz Lichon").
-author("Bartosz Walkowicz").

-include("op_logic.hrl").
%%-include_lib("ctool/include/logging.hrl").
-include_lib("ctool/include/api_errors.hrl").

% keys that are forbidden to appear simultaneously in a request's body
-define(KEYS_REQUIRED_TO_BE_EXCLUSIVE, [
    <<"deserialize">>, <<"copy">>, <<"move">>,
    <<"reference">>, <<"deserializevalue">>, <<"value">>
]).

-define(CDMI_VERSION_HEADER, <<"x-cdmi-specification-version">>).

%% API
-export([
    get_ranges/2, parse_content_range/2,
    parse_body/1,
    parse_content/1
]).


%%%===================================================================
%%% API
%%%===================================================================


%%--------------------------------------------------------------------
%% @doc Get requested ranges list.
%%--------------------------------------------------------------------
-spec get_ranges(cowboy_req:req(), Size :: non_neg_integer()) ->
    {[{non_neg_integer(), non_neg_integer()}] | undefined, cowboy_req:req()}.
get_ranges(Req, Size) ->
    case cowboy_req:header(<<"range">>, Req) of
        undefined ->
            {undefined, Req};
        RawRange ->
            case parse_byte_range(RawRange, Size) of
                invalid -> throw(?ERROR_BAD_DATA(<<"range">>));
                Ranges -> {Ranges, Req}
            end
    end.


%%--------------------------------------------------------------------
%% @doc Reads whole body and decodes it as json.
%%--------------------------------------------------------------------
-spec parse_body(cowboy_req:req()) -> {ok, map(), cowboy_req:req()}.
parse_body(Req) ->
    {ok, RawBody, Req1} = cowboy_req:read_body(Req),
    Body = case RawBody of
        <<>> -> #{};
        _ -> json_utils:decode(RawBody)
    end,
    validate_body(Body),
    {ok, Body, Req1}.


%%--------------------------------------------------------------------
%% @doc
%% Parses byte ranges from 'content-range' http header format to erlang
%% range tuple, i. e. <<"bytes 1-5/10">> will produce -> {1,5}
%% @end
%%--------------------------------------------------------------------
-spec parse_content_range(binary() | list(), non_neg_integer()) ->
    invalid | {Range :: {From :: integer(), To :: integer()}, ExpectedSize :: integer() | undefined}.
parse_content_range(RawHeaderRange, Size) when is_binary(RawHeaderRange) ->
    try
        [<<"bytes">>, RawRangeWithSize] = binary:split(RawHeaderRange, <<" ">>, [global]),
        [RawRange, RawExpectedSize] = binary:split(RawRangeWithSize, <<"/">>, [global]),
        [Range] = parse_byte_range([RawRange], Size),
        case RawExpectedSize of
            <<"*">> ->
                {Range, undefined};
            _ ->
                {Range, binary_to_integer(RawExpectedSize)}
        end
    catch
        _:_ ->
            invalid
    end.


%%--------------------------------------------------------------------
%% @doc
%% Parses content-type header to mimetype and charset part, if charset
%% is other than utf-8, function returns undefined
%% @end
%%--------------------------------------------------------------------
-spec parse_content(binary()) -> {Mimetype :: binary(), Encoding :: binary() | undefined}.
parse_content(Content) ->
    case binary:split(Content, <<";">>) of
        [RawMimetype, RawEncoding] ->
            case binary:split(utils:trim_spaces(RawEncoding), <<"=">>) of
                [<<"charset">>, <<"utf-8">>] ->
                    {utils:trim_spaces(RawMimetype), <<"utf-8">>};
                _ ->
                    {utils:trim_spaces(RawMimetype), undefined}
            end;
        [RawMimetype] ->
            {utils:trim_spaces(RawMimetype), undefined}
    end.


%%%===================================================================
%%% Internal functions
%%%===================================================================


%%--------------------------------------------------------------------
%% @private
%% @doc
%% Parses byte ranges from 'Range' http header format to list of
%% erlang range tuples, i. e. <<"bytes=1-5,-3">> for a file with length 10
%% will produce -> [{1,5},{7,9}]
%% @end
%%--------------------------------------------------------------------
-spec parse_byte_range(binary() | list(), non_neg_integer()) ->
    invalid | [{From :: integer(), To :: integer()}].
parse_byte_range(Range, Size) when is_binary(Range) ->
    case binary:split(Range, <<"=">>, [global]) of
        [<<"bytes">>, RawRange] ->
            parse_byte_range(binary:split(RawRange, <<",">>, [global]), Size);
        _ ->
            invalid
    end;
parse_byte_range(Ranges0, Size) ->
    Ranges1 = lists:map(fun(Range0) ->
        Range1 = case binary:split(Range0, <<"-">>, [global]) of
            [<<>>, FromEnd] ->
                {max(0, Size - binary_to_integer(FromEnd)), Size - 1};
            [From, <<>>] ->
                {binary_to_integer(From), Size - 1};
            [From_, To] ->
                {binary_to_integer(From_), binary_to_integer(To)};
            _ ->
                invalid
        end,
        case Range1 of
            invalid -> invalid;
            {F, T} when F > T -> invalid;
            _ -> Range1
        end
    end, Ranges0),
    case lists:member(invalid, Ranges1) of
        true -> invalid;
        false -> Ranges1
    end.


%%--------------------------------------------------------------------
%% @private
%% @doc
%% Validates correctness of request's body.
%% @end
%%--------------------------------------------------------------------
-spec validate_body(maps:map()) -> ok | no_return().
validate_body(Body) ->
    Keys = maps:keys(Body),
    KeySet = sets:from_list(Keys),
    ExclusiveRequiredKeysSet = sets:from_list(?KEYS_REQUIRED_TO_BE_EXCLUSIVE),
    case sets:size(sets:intersection(KeySet, ExclusiveRequiredKeysSet)) of
        N when N > 1 ->
            throw(?ERROR_MALFORMED_DATA);
        _ ->
            ok
    end.
