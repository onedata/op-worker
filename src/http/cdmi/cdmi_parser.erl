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
-include_lib("ctool/include/api_errors.hrl").

%% API
-export([
    parse_range_header/2, parse_content_range_header/2,
    parse_content_type_header/1,
    parse_body/1
]).

-type range() :: {From :: non_neg_integer(), To :: non_neg_integer()}.

% keys that are forbidden to appear simultaneously in a request's body
-define(KEYS_REQUIRED_TO_BE_EXCLUSIVE, [
    <<"deserialize">>, <<"copy">>, <<"move">>,
    <<"reference">>, <<"deserializevalue">>, <<"value">>
]).


%%%===================================================================
%%% API
%%%===================================================================


%%--------------------------------------------------------------------
%% @doc Parses byte ranges from 'range' http header.
%%--------------------------------------------------------------------
-spec parse_range_header(cowboy_req:req(), Threshold :: non_neg_integer()) ->
    undefined | [range()].
parse_range_header(Req, Threshold) ->
    case cowboy_req:header(<<"range">>, Req) of
        undefined ->
            undefined;
        RawRange ->
            case parse_byte_range(RawRange, Threshold) of
                invalid -> throw(?ERROR_BAD_DATA(<<"range">>));
                Ranges -> Ranges
            end
    end.


%%--------------------------------------------------------------------
%% @doc
%% Parses byte range from 'content-range' http header format to erlang
%% range tuple, i. e. <<"bytes 1-5/10">> will produce -> {1,5}.
%% @end
%%--------------------------------------------------------------------
-spec parse_content_range_header(cowboy_req:req(), Threshold :: non_neg_integer()) ->
    undefined | {range(), ExpectedSize :: integer() | undefined}.
parse_content_range_header(Req, Threshold) ->
    case cowboy_req:header(<<"content-range">>, Req) of
        undefined ->
            undefined;
        RangeHeader ->
            try
                [<<"bytes">>, RangeWithSize] = binary:split(RangeHeader, <<" ">>, [global]),
                [RangeBin, ExpectedSize] = binary:split(RangeWithSize, <<"/">>, [global]),
                [Range] = parse_byte_range([RangeBin], Threshold),
                case ExpectedSize of
                    <<"*">> -> {Range, undefined};
                    _ -> {Range, binary_to_integer(ExpectedSize)}
                end
            catch _:_ ->
                throw(?ERROR_BAD_DATA(<<"content-range">>))
            end
    end.


%%--------------------------------------------------------------------
%% @doc
%% Parses content-type header (`application/octet-stream` is taken as default
%% if header is not provided then) to mimetype and charset part, if charset
%% is other than utf-8, function returns undefined.
%% @end
%%--------------------------------------------------------------------
-spec parse_content_type_header(cowboy_req:req()) ->
    {Mimetype :: binary(), Encoding :: binary() | undefined}.
parse_content_type_header(Req) ->
    Content = cowboy_req:header(
        <<"content-type">>, Req,
        <<"application/octet-stream">>
    ),
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
    invalid | [range()].
parse_byte_range(RangeBin, Size) when is_binary(RangeBin) ->
    case binary:split(RangeBin, <<"=">>, [global]) of
        [<<"bytes">>, RawRange] ->
            parse_byte_range(binary:split(RawRange, <<",">>, [global]), Size);
        _ ->
            invalid
    end;
parse_byte_range(RawRanges, Size) ->
    ParsedRanges = lists:map(fun(RangeBin) ->
        ParsedRange = case binary:split(RangeBin, <<"-">>, [global]) of
            [<<>>, FromEnd] ->
                {max(0, Size - binary_to_integer(FromEnd)), Size - 1};
            [From, <<>>] ->
                {binary_to_integer(From), Size - 1};
            [From_, To] ->
                {binary_to_integer(From_), binary_to_integer(To)};
            _ ->
                invalid
        end,
        case ParsedRange of
            invalid -> invalid;
            {F, T} when F > T -> invalid;
            _ -> ParsedRange
        end
    end, RawRanges),
    case lists:member(invalid, ParsedRanges) of
        true -> invalid;
        false -> ParsedRanges
    end.


%%--------------------------------------------------------------------
%% @private
%% @doc
%% Validates correctness of request's body.
%% @end
%%--------------------------------------------------------------------
-spec validate_body(map()) -> ok | no_return().
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
