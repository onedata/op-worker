%%%--------------------------------------------------------------------
%%% @author Bartosz Walkowicz
%%% @copyright (C) 2020 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%--------------------------------------------------------------------
%%% @doc
%%% This module implements some functions for parsing and processing parameters
%%% of http request (e.g. headers).
%%% @end
%%%--------------------------------------------------------------------
-module(http_parser).
-author("Bartosz Walkowicz").

%% API
-export([
    parse_range_header/2
]).

% Range of bytes (inclusive at both ends), e.g.:
% - {0, 99} means 100 bytes beginning at offset 0,
% - {10, 10} means 1 byte beginning at offset 10.
-type bytes_range() :: {
    InclusiveRangeStart :: non_neg_integer(),
    InclusiveRangeEnd :: non_neg_integer()
}.

-export_type([bytes_range/0]).

-type content_size() :: non_neg_integer().


%%%===================================================================
%%% API
%%%===================================================================


-spec parse_range_header(cowboy_req:req(), content_size()) ->
    undefined | invalid | [bytes_range()].
parse_range_header(Req, Threshold) ->
    case cowboy_req:header(<<"range">>, Req) of
        undefined -> undefined;
        RawRange -> parse_bytes_ranges(RawRange, Threshold)
    end.


%%%===================================================================
%%% Internal functions
%%%===================================================================


%%--------------------------------------------------------------------
%% @private
%% @doc
%% Returns byte ranges obtained from parsing http 'Range' header:
%%
%% >> parse_byte_range(<<"bytes=1-5,-3,2-">>, 10)
%% [{1, 5}, {7, 9}, {2, 9}]
%%
%% @end
%%--------------------------------------------------------------------
-spec parse_bytes_ranges(binary() | list(), content_size()) -> invalid | [bytes_range()].
parse_bytes_ranges(RangesBin, Threshold) when is_binary(RangesBin) ->
    case binary:split(RangesBin, <<"=">>, [global]) of
        [<<"bytes">>, RawRanges] ->
            parse_bytes_ranges(binary:split(RawRanges, <<",">>, [global]), Threshold);
        _ ->
            invalid
    end;
parse_bytes_ranges(RawRanges, Threshold) ->
    try
        lists:map(fun(RangeBin) -> parse_bytes_range(RangeBin, Threshold) end, RawRanges)
    catch _:_  ->
        invalid
    end.


%% @private
-spec parse_bytes_range(binary() | [binary()], content_size()) -> bytes_range() | no_return().
parse_bytes_range(RangeBin, Threshold) when is_binary(RangeBin) ->
    parse_bytes_range(binary:split(RangeBin, <<"-">>, [global]), Threshold);
parse_bytes_range([<<>>, FromEndBin], Threshold) ->
    validate_bytes_range(
        {max(0, Threshold - binary_to_integer(FromEndBin)), Threshold - 1},
        Threshold
    );
parse_bytes_range([RangeStartBin, <<>>], Threshold) ->
    validate_bytes_range(
        {binary_to_integer(RangeStartBin), Threshold - 1},
        Threshold
    );
parse_bytes_range([RangeStartBin, RangeEndBin], Threshold) ->
    validate_bytes_range(
        {binary_to_integer(RangeStartBin), binary_to_integer(RangeEndBin)},
        Threshold
    );
parse_bytes_range(_InvalidRange, _Threshold) ->
    throw(invalid).


%% @private
-spec validate_bytes_range(bytes_range(), content_size()) -> bytes_range() | no_return().
validate_bytes_range({RangeStart, RangeEnd}, Threshold) when
    RangeStart < 0;
    RangeStart > RangeEnd;
    RangeStart >= Threshold
->
    throw(invalid);
validate_bytes_range(Range, _Threshold) ->
    Range.
