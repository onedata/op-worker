%% ===================================================================
%% @author Tomasz Lichon
%% @copyright (C): 2014 ACK CYFRONET AGH
%% This software is released under the MIT license
%% cited in 'LICENSE.txt'.
%% @end
%% ===================================================================
%% @doc Support for CDMI Object ID (taken from https://github.com/jeastman/crime)
%%
%% Orginal description:
%% Object IDs are used to identify objects in CDMI. Object IDs are intended
%% to be globally unique values that have a specific structure. The native
%% format of an object ID is a variable length byte sequence with a maximum
%% size of 40 bytes. This leaves an implementer up to 32 bytes for data
%% that can be used for whatever purpose is needed.
%%
%% Refer to clause 5.10 of the CDMI specification
%% for more information.
%% @end
%% ===================================================================
-module(cdmi_id).

-include("oneprovider_modules/control_panel/cdmi_id.hrl").
-include("oneprovider_modules/dao/dao_types.hrl").
-include_lib("eunit/include/eunit.hrl").

%% API
-export([uuid_to_objectid/1,objectid_to_uuid/1]).

-ifdef(TEST).
-compile([export_all]).
-endif.

%% uuid_to_objectid/1
%% ====================================================================
%% @doc Converts uuid to cdmi objectid format
-spec uuid_to_objectid(uuid()) -> binary() | {error,atom()}.
%% ====================================================================
uuid_to_objectid(Uuid) when is_list(Uuid) ->
    case build_objectid(Uuid) of
        {error,Error} -> {error, Error};
        Id -> list_to_binary(to_base16(Id))
    end.

%% objectid_to_uuid/1
%% ====================================================================
%% @doc Converts cdmi objectid format to uuid
-spec objectid_to_uuid(binary()) -> uuid() | {error,atom()}.
%% ====================================================================
objectid_to_uuid(ObjectId) when is_binary(ObjectId) ->
    case from_base16(binary_to_list(ObjectId)) of
        <<0:8, _Enum:24, 0:8, _Length:8, _Crc:16, Data/binary>> -> binary_to_list(Data);
        _Other -> {error, badarg}
    end.

%% ====================================================================
%% Internal functions
%% ====================================================================

%% build_objectid/1
%% ====================================================================
%% @doc Build an object ID based on our own enterprise number. Data is
%% expected to be whether a string or a binary.
%% @end
-spec build_objectid(Data::{string() | binary()}) -> binary() | {error, atom()}.
%% ====================================================================
build_objectid(Data) ->
    build_objectid(?ENTERPRISENUM, Data).

%% build_objectid/2
%% ====================================================================
%% @doc Build an object ID given an enterprise number and data as a
%% binary. We ensure here that the Data is not more than 32 bytes. The
%% object ID is composed of a number of fields:
%%
%% +----------+------------+-----------+--------+-------+-----------+
%% |     0    | 1 | 2 | 3  |     4     |   5    | 6 | 7 | 8 |..| 39 |
%% +----------+------------+-----------+--------+-------+-----------+
%% | Reserved | Enterprise | Reserverd | Length |  CRC  | Opaque    |
%% | (zero)   | Number     | (zero)    |        |       | Data      |
%% +----------+------------+-----------+--------+-------+-----------+
%% @end
-spec build_objectid(Enum::integer(), Data::string()|binary()) -> binary() | {error, atom()}.
%% ====================================================================
build_objectid(Enum, Data) when is_list(Data) ->
    build_objectid(Enum, list_to_binary(Data));
build_objectid(Enum, Data) when is_binary(Data) ->
    Length = size(Data),
    case (Length =< 32) of
        true ->
            Bin = <<0:8, Enum:24, 0:8, Length:8, 0:16, Data/binary>>,
            Crc = crc16(binary_to_list(Bin)),
            <<0:8, Enum:24, 0:8, Length:8, Crc:16, Data/binary>>;
        false ->
            {error, badarg}
    end.

%% to_base16/1
%% ====================================================================
%% @doc Convert an object ID to a Base16 encoded string.
-spec to_base16(Bin::binary()) -> string().
%% ====================================================================
to_base16(Bin) ->
    lists:flatten([io_lib:format("~2.16.0B", [X]) ||
        X <- binary_to_list(Bin)]).

%% from_base16/1
%% ====================================================================
%% @doc Convert an encoded object ID to its binary form.
-spec from_base16(Encoded::string()) -> binary().
%% ====================================================================
from_base16(Encoded) ->
    from_base16(Encoded, []).
from_base16([], Acc) ->
    list_to_binary(lists:reverse(Acc));
from_base16([X,Y | T], Acc) ->
    {ok, [V], []} = io_lib:fread("~16u", [X,Y]),
    from_base16(T, [V | Acc]).

%% crc16/1
%% ====================================================================
%% @doc Computes CRC sum for given string
%% ====================================================================
-spec crc16(Data::string()) -> integer().
crc16(Data) ->
    crc16(Data, 0).
crc16([], Crc) ->
    Crc;
crc16([Head | Data], Crc) ->
    NewCrc = lists:nth((Head bxor (Crc band 16#FF)) + 1,
        ?CRC16TABLE) bxor (Crc bsr 8),
    crc16(Data, NewCrc).
