%%%--------------------------------------------------------------------
%%% @author Tomasz Lichon
%%% @copyright (C) 2015 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%--------------------------------------------------------------------
%%% @doc Support for CDMI Object ID (taken from https://github.com/jeastman/crime)
%%%
%%% Orginal description:
%%% Object IDs are used to identify objects in CDMI. Object IDs are intended
%%% to be globally unique values that have a specific structure. The native
%%% format of an object ID is a variable length byte sequence with a maximum
%%% size of 40 bytes. This leaves an implementer up to 32 bytes for data
%%% that can be used for whatever purpose is needed.
%%%
%%% Refer to clause 5.10 of the CDMI specification
%%% for more information.
%%% @end
%%%--------------------------------------------------------------------
-module(cdmi_id).

-include_lib("ctool/include/logging.hrl").
-include_lib("eunit/include/eunit.hrl").

%% API
-export([uuid_to_objectid/1, objectid_to_uuid/1]).

%% test API
-export([crc16/1, build_objectid/1, build_objectid/2, to_base16/1, from_base16/1]).

%% The SNMP Enterprise Number for your organization in network byte
%% order. See RFC 2578 and
%% http://www.iana.org/assignments/enterprise-numbers
%%
%% This reference implementation uses a value of 0.
-define(ENTERPRISENUM, 0).

%% Support for CDMI CRC-16 for Object IDs.
%%
%% The CDMI specification requires a CRC field as part of the object
%% ID. The CRC algorithim specified has the following parameters:
%% -- Name: "CRC-16"
%% -- Width: 16
%% -- Poly: 0x8005
%% -- Init: 0x0000
%% -- RefIn: True
%% -- RefOut: True
%% -- XorOut: 0x0000
%% -- Check: 0xBB3D
-define(CRC16TABLE,
    [   16#0,    16#C0C1, 16#C181, 16#140,  16#C301, 16#3C0,  16#280,  16#C241,
        16#C601, 16#6C0,  16#780,  16#C741, 16#500,  16#C5C1, 16#C481, 16#440,
        16#CC01, 16#CC0,  16#D80,  16#CD41, 16#F00,  16#CFC1, 16#CE81, 16#E40,
        16#A00,  16#CAC1, 16#CB81, 16#B40,  16#C901, 16#9C0,  16#880,  16#C841,
        16#D801, 16#18C0, 16#1980, 16#D941, 16#1B00, 16#DBC1, 16#DA81, 16#1A40,
        16#1E00, 16#DEC1, 16#DF81, 16#1F40, 16#DD01, 16#1DC0, 16#1C80, 16#DC41,
        16#1400, 16#D4C1, 16#D581, 16#1540, 16#D701, 16#17C0, 16#1680, 16#D641,
        16#D201, 16#12C0, 16#1380, 16#D341, 16#1100, 16#D1C1, 16#D081, 16#1040,
        16#F001, 16#30C0, 16#3180, 16#F141, 16#3300, 16#F3C1, 16#F281, 16#3240,
        16#3600, 16#F6C1, 16#F781, 16#3740, 16#F501, 16#35C0, 16#3480, 16#F441,
        16#3C00, 16#FCC1, 16#FD81, 16#3D40, 16#FF01, 16#3FC0, 16#3E80, 16#FE41,
        16#FA01, 16#3AC0, 16#3B80, 16#FB41, 16#3900, 16#F9C1, 16#F881, 16#3840,
        16#2800, 16#E8C1, 16#E981, 16#2940, 16#EB01, 16#2BC0, 16#2A80, 16#EA41,
        16#EE01, 16#2EC0, 16#2F80, 16#EF41, 16#2D00, 16#EDC1, 16#EC81, 16#2C40,
        16#E401, 16#24C0, 16#2580, 16#E541, 16#2700, 16#E7C1, 16#E681, 16#2640,
        16#2200, 16#E2C1, 16#E381, 16#2340, 16#E101, 16#21C0, 16#2080, 16#E041,
        16#A001, 16#60C0, 16#6180, 16#A141, 16#6300, 16#A3C1, 16#A281, 16#6240,
        16#6600, 16#A6C1, 16#A781, 16#6740, 16#A501, 16#65C0, 16#6480, 16#A441,
        16#6C00, 16#ACC1, 16#AD81, 16#6D40, 16#AF01, 16#6FC0, 16#6E80, 16#AE41,
        16#AA01, 16#6AC0, 16#6B80, 16#AB41, 16#6900, 16#A9C1, 16#A881, 16#6840,
        16#7800, 16#B8C1, 16#B981, 16#7940, 16#BB01, 16#7BC0, 16#7A80, 16#BA41,
        16#BE01, 16#7EC0, 16#7F80, 16#BF41, 16#7D00, 16#BDC1, 16#BC81, 16#7C40,
        16#B401, 16#74C0, 16#7580, 16#B541, 16#7700, 16#B7C1, 16#B681, 16#7640,
        16#7200, 16#B2C1, 16#B381, 16#7340, 16#B101, 16#71C0, 16#7080, 16#B041,
        16#5000, 16#90C1, 16#9181, 16#5140, 16#9301, 16#53C0, 16#5280, 16#9241,
        16#9601, 16#56C0, 16#5780, 16#9741, 16#5500, 16#95C1, 16#9481, 16#5440,
        16#9C01, 16#5CC0, 16#5D80, 16#9D41, 16#5F00, 16#9FC1, 16#9E81, 16#5E40,
        16#5A00, 16#9AC1, 16#9B81, 16#5B40, 16#9901, 16#59C0, 16#5880, 16#9841,
        16#8801, 16#48C0, 16#4980, 16#8941, 16#4B00, 16#8BC1, 16#8A81, 16#4A40,
        16#4E00, 16#8EC1, 16#8F81, 16#4F40, 16#8D01, 16#4DC0, 16#4C80, 16#8C41,
        16#4400, 16#84C1, 16#8581, 16#4540, 16#8701, 16#47C0, 16#4680, 16#8641,
        16#8201, 16#42C0, 16#4380, 16#8341, 16#4100, 16#81C1, 16#8081, 16#4040]).

%%%===================================================================
%%% API
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc Converts uuid to cdmi objectid format
%%--------------------------------------------------------------------
-spec uuid_to_objectid(onedata_file_api:file_guid()) -> {ok, binary()} | {error, atom()}.
uuid_to_objectid(Uuid) ->
    case build_objectid(http_utils:base64url_decode(Uuid)) of
        {error, Error} -> {error, Error};
        Id -> {ok, to_base16(Id)}
    end.

%%--------------------------------------------------------------------
%% @doc Converts cdmi objectid format to uuid
%%--------------------------------------------------------------------
-spec objectid_to_uuid(binary()) -> {ok, onedata_file_api:file_guid()} | {error, atom()}.
objectid_to_uuid(ObjectId) ->
    case from_base16(ObjectId) of
        <<0:8, _Enum:24, 0:8, _Length:8, _Crc:16, Data/binary>> ->
            {ok, http_utils:base64url_encode(Data)};
        _Other -> {error, badarg}
    end.

%%%===================================================================
%%% Internal functions
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc Build an object ID based on our own enterprise number. Data is
%% expected to be whether a string or a binary.
%% @end
%%--------------------------------------------------------------------
-spec build_objectid(Data :: binary()) -> binary() | {error, atom()}.
build_objectid(Data) ->
    build_objectid(?ENTERPRISENUM, Data).

%%--------------------------------------------------------------------
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
%%--------------------------------------------------------------------
-spec build_objectid(Enum :: integer(), Data :: binary()) -> binary() | {error, atom()}.
build_objectid(Enum, Data) when is_binary(Data) ->
    Length = size(Data),
    case (Length =< 320) of
        true ->
            Bin = <<0:8, Enum:24, 0:8, Length:8, 0:16, Data/binary>>,
            Crc = crc16(binary_to_list(Bin)),
            <<0:8, Enum:24, 0:8, Length:8, Crc:16, Data/binary>>;
        false ->
            {error, badarg}
    end.

%%--------------------------------------------------------------------
%% @doc Convert an object ID to a Base16 encoded string.
%%--------------------------------------------------------------------
-spec to_base16(Bin :: binary()) -> binary().
to_base16(Bin) ->
    list_to_binary(
        lists:flatten([io_lib:format("~2.16.0B", [X]) ||
            X <- binary_to_list(Bin)])
    ).

%%--------------------------------------------------------------------
%% @doc Convert an encoded object ID to its binary form.
%%--------------------------------------------------------------------
-spec from_base16(Encoded :: binary()) -> binary().
from_base16(Encoded) ->
    from_base16(Encoded, <<"">>).
from_base16(<<"">>, Acc) ->
    str_utils:reverse_binary(Acc);
from_base16(<<X, Y, Rest/binary>>, Acc) ->
    {ok, [V], []} = io_lib:fread("~16u", [X, Y]),
    from_base16(Rest, <<V, Acc/binary>>).

%%--------------------------------------------------------------------
%% @doc Computes CRC sum for given string
%%--------------------------------------------------------------------
-spec crc16(Data :: string()) -> integer().
crc16(Data) ->
    crc16(Data, 0).
crc16([], Crc) ->
    Crc;
crc16([Head | Data], Crc) ->
    NewCrc = lists:nth((Head bxor (Crc band 16#FF)) + 1,
        ?CRC16TABLE) bxor (Crc bsr 8),
    crc16(Data, NewCrc).
