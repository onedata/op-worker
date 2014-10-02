%% ===================================================================
%% @author Tomasz Lichon
%% @copyright (C): 2014 ACK CYFRONET AGH
%% This software is released under the MIT license
%% cited in 'LICENSE.txt'.
%% @end
%% ===================================================================
%% @doc This file tests cdmi functions that are used to encode and decode objectid
%% ===================================================================
-module(cdmi_id_test).

-include_lib("eunit/include/eunit.hrl").
-include("oneprovider_modules/control_panel/cdmi_id.hrl").

%% API
-export([crc_test/0,build_with_enum_test/0,build_without_enum_test/0,build_with_badarg_test/0,build_with_badarg2_test/0,base16_test/0]).

crc_test() ->
    Expected = 40679,
    ?assertEqual(Expected, cdmi_id:crc16("test string")).

build_with_enum_test() ->
    TestString = "data string",
    TestNum = 96,
    Crc = 27447,
    Length = length(TestString),
    Obj = cdmi_id:build_objectid(TestNum, TestString),
    CmpString = list_to_binary(TestString),
    ?assertEqual(Obj, <<0:8, TestNum:24,
    0:8, Length:8, Crc:16, CmpString/binary>>).

build_without_enum_test() ->
    TestString = "data string",
    Crc = 17183,
    Length = length(TestString),
    Obj = cdmi_id:build_objectid(TestString),
    CmpString = list_to_binary(TestString),
    ?assertEqual(Obj, <<0:8, ?ENTERPRISENUM:24,
    0:8, Length:8, Crc:16, CmpString/binary>>).

build_with_badarg_test() ->
    TooLong = "12345678901234567890123456789012345",
    ?assertEqual({error, badarg},
        cdmi_id:build_objectid(TooLong)).

build_with_badarg2_test() ->
    TooLong = "12345678901234567890123456789012345",
    ?assertEqual({error, badarg},
        cdmi_id:build_objectid(?ENTERPRISENUM, TooLong)).

base16_test() ->
    TestString = "data string",
    Obj = cdmi_id:build_objectid(TestString),
    Encode = cdmi_id:to_base16(Obj),
    ?assertEqual(Obj, cdmi_id:from_base16(Encode)).

uuid_to_objectid_test() ->
    Uuid = "123456789123456",
    ObjectId = cdmi_id:uuid_to_objectid(Uuid),
    ?assertEqual(Uuid, cdmi_id:objectid_to_uuid(ObjectId)).
