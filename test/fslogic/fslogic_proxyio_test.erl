%%%--------------------------------------------------------------------
%%% @author Konrad Zemek
%%% @copyright (C) 2015 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%--------------------------------------------------------------------
%%% @doc
%%% Unit tests for fslogic_proxyio module.
%%% @end
%%%--------------------------------------------------------------------
-module(fslogic_proxyio_test).
-author("Konrad Zemek").

-ifdef(TEST).

-include("proto/oneclient/common_messages.hrl").
-include("proto/oneclient/proxyio_messages.hrl").
-include_lib("ctool/include/posix/errors.hrl").
-include_lib("eunit/include/eunit.hrl").


%%%===================================================================
%%% Test generators
%%%===================================================================


proxyio_successful_write_test_() ->
    {foreach,
        fun start/0,
        fun stop/1,
        [
            fun write_should_create_storage_file_manager_handle/1,
            fun write_should_open_file_with_write/1,
            fun write_should_write_data/1,
            fun successful_write_should_return_success/1,
            fun write_should_accept_partial_write/1
        ]}.


proxyio_successful_read_test_() ->
    {foreach,
        fun start/0,
        fun stop/1,
        [
            fun read_should_create_storage_file_manager_handle/1,
            fun read_should_open_file_with_read/1,
            fun read_should_read_data/1,
            fun successful_read_should_return_success/1,
            fun read_should_accept_partial_read/1
        ]}.


proxyio_failed_write_test_() ->
    {foreach,
        fun start/0,
        fun stop/1,
        [
            fun write_should_fail_on_failed_open/1,
            fun write_should_fail_on_failed_write/1
        ]}.


proxyio_failed_read_test_() ->
    {foreach,
        fun start/0,
        fun stop/1,
        [
            fun read_should_fail_on_failed_open/1,
            fun read_should_fail_on_failed_write/1
        ]}.


%%%===================================================================
%%% Test functions
%%%===================================================================


write_should_create_storage_file_manager_handle(_) ->
    fslogic_proxyio:write(<<"SessionID">>, <<"SpaceId">>, <<"StorageId">>,
        <<"FileId">>, 12, <<"Data">>),

    ?_assert(meck:called(storage_file_manager, new_handle,
        [<<"SessionID">>, <<"SpaceId">>, storage_mock, <<"FileId">>])).


write_should_open_file_with_write(_) ->
    fslogic_proxyio:write(<<"a">>, <<"b">>, <<"c">>, <<"d">>, 0, <<"e">>),
    ?_assert(meck:called(storage_file_manager, open, [sfm_handle_mock, write])).


write_should_write_data(_) ->
    fslogic_proxyio:write(<<"a">>, <<"b">>, <<"c">>, <<"d">>, 42, <<"Data">>),

    ?_assert(meck:called(storage_file_manager, write,
        [file_handle_mock, 42, <<"Data">>])).


successful_write_should_return_success(_) ->
    Data = <<0:(8 * 42)>>,

    ?_assertEqual(
        #proxyio_response{
            status = #status{code = ?OK},
            proxyio_response = #remote_write_result{wrote = byte_size(Data)}
        },
        fslogic_proxyio:write(<<"a">>, <<"b">>, <<"c">>, <<"d">>, 42, Data)
    ).


read_should_create_storage_file_manager_handle(_) ->
    fslogic_proxyio:read(<<"SessionID">>, <<"SpaceId">>, <<"StorageId">>,
        <<"FileId">>, 12, 33),

    ?_assert(meck:called(storage_file_manager, new_handle,
        [<<"SessionID">>, <<"SpaceId">>, storage_mock, <<"FileId">>])).


read_should_open_file_with_read(_) ->
    fslogic_proxyio:read(<<"a">>, <<"b">>, <<"c">>, <<"d">>, 0, 132),
    ?_assert(meck:called(storage_file_manager, open, [sfm_handle_mock, read])).


read_should_read_data(_) ->
    fslogic_proxyio:read(<<"a">>, <<"b">>, <<"c">>, <<"d">>, 42, 64),

    ?_assert(meck:called(storage_file_manager, read,
        [file_handle_mock, 42, 64])).


successful_read_should_return_success(_) ->
    ?_assertMatch(
        #proxyio_response{
            status = #status{code = ?OK},
            proxyio_response = #remote_data{data = <<0:(8 * 12)>>}
        },
        fslogic_proxyio:read(<<"a">>, <<"b">>, <<"c">>, <<"d">>, 42, 12)
    ).


write_should_accept_partial_write(_) ->
    meck:delete(storage_file_manager, write, 3),
    meck:expect(storage_file_manager, write, 3, {ok, 1}),

    ?_assertEqual(
        #proxyio_response{
            status = #status{code = ?OK},
            proxyio_response = #remote_write_result{wrote = 1}
        },
        fslogic_proxyio:write(<<"a">>, <<"b">>, <<"c">>, <<"d">>, 4, <<"data">>)
    ).


read_should_accept_partial_read(_) ->
    meck:delete(storage_file_manager, read, 3),
    meck:expect(storage_file_manager, read, 3, {ok, <<"da">>}),

    ?_assertEqual(
        #proxyio_response{
            status = #status{code = ?OK},
            proxyio_response = #remote_data{data = <<"da">>}
        },
        fslogic_proxyio:read(<<"a">>, <<"b">>, <<"c">>, <<"d">>, 42, 1000)
    ).


write_should_fail_on_failed_open(_) ->
    meck:delete(storage_file_manager, open, 2),
    meck:expect(storage_file_manager, open, 2, {error, enetdown}),

    ?_assertEqual(
        #proxyio_response{status = #status{code = ?ENETDOWN}},
        fslogic_proxyio:write(<<"a">>, <<"b">>, <<"c">>, <<"d">>, 42, <<"hi">>)
    ).


write_should_fail_on_failed_write(_) ->
    meck:delete(storage_file_manager, write, 3),
    meck:expect(storage_file_manager, write, 3, {error, emfile}),

    ?_assertEqual(
        #proxyio_response{status = #status{code = ?EMFILE}},
        fslogic_proxyio:write(<<"a">>, <<"b">>, <<"c">>, <<"d">>, 42, <<"hi">>)
    ).


read_should_fail_on_failed_open(_) ->
    meck:delete(storage_file_manager, open, 2),
    meck:expect(storage_file_manager, open, 2, {error, enotconn}),

    ?_assertEqual(
        #proxyio_response{status = #status{code = ?ENOTCONN}},
        fslogic_proxyio:read(<<"a">>, <<"b">>, <<"c">>, <<"d">>, 42, 10)
    ).


read_should_fail_on_failed_write(_) ->
    meck:delete(storage_file_manager, read, 3),
    meck:expect(storage_file_manager, read, 3, {error, enospc}),

    ?_assertEqual(
        #proxyio_response{status = #status{code = ?ENOSPC}},
        fslogic_proxyio:read(<<"a">>, <<"b">>, <<"c">>, <<"d">>, 42, 5)
    ).


%%%===================================================================
%%% Test fixtures
%%%===================================================================


start() ->
    meck:new([storage_file_manager, storage]),

    meck:expect(storage_file_manager, new_handle, 4, sfm_handle_mock),
    meck:expect(storage_file_manager, open, 2, {ok, file_handle_mock}),

    meck:expect(storage_file_manager, write, 3,
        fun(_, _, Data) -> {ok, byte_size(Data)} end),

    meck:expect(storage_file_manager, read, 3,
        fun(_, _, Size) -> {ok, <<0:(8 * Size)>>} end),

    meck:expect(storage, get, 1, {ok, storage_mock}),

    ok.


stop(_) ->
    ?assert(meck:validate([storage_file_manager, storage])),
    meck:unload().


-endif.
