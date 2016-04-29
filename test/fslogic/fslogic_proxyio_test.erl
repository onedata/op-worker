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

-include("modules/fslogic/fslogic_common.hrl").
-include("proto/oneclient/common_messages.hrl").
-include("proto/oneclient/proxyio_messages.hrl").
-include("modules/datastore/datastore_specific_models_def.hrl").
-include_lib("cluster_worker/include/modules/datastore/datastore_models_def.hrl").
-include_lib("ctool/include/posix/errors.hrl").
-include_lib("eunit/include/eunit.hrl").

-define(SPACE_ID, <<"SpaceId">>).
-define(BS(Offset, Data), [#byte_sequence{offset = Offset, data = Data}]).

%%%===================================================================
%%% Test generators
%%%===================================================================

proxyio_successful_write_test_() ->
    {foreach,
        fun start/0,
        fun stop/1,
        [
            fun write_should_get_session_when_given_handle_id/1,
            fun write_should_get_session_when_given_file_uuid/1,
            fun write_should_not_get_space_id_when_given_handle_id/1,
            fun write_should_get_space_id_when_given_file_uuid/1,
            fun write_should_not_create_storage_file_manager_handle_when_given_handle_id/1,
            fun write_should_create_storage_file_manager_handle_when_given_file_uuid/1,
            fun write_should_not_open_file_when_given_handle_id/1,
            fun write_should_open_file_with_write_when_given_file_uuid/1,
            fun write_should_write_data_when_given_handle_id/1,
            fun write_should_write_data_when_given_file_uuid/1,
            fun successful_write_should_return_success_when_given_handle_id/1,
            fun successful_write_should_return_success_when_given_file_uuid/1,
            fun write_should_handle_partial_writes_when_given_handle_id/1,
            fun write_should_handle_partial_writes_when_given_file_uuid/1
        ]}.


proxyio_successful_read_test_() ->
    {foreach,
        fun start/0,
        fun stop/1,
        [
            fun read_should_get_session_when_given_handle_id/1,
            fun read_should_get_session_when_given_file_uuid/1,
            fun read_should_not_get_space_id_when_given_handle_id/1,
            fun read_should_get_space_id_when_given_file_uuid/1,
            fun read_should_not_create_storage_file_manager_handle_when_given_handle_id/1,
            fun read_should_create_storage_file_manager_handle_when_given_file_uuid/1,
            fun read_should_not_open_file_when_given_handle_id/1,
            fun read_should_open_file_with_read_when_given_file_uuid/1,
            fun read_should_read_data_when_given_handle_id/1,
            fun read_should_read_data_when_given_file_uuid/1,
            fun successful_read_should_return_success_when_given_handle_id/1,
            fun successful_read_should_return_success_when_given_file_uuid/1,
            fun read_should_accept_partial_read_when_given_handle_id/1,
            fun read_should_accept_partial_read_when_given_file_uuid/1
        ]}.


proxyio_failed_write_test_() ->
    {foreach,
        fun start/0,
        fun stop/1,
        [
            fun write_should_fail_on_failed_open_when_given_file_uuid/1,
            fun write_should_fail_on_failed_write_when_given_handle_id/1,
            fun write_should_fail_on_failed_write_when_given_file_uuid/1
        ]}.


proxyio_failed_read_test_() ->
    {foreach,
        fun start/0,
        fun stop/1,
        [
            fun read_should_fail_on_failed_open_when_given_file_uuid/1,
            fun read_should_fail_on_failed_read_when_given_handle_id/1,
            fun read_should_fail_on_failed_read_when_given_file_uuid/1
        ]}.

%%%===================================================================
%%% Test functions
%%%===================================================================

write_should_get_session_when_given_handle_id(_) ->
    fslogic_proxyio:write(<<"SessionId">>, #{?PROXYIO_PARAMETER_HANDLE_ID => <<"HandleId">>},
        <<"c">>, <<"d">>, ?BS(0, <<"f">>)),

    ?_assert(meck:called(session, get, [<<"SessionId">>])).


write_should_get_session_when_given_file_uuid(_) ->
    fslogic_proxyio:write(<<"SessionId">>, #{?PROXYIO_PARAMETER_FILE_UUID => <<"b">>},
        <<"c">>, <<"d">>, ?BS(0, <<"f">>)),

    ?_assert(meck:called(session, get, [<<"SessionId">>])).

write_should_not_get_space_id_when_given_handle_id(_) ->
    fslogic_proxyio:write(<<"a">>, #{?PROXYIO_PARAMETER_HANDLE_ID => <<"HandleId">>},
        <<"c">>, <<"d">>, ?BS(0, <<"f">>)),

    ?_assertEqual(0, meck:num_calls(fslogic_spaces, get_space, 2)).


write_should_get_space_id_when_given_file_uuid(_) ->
    fslogic_proxyio:write(<<"a">>, #{?PROXYIO_PARAMETER_FILE_UUID => <<"FileUuid">>},
        <<"c">>, <<"d">>, ?BS(0, <<"f">>)),

    ?_assert(meck:called(fslogic_spaces, get_space,
        [{uuid, <<"FileUuid">>}, <<"UserId">>])).


write_should_not_create_storage_file_manager_handle_when_given_handle_id(_) ->
    fslogic_proxyio:write(<<"SessionID">>, #{?PROXYIO_PARAMETER_HANDLE_ID => <<"HandleId">>},
        <<"StorageId">>, <<"FileId">>, ?BS(12, <<"Data">>)),

    ?_assertEqual(0, meck:num_calls(storage_file_manager, new_handle, 5)).


write_should_create_storage_file_manager_handle_when_given_file_uuid(_) ->
    fslogic_proxyio:write(<<"SessionID">>, #{?PROXYIO_PARAMETER_FILE_UUID => <<"FileUuid">>},
        <<"StorageId">>, <<"FileId">>, ?BS(12, <<"Data">>)),

    ?_assert(meck:called(storage_file_manager, new_handle,
        [<<"SessionID">>, ?SPACE_ID, <<"FileUuid">>, storage_mock, <<"FileId">>])).


write_should_not_open_file_when_given_handle_id(_) ->
    fslogic_proxyio:write(<<"a">>, #{?PROXYIO_PARAMETER_HANDLE_ID => <<"HandleId">>},
        <<"c">>, <<"d">>, ?BS(0, <<"e">>)),

    ?_assertEqual(0, meck:num_calls(storage_file_manager, open, 2)).


write_should_open_file_with_write_when_given_file_uuid(_) ->
    fslogic_proxyio:write(<<"a">>, #{?PROXYIO_PARAMETER_FILE_UUID => <<"b">>}, <<"c">>,
        <<"d">>, ?BS(0, <<"e">>)),

    ?_assert(meck:called(storage_file_manager, open, [sfm_handle_mock, write])).


write_should_write_data_when_given_handle_id(_) ->
    fslogic_proxyio:write(<<"a">>, #{?PROXYIO_PARAMETER_HANDLE_ID => <<"HandleId">>},
        <<"c">>, <<"d">>, ?BS(42, <<"Data">>)),

    ?_assert(meck:called(storage_file_manager, write,
        [file_handle_mock, 42, <<"Data">>])).


write_should_write_data_when_given_file_uuid(_) ->
    fslogic_proxyio:write(<<"a">>, #{?PROXYIO_PARAMETER_FILE_UUID => <<"b">>}, <<"c">>,
        <<"d">>, ?BS(42, <<"Data">>)),

    ?_assert(meck:called(storage_file_manager, write,
        [file_handle_mock, 42, <<"Data">>])).


successful_write_should_return_success_when_given_handle_id(_) ->
    Data = <<0:(8 * 42)>>,

    ?_assertEqual(
        #proxyio_response{
            status = #status{code = ?OK},
            proxyio_response = #remote_write_result{wrote = byte_size(Data)}
        },
        fslogic_proxyio:write(<<"a">>, #{?PROXYIO_PARAMETER_HANDLE_ID => <<"HandleId">>},
            <<"c">>, <<"d">>, ?BS(42, Data))
    ).


successful_write_should_return_success_when_given_file_uuid(_) ->
    Data = <<0:(8 * 42)>>,

    ?_assertEqual(
        #proxyio_response{
            status = #status{code = ?OK},
            proxyio_response = #remote_write_result{wrote = byte_size(Data)}
        },
        fslogic_proxyio:write(<<"a">>, #{?PROXYIO_PARAMETER_FILE_UUID => <<"b">>}, <<"c">>,
            <<"d">>, ?BS(42, Data))
    ).


read_should_get_session_when_given_handle_id(_) ->
    fslogic_proxyio:read(<<"SessionId">>, #{?PROXYIO_PARAMETER_HANDLE_ID => <<"HandleId">>},
        <<"c">>, <<"d">>, 0, 5),

    ?_assert(meck:called(session, get, [<<"SessionId">>])).


read_should_get_session_when_given_file_uuid(_) ->
    fslogic_proxyio:read(<<"SessionId">>, #{?PROXYIO_PARAMETER_FILE_UUID => <<"b">>},
        <<"c">>, <<"d">>, 0, 5),

    ?_assert(meck:called(session, get, [<<"SessionId">>])).


read_should_not_get_space_id_when_given_handle_id(_) ->
    fslogic_proxyio:read(<<"a">>, #{?PROXYIO_PARAMETER_HANDLE_ID => <<"HandleId">>}, <<"c">>,
        <<"d">>, 0, 10),

    ?_assertEqual(0, meck:num_calls(fslogic_spaces, get_space, 2)).


read_should_get_space_id_when_given_file_uuid(_) ->
    fslogic_proxyio:read(<<"a">>, #{?PROXYIO_PARAMETER_FILE_UUID => <<"FileUuid">>}, <<"c">>,
        <<"d">>, 0, 10),

    ?_assert(meck:called(fslogic_spaces, get_space,
        [{uuid, <<"FileUuid">>}, <<"UserId">>])).


read_should_not_create_storage_file_manager_handle_when_given_handle_id(_) ->
    fslogic_proxyio:read(<<"SessionID">>, #{?PROXYIO_PARAMETER_HANDLE_ID => <<"HandleId">>},
        <<"StorageId">>, <<"FileId">>, 12, 33),

    ?_assertEqual(0, meck:num_calls(storage_file_manager, new_handle, 5)).


read_should_create_storage_file_manager_handle_when_given_file_uuid(_) ->
    fslogic_proxyio:read(<<"SessionID">>, #{?PROXYIO_PARAMETER_FILE_UUID => <<"FileUuid">>},
        <<"StorageId">>, <<"FileId">>, 12, 33),

    ?_assert(meck:called(storage_file_manager, new_handle,
        [<<"SessionID">>, ?SPACE_ID, <<"FileUuid">>, storage_mock, <<"FileId">>])).


read_should_not_open_file_when_given_handle_id(_) ->
    fslogic_proxyio:read(<<"a">>, #{?PROXYIO_PARAMETER_HANDLE_ID => <<"HandleId">>}, <<"c">>,
        <<"d">>, 0, 132),

    ?_assertEqual(0, meck:num_calls(storage_file_manager, open, 2)).


read_should_open_file_with_read_when_given_file_uuid(_) ->
    fslogic_proxyio:read(<<"a">>, #{?PROXYIO_PARAMETER_FILE_UUID => <<"b">>}, <<"c">>,
        <<"d">>, 0, 132),

    ?_assert(meck:called(storage_file_manager, open, [sfm_handle_mock, read])).


read_should_read_data_when_given_handle_id(_) ->
    fslogic_proxyio:read(<<"a">>, #{?PROXYIO_PARAMETER_HANDLE_ID => <<"HandleId">>}, <<"c">>,
        <<"d">>, 42, 64),

    ?_assert(meck:called(storage_file_manager, read,
        [file_handle_mock, 42, 64])).


read_should_read_data_when_given_file_uuid(_) ->
    fslogic_proxyio:read(<<"a">>, #{?PROXYIO_PARAMETER_FILE_UUID => <<"b">>}, <<"c">>,
        <<"d">>, 42, 64),

    ?_assert(meck:called(storage_file_manager, read,
        [file_handle_mock, 42, 64])).


successful_read_should_return_success_when_given_handle_id(_) ->
    ?_assertMatch(
        #proxyio_response{
            status = #status{code = ?OK},
            proxyio_response = #remote_data{data = <<0:(8 * 12)>>}
        },
        fslogic_proxyio:read(<<"a">>, #{?PROXYIO_PARAMETER_HANDLE_ID => <<"HandleId">>},
            <<"c">>, <<"d">>, 42, 12)
    ).


successful_read_should_return_success_when_given_file_uuid(_) ->
    ?_assertMatch(
        #proxyio_response{
            status = #status{code = ?OK},
            proxyio_response = #remote_data{data = <<0:(8 * 12)>>}
        },
        fslogic_proxyio:read(<<"a">>, #{?PROXYIO_PARAMETER_FILE_UUID => <<"b">>}, <<"c">>,
            <<"d">>, 42, 12)
    ).


write_should_handle_partial_writes_when_given_handle_id(_) ->
    meck:delete(storage_file_manager, write, 3),
    meck:expect(storage_file_manager, write, 3, {ok, 1}),

    [
      ?_assertEqual(
          #proxyio_response{
              status = #status{code = ?OK},
              proxyio_response = #remote_write_result{wrote = 4}
          },
          fslogic_proxyio:write(<<"a">>, #{?PROXYIO_PARAMETER_HANDLE_ID => <<"HandleId">>},
              <<"c">>, <<"d">>, ?BS(4, <<"data">>))
      ),
      ?_assertEqual(4, meck:num_calls(storage_file_manager, write, 3))
    ].


write_should_handle_partial_writes_when_given_file_uuid(_) ->
    meck:delete(storage_file_manager, write, 3),
    meck:expect(storage_file_manager, write, 3, {ok, 1}),

    [
      ?_assertEqual(
          #proxyio_response{
              status = #status{code = ?OK},
              proxyio_response = #remote_write_result{wrote = 4}
          },
          fslogic_proxyio:write(<<"a">>, #{?PROXYIO_PARAMETER_FILE_UUID => <<"b">>}, <<"c">>,
              <<"d">>, ?BS(4, <<"data">>))
      ),
      ?_assertEqual(4, meck:num_calls(storage_file_manager, write, 3))
    ].


read_should_accept_partial_read_when_given_handle_id(_) ->
    meck:delete(storage_file_manager, read, 3),
    meck:expect(storage_file_manager, read, 3, {ok, <<"da">>}),

    ?_assertEqual(
        #proxyio_response{
            status = #status{code = ?OK},
            proxyio_response = #remote_data{data = <<"da">>}
        },
        fslogic_proxyio:read(<<"a">>, #{?PROXYIO_PARAMETER_HANDLE_ID => <<"HandleId">>},
            <<"c">>, <<"d">>, 42, 1000)
    ).


read_should_accept_partial_read_when_given_file_uuid(_) ->
    meck:delete(storage_file_manager, read, 3),
    meck:expect(storage_file_manager, read, 3, {ok, <<"da">>}),

    ?_assertEqual(
        #proxyio_response{
            status = #status{code = ?OK},
            proxyio_response = #remote_data{data = <<"da">>}
        },
        fslogic_proxyio:read(<<"a">>, #{?PROXYIO_PARAMETER_FILE_UUID => <<"b">>}, <<"c">>,
            <<"d">>, 42, 1000)
    ).


write_should_fail_on_failed_open_when_given_file_uuid(_) ->
    meck:delete(storage_file_manager, open, 2),
    meck:expect(storage_file_manager, open, 2, {error, enetdown}),

    ?_assertEqual(
        #proxyio_response{status = #status{code = ?ENETDOWN}},
        fslogic_proxyio:write(<<"a">>, #{?PROXYIO_PARAMETER_FILE_UUID => <<"b">>}, <<"c">>,
            <<"d">>, ?BS(42, <<"hi">>))
    ).


write_should_fail_on_failed_write_when_given_handle_id(_) ->
    meck:delete(storage_file_manager, write, 3),
    meck:expect(storage_file_manager, write, 3, {error, emfile}),

    ?_assertEqual(
        #proxyio_response{status = #status{code = ?EMFILE}},
        fslogic_proxyio:write(<<"a">>, #{?PROXYIO_PARAMETER_HANDLE_ID => <<"HandleId">>},
            <<"c">>, <<"d">>, ?BS(42, <<"hi">>))
    ).


write_should_fail_on_failed_write_when_given_file_uuid(_) ->
    meck:delete(storage_file_manager, write, 3),
    meck:expect(storage_file_manager, write, 3, {error, emfile}),

    ?_assertEqual(
        #proxyio_response{status = #status{code = ?EMFILE}},
        fslogic_proxyio:write(<<"a">>, #{?PROXYIO_PARAMETER_FILE_UUID => <<"b">>}, <<"c">>,
            <<"d">>, ?BS(42, <<"hi">>))
    ).


read_should_fail_on_failed_open_when_given_file_uuid(_) ->
    meck:delete(storage_file_manager, open, 2),
    meck:expect(storage_file_manager, open, 2, {error, enotconn}),

    ?_assertEqual(
        #proxyio_response{status = #status{code = ?ENOTCONN}},
        fslogic_proxyio:read(<<"a">>, #{?PROXYIO_PARAMETER_FILE_UUID => <<"b">>}, <<"c">>,
            <<"d">>, 42, 10)
    ).


read_should_fail_on_failed_read_when_given_handle_id(_) ->
    meck:delete(storage_file_manager, read, 3),
    meck:expect(storage_file_manager, read, 3, {error, enospc}),

    ?_assertEqual(
        #proxyio_response{status = #status{code = ?ENOSPC}},
        fslogic_proxyio:read(<<"a">>, #{?PROXYIO_PARAMETER_HANDLE_ID => <<"HandleId">>},
            <<"c">>, <<"d">>, 42, 5)
    ).


read_should_fail_on_failed_read_when_given_file_uuid(_) ->
    meck:delete(storage_file_manager, read, 3),
    meck:expect(storage_file_manager, read, 3, {error, enospc}),

    ?_assertEqual(
        #proxyio_response{status = #status{code = ?ENOSPC}},
        fslogic_proxyio:read(<<"a">>, #{?PROXYIO_PARAMETER_FILE_UUID => <<"b">>}, <<"c">>,
            <<"d">>, 42, 5)
    ).

%%%===================================================================
%%% Test fixtures
%%%===================================================================

start() ->
    meck:new([storage_file_manager, storage, fslogic_spaces, session]),

    meck:expect(storage_file_manager, new_handle, 5, sfm_handle_mock),
    meck:expect(storage_file_manager, open, 2, {ok, file_handle_mock}),

    meck:expect(storage_file_manager, write, 3,
        fun(_, _, Data) -> {ok, byte_size(Data)} end),

    meck:expect(storage_file_manager, read, 3,
        fun(_, _, Size) -> {ok, <<0:(8 * Size)>>} end),

    meck:expect(storage, get, 1, {ok, storage_mock}),

    meck:expect(session, get, 1, {ok,
        #document{value = #session{
            identity = #identity{user_id = <<"UserId">>},
            handles = #{<<"HandleId">> => file_handle_mock}}}}),

    meck:expect(fslogic_spaces, get_space, 2, {ok, #document{key = ?SPACE_ID}}),

    ok.


stop(_) ->
    ?assert(meck:validate([storage_file_manager, storage, fslogic_spaces, session])),
    meck:unload().


-endif.
