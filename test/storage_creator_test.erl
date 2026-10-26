%%%-------------------------------------------------------------------
%%% @author Bartosz Walkowicz
%%% @copyright (C) 2026 Onedata (onedata.org)
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% Unit tests of the storage create's handling of its own failures: the spec is
%%% printed to the log by its type, and a failure there must end as the rest of
%%% the create's failures do, in an error returned to the caller; and a refusal,
%%% which is thrown, must leave the same line in the log as a returned error.
%%% @end
%%%-------------------------------------------------------------------
-module(storage_creator_test).
-author("Bartosz Walkowicz").

-include_lib("ctool/include/errors.hrl").
-include_lib("opw_panel_contracts/include/storage/common.hrl").
-include_lib("opw_panel_contracts/include/storage/posix.hrl").
-include_lib("eunit/include/eunit.hrl").


%%%===================================================================
%%% Tests
%%%===================================================================


%% A type that no helper spec module serves makes the print raise.
unprintable_spec_fails_the_create_with_an_error_test_() ->
    {setup, fun mock_logger/0, fun unmock_logger/1, fun() ->
        Spec = #storage_create_spec{
            type = <<"unknown">>,
            name = <<"some-storage">>,
            luma = #luma_spec{feed = auto},
            credentials = #posix_helper_credentials{uid = 0}
        },

        ?assertMatch(?ERR_INTERNAL_SERVER_ERROR(_, _), storage_creator:create(Spec))
    end}.


%% A refusal is thrown, not returned, and must leave the same line in the log
%% as a returned error does.
refused_create_is_logged_test_() ->
    {setup, fun mock_refusing_path_type_check/0, fun unmock_refusing_path_type_check/1, fun() ->
        Spec = #storage_create_spec{
            type = <<"posix">>,
            name = <<"some-storage">>,
            luma = #luma_spec{feed = auto},
            credentials = #posix_helper_credentials{uid = 0}
        },

        ?assertMatch(
            ?ERR_BAD_VALUE_NOT_ALLOWED(_, <<"storagePathType">>, _),
            storage_creator:create(Spec)
        ),
        ?assertEqual(1, count_error_logs_containing(<<"Failed to add storage 'some-storage'">>))
    end}.


%%%===================================================================
%%% Internal functions
%%%===================================================================


%% @private
mock_logger() ->
    meck:new(onedata_logger, [passthrough, no_link]),
    meck:expect(onedata_logger, should_log, fun(_Loglevel) -> true end),
    meck:expect(onedata_logger, log, fun(_Loglevel, _Metadata, _FormattedLog) -> ok end).


%% @private
unmock_logger(_) ->
    meck:unload(onedata_logger).


%% @private
mock_refusing_path_type_check() ->
    mock_logger(),
    meck:new([helper_spec, storage_crud_utils], [passthrough, no_link]),
    meck:expect(helper_spec, build, fun(_Spec) -> helper_spec_not_inspected end),
    meck:expect(storage_crud_utils, verify_storage_path_type, fun(_HelperSpec) ->
        throw(path_type_refusal())
    end).


%% @private
unmock_refusing_path_type_check(_) ->
    meck:unload().


%% @private
path_type_refusal() ->
    ?ERR_BAD_VALUE_NOT_ALLOWED(?err_ctx(), <<"storagePathType">>, [<<"canonical">>]).


%% @private
count_error_logs_containing(Pattern) ->
    length([
        FormattedLog
        || {_Pid, {onedata_logger, log, [error, _Metadata, FormattedLog]}, _Result} <- meck:history(onedata_logger),
        string:find(FormattedLog, Pattern) /= nomatch
    ]).
