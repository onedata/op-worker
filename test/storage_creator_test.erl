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
%%% Besides, the QoS parameters must reach Onezone as numbers where a QoS
%%% expression reads them as numbers, and a create whose credentials would fail
%%% every use of the storage must be refused before Onezone.
%%% @end
%%%-------------------------------------------------------------------
-module(storage_creator_test).
-author("Bartosz Walkowicz").

-include_lib("ctool/include/errors.hrl").
-include_lib("opw_panel_contracts/include/storage/common.hrl").
-include_lib("opw_panel_contracts/include/storage/http.hrl").
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


%% Onepanel passes the QoS parameters as the REST request had them, as strings.
create_sends_normalized_qos_parameters_to_onezone_test_() ->
    {setup, fun mock_create_up_to_onezone/0, fun unmock_create_up_to_onezone/1, fun() ->
        Spec = #storage_create_spec{
            type = <<"posix">>,
            name = <<"some-storage">>,
            luma = #luma_spec{feed = auto},
            qos_parameters = #{<<"priority">> => <<"2">>, <<"region">> => <<"eu">>},
            credentials = #posix_helper_credentials{uid = 0}
        },

        storage_creator:create(Spec),

        ?assertEqual(
            #{<<"priority">> => 2, <<"region">> => <<"eu">>},
            meck:capture(first, storage_logic, create_in_zone, '_', 4)
        )
    end}.


%% Onepanel's model has the feed's URL optional; with no URL every request to the
%% feed would go nowhere.
external_luma_feed_without_url_is_refused_before_onezone_test_() ->
    {setup, fun mock_create_up_to_onezone/0, fun unmock_create_up_to_onezone/1, fun() ->
        Spec = #storage_create_spec{
            type = <<"posix">>,
            name = <<"some-storage">>,
            luma = #luma_spec{feed = external},
            credentials = #posix_helper_credentials{uid = 0}
        },

        ?assertMatch(?ERR_MISSING_REQUIRED_VALUE(_, url), storage_creator:create(Spec)),
        ?assertEqual(0, meck:num_calls(storage_logic, create_in_zone, '_'))
    end}.


%% The parser takes an empty string as well.
external_luma_feed_with_empty_url_is_refused_before_onezone_test_() ->
    {setup, fun mock_create_up_to_onezone/0, fun unmock_create_up_to_onezone/1, fun() ->
        Spec = #storage_create_spec{
            type = <<"posix">>,
            name = <<"some-storage">>,
            luma = #luma_spec{feed = external, url = <<>>},
            credentials = #posix_helper_credentials{uid = 0}
        },

        ?assertMatch(?ERR_BAD_VALUE_EMPTY(_, url), storage_creator:create(Spec)),
        ?assertEqual(0, meck:num_calls(storage_logic, create_in_zone, '_'))
    end}.


%% Onepanel's model has the credentials optional, while every use of an HTTP
%% storage requires them unless its credentials type is none. The diagnostics
%% skip HTTP, so nothing else stops such a create.
credentials_are_refused_before_onezone_test_() ->
    {setup, fun mock_checks_up_to_onezone/0, fun unmock_create_up_to_onezone/1, fun() ->
        Spec = #storage_create_spec{
            type = <<"http">>,
            name = <<"some-storage">>,
            readonly = true,
            imported = true,
            luma = #luma_spec{feed = auto},
            configuration = #http_helper_configuration{
                endpoint = <<"https://example.com">>,
                storage_path_type = canonical
            },
            credentials = #http_helper_credentials{credentials_type = basic}
        },

        ?assertMatch(?ERR_MISSING_REQUIRED_VALUE(_, <<"credentials">>), storage_creator:create(Spec)),
        ?assertEqual(0, meck:num_calls(storage_logic, create_in_zone, '_'))
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
    meck:expect(storage_crud_utils, verify_credentials, fun(_HelperSpec) -> ok end),
    meck:expect(storage_crud_utils, verify_storage_path_type, fun(_HelperSpec) ->
        throw(path_type_refusal())
    end).


%% @private
unmock_refusing_path_type_check(_) ->
    meck:unload().


%% @private
%% Everything the create does before it writes to Onezone passes, and the write
%% to Onezone fails, which ends the create there.
mock_create_up_to_onezone() ->
    mock_checks_up_to_onezone(),
    meck:expect(helper_spec, build, fun(_Spec) -> helper_spec_not_inspected end),
    meck:expect(storage_crud_utils, verify_credentials, fun(_HelperSpec) -> ok end).


%% @private
%% The checks the create runs before Onezone pass, but for the check of the
%% credentials, which runs on the helper spec built from the create spec; the
%% write to Onezone fails.
mock_checks_up_to_onezone() ->
    mock_logger(),
    meck:new([helper_spec, storage_crud_utils], [passthrough, no_link]),
    meck:new(storage_logic, [no_link]),
    meck:expect(storage_crud_utils, verify_storage_path_type, fun(_HelperSpec) -> ok end),
    meck:expect(storage_crud_utils, verify_configuration, fun(_Name, _Readonly, _Imported, _HelperSpec) -> ok end),
    meck:expect(storage_crud_utils, run_diagnostics, fun(_HelperSpec, _LumaFeed, _Mode) -> ok end),
    meck:expect(storage_logic, create_in_zone, fun(_Name, _Imported, _Readonly, _QosParameters) ->
        {error, onezone_write_not_reached_in_test}
    end).


%% @private
unmock_create_up_to_onezone(_) ->
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
