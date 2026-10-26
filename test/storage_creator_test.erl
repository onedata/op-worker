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
%%% the create's failures do, in an error returned to the caller.
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
