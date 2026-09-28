%%%-------------------------------------------------------------------
%%% @author Bartosz Walkowicz
%%% @copyright (C) 2026 Onedata (onedata.org)
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% Unit tests of registering a storage in rtransfer, pinning down the one
%%% property both callers depend on: it is best effort. By the time either the
%%% storage creation or the storage update saga gets to register a storage, the
%%% change is already persisted in Onezone and in the local datastore and
%%% neither of them can take it back - so a registration that did not get
%%% through must be reported to the logs and nowhere else.
%%%
%%% The failures are injected at the far end, in rtransfer_link, so that what
%%% runs in between - building the helper params, spreading the call over the
%%% nodes, sifting through the answers - is the real thing.
%%% @end
%%%-------------------------------------------------------------------
-module(rtransfer_config_test).
-author("Bartosz Walkowicz").

-include("modules/datastore/datastore_models.hrl").
-include("modules/storage/helpers/helpers.hrl").
-include_lib("eunit/include/eunit.hrl").

-define(STORAGE_ID, <<"dummyStorageId">>).


%%%===================================================================
%%% Tests
%%%===================================================================


successful_registration_reaches_rtransfer_link_test_() ->
    {setup, fun mock_registration_deps/0, fun unmock_registration_deps/1, fun() ->
        ?assertEqual(ok, rtransfer_config:add_storage(?STORAGE_ID)),

        ?assertEqual(1, meck:num_calls(rtransfer_link, add_storage, '_'))
    end}.


rtransfer_link_error_does_not_fail_registration_test_() ->
    {setup, fun mock_registration_deps/0, fun unmock_registration_deps/1, fun() ->
        meck:expect(rtransfer_link, add_storage, fun(_StorageId, _Name, _Params) ->
            {error, timeout}
        end),

        ?assertEqual(ok, rtransfer_config:add_storage(?STORAGE_ID))
    end}.


crashing_rtransfer_link_does_not_fail_registration_test_() ->
    {setup, fun mock_registration_deps/0, fun unmock_registration_deps/1, fun() ->
        meck:expect(rtransfer_link, add_storage, fun(_StorageId, _Name, _Params) ->
            error(badarg)
        end),

        ?assertEqual(ok, rtransfer_config:add_storage(?STORAGE_ID))
    end}.


crash_before_reaching_rtransfer_link_does_not_fail_registration_test_() ->
    {setup, fun mock_registration_deps/0, fun unmock_registration_deps/1, fun() ->
        % everything the params are built from is read inside the call, so it
        % can fail before there is anything to send anywhere
        meck:expect(storage, get_helper_spec, fun(_StorageId) -> error(badarg) end),

        ?assertEqual(ok, rtransfer_config:add_storage(?STORAGE_ID))
    end}.


%%%===================================================================
%%% Internal functions
%%%===================================================================


%% @private
mock_registration_deps() ->
    meck:new([storage, consistent_hashing, rtransfer_link], [no_link]),

    meck:expect(storage, get_helper_spec, fun(_StorageId) -> #helper_spec{
        name = ?POSIX_HELPER_NAME,
        configuration = #{<<"mountPoint">> => <<"/mnt/st1">>},
        credentials = #{<<"uid">> => <<"1000">>, <<"gid">> => <<"1000">>}
    } end),
    % a single-element list holding this node is applied directly rather than
    % sent over rpc, which keeps the tests free of a distributed node
    meck:expect(consistent_hashing, get_all_nodes, fun() -> [node()] end),
    meck:expect(rtransfer_link, add_storage, fun(_StorageId, _Name, _Params) -> ok end).


%% @private
unmock_registration_deps(_) ->
    meck:unload().
