%%%-------------------------------------------------------------------
%%% @author Bartosz Walkowicz
%%% @copyright (C) 2026 Onedata (onedata.org)
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% Helpers shared by the file_lfm_*_tests modules - resolving the node, the
%%% session and the space the whole family works against, and creating the
%%% per test case root directory that keeps the cases from stepping on each
%%% other.
%%%
%%% Which provider, space and users these resolve to is fixed by the family (see
%%% file/file_lfm_test.hrl) rather than passed around, so that a case reads as
%%% what it tests rather than as environment plumbing.
%%% @end
%%%-------------------------------------------------------------------
-module(file_lfm_test_utils).
-author("Bartosz Walkowicz").

-include("file/file_lfm_test.hrl").
-include_lib("ctool/include/test/assertions.hrl").
-include_lib("onenv_ct/include/oct_background.hrl").

-export([get_node/0, get_session_id/1, set_direct_io/2]).
-export([get_space_id/0, get_space_dir_guid/0, build_space_path/0, build_space_path/1]).
-export([create_test_root_dir/2]).
-export([ensure_storage_driver_unmocked/0, ensure_direct_io/0]).


%%%===================================================================
%%% API
%%%===================================================================


-spec get_node() -> node().
get_node() ->
    oct_background:get_random_provider_node(?PROVIDER_SELECTOR).


-spec get_session_id(oct_background:entity_selector()) -> session:id().
get_session_id(UserSelector) ->
    oct_background:get_user_session_id(UserSelector, ?PROVIDER_SELECTOR).


%%--------------------------------------------------------------------
%% @doc
%% Declares whether the client behind the session does the io on the space
%% storage itself, rather than having the provider proxy it.
%%
%% A real oneclient settles this during the handshake, by trying to reach the
%% storage on its own (see fslogic_worker:handle_fuse_request/3 for
%% #verify_storage_test_file{}); a session created for a test never does, and a
%% space with no answer recorded counts as direct io. With direct io on, the
%% provider does not open the file on storage when the file is opened - it does
%% so lazily, upon the first read or write that reaches it anyway.
%% @end
%%--------------------------------------------------------------------
-spec set_direct_io(session:id(), boolean()) -> ok.
set_direct_io(SessId, IsDirectIo) ->
    ?assertEqual(ok, rpc:call(get_node(), session, set_direct_io, [SessId, get_space_id(), IsDirectIo])).


-spec get_space_id() -> od_space:id().
get_space_id() ->
    oct_background:get_space_id(?SPACE_SELECTOR).


-spec get_space_dir_guid() -> file_id:file_guid().
get_space_dir_guid() ->
    space_dir:guid(get_space_id()).


-spec build_space_path() -> file_meta:path().
build_space_path() ->
    <<"/", (oct_background:get_space_name(?SPACE_SELECTOR))/binary>>.


-spec build_space_path(file_meta:name()) -> file_meta:path().
build_space_path(FileName) ->
    <<(build_space_path())/binary, "/", FileName/binary>>.


%%--------------------------------------------------------------------
%% @doc
%% Creates a randomly named directory directly in the space, for a test case to
%% work within. The default directory mode is used, so that a user other than
%% the creator can still traverse it.
%% @end
%%--------------------------------------------------------------------
-spec create_test_root_dir(node(), session:id()) -> {file_id:file_guid(), file_meta:path()}.
create_test_root_dir(Node, SessId) ->
    DirPath = build_space_path(generator:gen_name()),
    {ok, DirGuid} = ?assertMatch({ok, _}, lfm_proxy:mkdir(Node, SessId, DirPath)),
    {DirGuid, DirPath}.


%%--------------------------------------------------------------------
%% @doc
%% Several tests mock storage_driver for the duration of a single case and
%% unload it themselves. A case killed by its timetrap never gets that far, so
%% this is called defensively from end_per_testcase and, for a run that died
%% without running even that, from the init_per_suite posthook (unloading a
%% module that was never mocked is a no-op).
%% @end
%%--------------------------------------------------------------------
-spec ensure_storage_driver_unmocked() -> ok.
ensure_storage_driver_unmocked() ->
    test_utils:mock_unload(oct_background:get_provider_nodes(?PROVIDER_SELECTOR), [storage_driver]).


%%--------------------------------------------------------------------
%% @doc
%% Counterpart of the above for the tests that need the provider to do the io -
%% they turn direct io off for the duration of a single case and turn it back on
%% themselves. Left off, it would silently move every later case onto the other
%% io path. Sessions do not outlive a run (their nonce is drawn anew per
%% oct_background:init_per_suite), so unlike a mock this needs no defence at the
%% suite level - only between the test cases of one run.
%% @end
%%--------------------------------------------------------------------
-spec ensure_direct_io() -> ok.
ensure_direct_io() ->
    lists:foreach(fun(UserSelector) ->
        set_direct_io(get_session_id(UserSelector), true)
    end, [?USER_SELECTOR, ?OTHER_USER_SELECTOR]).
