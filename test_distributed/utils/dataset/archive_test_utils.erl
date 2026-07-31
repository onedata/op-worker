%%%-------------------------------------------------------------------
%%% @author Jakub Kudzia
%%% @copyright (C) 2021-2026 Onedata (onedata.org)
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% Utility functions for creating archives and awaiting their synchronization
%%% between providers. Assertions about archives live in
%%% archive_verification_test_utils.
%%% @end
%%%-------------------------------------------------------------------
-module(archive_test_utils).
-author("Jakub Kudzia").

-include("file/file_tree_test.hrl").
-include("modules/dataset/archive.hrl").
-include("modules/logical_file_manager/lfm.hrl").
-include("proto/oneprovider/provider_messages.hrl").
-include_lib("ctool/include/test/test_utils.hrl").


-export([
    set_up_archive/3,
    set_up_archive/4,
    await_archive_sync/5
]).
-export([
    mock_gated_archive_verification/0,
    await_gated_archive_verification/2,
    resume_gated_archive_verification/2
]).

-type archive_spec() :: #archive_spec{}.
-type archive_object() :: #archive_object{}.

-export_type([archive_spec/0, archive_object/0]).


-define(ATTEMPTS, 600).


%%%===================================================================
%%% API
%%%===================================================================

-spec set_up_archive(
    oct_background:entity_selector(),
    od_user:id(),
    dataset:id()
) ->
    undefined | archive_object().
set_up_archive(CreationProvider, UserId, DatasetId) ->
    set_up_archive(CreationProvider, UserId, DatasetId, #archive_spec{}).


-spec set_up_archive(
    oct_background:entity_selector(),
    oct_background:entity_selector(),
    dataset:id(),
    undefined | archive_spec()
) ->
    undefined | archive_object().
set_up_archive(_CreationProvider, _UserId, _DatasetId, undefined) ->
    undefined;
set_up_archive(CreationProvider, UserId, DatasetId, #archive_spec{
    config = ConfigOrUndefined,
    description = AttrsOrUndefined
}) ->
    CreationNode = oct_background:get_random_provider_node(CreationProvider),
    UserSessId = oct_background:get_user_session_id(UserId, CreationProvider),
    Config = utils:ensure_defined(ConfigOrUndefined, random_archive_config()),
    Description = utils:ensure_defined(AttrsOrUndefined, random_archive_description()),

    {ok, ArchiveId} = ?assertMatch(
        {ok, _},
        opt_archives:archive_dataset(CreationNode, UserSessId, DatasetId, Config, Description)
    ),

    {ok, ArchiveInfo = #archive_info{
        config = FinalConfig
    }} = opt_archives:get_info(CreationNode, UserSessId, ArchiveId),
    #archive_object{
        id = ArchiveId,
        config = FinalConfig,
        description = Description,
        index = ArchiveInfo#archive_info.index
    }.


-spec await_archive_sync(
    oct_background:entity_selector(),
    [oct_background:entity_selector()],
    oct_background:entity_selector(),
    undefined | archive_object(),
    datset:id()
) ->
    ok | no_return().
await_archive_sync(_CreationProvider, _SyncProviders, _UserId, undefined, _) ->
    ok;

await_archive_sync(CreationProvider, SyncProviders, UserId, #archive_object{id = ArchiveId}, DatasetId) ->
    CreationNode = oct_background:get_random_provider_node(CreationProvider),
    CreationNodeSessId = oct_background:get_user_session_id(UserId, CreationProvider),

    ?assertMatch(
        {ok, #archive_info{id = ArchiveId}},
        opt_archives:get_info(CreationNode, CreationNodeSessId, ArchiveId)
    ),

    lists:foreach(fun(SyncProvider) ->
        SyncNode = oct_background:get_random_provider_node(SyncProvider),
        SessId = oct_background:get_user_session_id(UserId, SyncProvider),

        ?assertMatch(
            {ok, #archive_info{id = ArchiveId}},
            opt_archives:get_info(SyncNode, SessId, ArchiveId),
            ?ATTEMPTS
        ),
        ListArchivesFun = fun() ->
            {ok, {Archives, _}} = opt_archives:list(SyncNode, SessId, DatasetId, #{offset => 0, limit => 10000}),
            [AId || {_, AId} <- Archives]
        end,
        ?assertEqual(true, lists:member(ArchiveId, ListArchivesFun()), ?ATTEMPTS)

    end, SyncProviders).

%%%===================================================================
%%% Archive verification traverse gate
%%%
%%% Blocks the product's archive verification traverse right before it makes
%%% the archive immutable, so that a test can modify the archived files and
%%% only then let the traverse proceed.
%%%
%%% As the expectation fun below is evaluated on the provider node, this module
%%% must be added to ?LOAD_MODULES of any suite gating the traverse - otherwise
%%% the fun cannot be applied there and the block is never reached, which shows
%%% up as await_gated_archive_verification/2 timing out.
%%%===================================================================


-spec mock_gated_archive_verification() -> ok.
mock_gated_archive_verification() ->
    Nodes = oct_background:get_all_providers_nodes(),
    test_utils:mock_new(Nodes, archive_verification_traverse, [passthrough]),
    Pid = self(),
    test_utils:mock_expect(Nodes, archive_verification_traverse, block_archive_modification,
        fun(ArchiveDoc) ->
            {ok, ArchiveId} = archive:get_id(ArchiveDoc),
            Pid ! {archive_verification_mock, ArchiveId, self()},
            receive {continue, ArchiveId} ->
                meck:passthrough([ArchiveDoc])
            end
        end).


%% @doc Awaits the gated traverse reaching the block. Requires prior mocking.
-spec await_gated_archive_verification(archive:id(), TimeoutSeconds :: pos_integer()) ->
    {ok, pid()} | {error, term()}.
await_gated_archive_verification(ArchiveId, TimeoutSeconds) ->
    receive {archive_verification_mock, ArchiveId, Pid} ->
        {ok, Pid}
    after timer:seconds(TimeoutSeconds) ->
        {error, archive_creation_not_finished}
    end.


%% @doc Releases the block awaited by await_gated_archive_verification/2.
-spec resume_gated_archive_verification(pid(), archive:id()) -> ok.
resume_gated_archive_verification(Pid, ArchiveId) ->
    Pid ! {continue, ArchiveId},
    ok.


%%%===================================================================
%%% Internal functions
%%%===================================================================

%% @private
-spec random_archive_config() -> archive:config().
random_archive_config() ->
    #archive_config{
        include_dip = lists_utils:random_element(?SUPPORTED_INCLUDE_DIP_VALUES),
        layout = lists_utils:random_element(?ARCHIVE_LAYOUTS)
    }.


%% @private
-spec random_archive_description() -> archive:description().
random_archive_description() ->
    case rand:uniform(2) of
        1 -> ?DEFAULT_ARCHIVE_DESCRIPTION;
        2 -> str_utils:rand_hex(20)
    end.
