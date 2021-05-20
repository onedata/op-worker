%%%-------------------------------------------------------------------
%%% @author Jakub Kudzia
%%% @copyright (C) 2021 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% Utility functions operating on archives used in onenv ct tests.
%%% @end
%%%-------------------------------------------------------------------
-module(onenv_archive_test_utils).
-author("Jakub Kudzia").

-include("onenv_test_utils.hrl").
-include("modules/dataset/archive.hrl").
-include("modules/logical_file_manager/lfm.hrl").
-include("proto/oneprovider/provider_messages.hrl").
-include_lib("ctool/include/test/test_utils.hrl").


-export([
    set_up_and_sync_archive/3,
    set_up_archive/3,
    set_up_archive/4,
    await_archive_sync/5
]).

-type archive_spec() :: #archive_spec{}.
-type archive_object() :: #archive_object{}.

-export_type([archive_spec/0, archive_object/0]).


-define(ATTEMPTS, 120).


%%%===================================================================
%%% API
%%%===================================================================

-spec set_up_and_sync_archive(
    [oct_background:entity_selector()],
    oct_background:entity_selector(),
    dataset:id()
) ->
    undefined | archive_object().
set_up_and_sync_archive(Providers, UserSelector, DatasetId) ->
    [CreationProvider | SyncProviders] = lists_utils:shuffle(Providers),
    ArchiveObj = set_up_archive(CreationProvider, UserSelector, DatasetId),
    await_archive_sync(CreationProvider, SyncProviders, UserSelector, ArchiveObj, DatasetId),
    ArchiveObj.


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
    CreationNode = ?OCT_RAND_OP_NODE(CreationProvider),
    UserSessId = oct_background:get_user_session_id(UserId, CreationProvider),
    Config = utils:ensure_defined(ConfigOrUndefined, random_archive_config()),
    Description = utils:ensure_defined(AttrsOrUndefined, random_archive_description()),

    {ok, ArchiveId} = ?assertMatch(
        {ok, _},
        lfm_proxy:archive_dataset(CreationNode, UserSessId, DatasetId, Config, Description)
    ),

    {ok, ArchiveInfo} = lfm_proxy:get_archive_info(CreationNode, UserSessId, ArchiveId),
    
    #archive_object{
        id = ArchiveId,
        config = Config,
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
    CreationNode = ?OCT_RAND_OP_NODE(CreationProvider),
    CreationNodeSessId = oct_background:get_user_session_id(UserId, CreationProvider),

    ?assertMatch(
        {ok, #archive_info{id = ArchiveId}},
        lfm_proxy:get_archive_info(CreationNode, CreationNodeSessId, ArchiveId)
    ),

    lists:foreach(fun(SyncProvider) ->
        SyncNode = ?OCT_RAND_OP_NODE(SyncProvider),
        SessId = oct_background:get_user_session_id(UserId, SyncProvider),

        ?assertMatch(
            {ok, #archive_info{id = ArchiveId}},
            lfm_proxy:get_archive_info(SyncNode, SessId, ArchiveId),
            ?ATTEMPTS
        ),
        ListArchivesFun = fun() ->
            {ok, Archives, _} = lfm_proxy:list_archives(SyncNode, SessId, DatasetId, #{offset => 0, limit => 10000}),
            [AId || {_, AId} <- Archives]
        end,
        ?assertEqual(true, lists:member(ArchiveId, ListArchivesFun()), ?ATTEMPTS)

    end, SyncProviders).

%%%===================================================================
%%% Internal functions
%%%===================================================================

%% @private
-spec random_archive_config() -> archive:config().
random_archive_config() ->
    #archive_config{
        incremental = lists_utils:random_element(?SUPPORTED_INCREMENTAL_VALUES),
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