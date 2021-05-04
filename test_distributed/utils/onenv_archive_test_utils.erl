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
-include("modules/archive/archive.hrl").
-include("modules/logical_file_manager/lfm.hrl").
-include("proto/oneprovider/provider_messages.hrl").
-include_lib("ctool/include/test/test_utils.hrl").


-export([
    set_up_and_sync_archive/3,
    set_up_archive/3,
    set_up_archive/4,
    await_archive_sync/4
]).

-type archive_spec() :: #archive_spec{}.
-type archive_object() :: #archive_object{}.

-export_type([archive_spec/0, archive_object/0]).


-define(ATTEMPTS, 30).


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
    await_archive_sync(CreationProvider, SyncProviders, UserSelector, ArchiveObj),
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
    params = ParamsOrUndefined,
    attrs = AttrsOrUndefined
}) ->
    CreationNode = ?OCT_RAND_OP_NODE(CreationProvider),
    UserSessId = oct_background:get_user_session_id(UserId, CreationProvider),
    Params = utils:ensure_defined(ParamsOrUndefined, random_archive_params()),
    Attrs = utils:ensure_defined(AttrsOrUndefined, random_archive_attrs()),

    {ok, ArchiveId} = ?assertMatch(
        {ok, _},
        lfm_proxy:archive_dataset(CreationNode, UserSessId, DatasetId, Params, Attrs),
        ?ATTEMPTS
    ),

    {ok, #archive_info{index = Index}} = lfm_proxy:get_archive_info(CreationNode, UserSessId, ArchiveId),
    
    #archive_object{
        id = ArchiveId,
        params = Params,
        attrs = Attrs,
        index = Index
    }.


-spec await_archive_sync(
    oct_background:entity_selector(),
    [oct_background:entity_selector()],
    oct_background:entity_selector(),
    undefined | archive_object()
) ->
    ok | no_return().
await_archive_sync(_CreationProvider, _SyncProviders, _UserId, undefined) ->
    ok;

await_archive_sync(CreationProvider, SyncProviders, UserId, #archive_object{id = ArchiveId}) ->
    CreationNode = ?OCT_RAND_OP_NODE(CreationProvider),
    CreationNodeSessId = oct_background:get_user_session_id(UserId, CreationProvider),

    {ok, ArchiveInfo} = ?assertMatch(
        {ok, #archive_info{id = ArchiveId}},
        lfm_proxy:get_archive_info(CreationNode, CreationNodeSessId, ArchiveId)
    ),

    lists:foreach(fun(SyncProvider) ->
        SyncNode = ?OCT_RAND_OP_NODE(SyncProvider),
        SessId = oct_background:get_user_session_id(UserId, SyncProvider),

        ?assertEqual(
            {ok, ArchiveInfo},
            lfm_proxy:get_archive_info(SyncNode, SessId, ArchiveId),
            ?ATTEMPTS
        )
    end, SyncProviders).

%%%===================================================================
%%% Internal functions
%%%===================================================================

%% @private
-spec random_archive_params() -> archive:params().
random_archive_params() ->
    #archive_params{
        type = lists_utils:random_element(?ARCHIVE_TYPES),
        character = lists_utils:random_element(?ARCHIVE_CHARACTERS),
        data_structure = lists_utils:random_element(?ARCHIVE_DATA_STRUCTURES),
        metadata_structure = lists_utils:random_element(?ARCHIVE_METADATA_STRUCTURES)
    }.


%% @private
-spec random_archive_attrs() -> archive:attrs().
random_archive_attrs() ->
    case rand:uniform(2) of
        1 -> #archive_attrs{};
        2 -> #archive_attrs{description = str_utils:rand_hex(20)}
    end.